"""Where DSL parser and evaluator per Addendum E.

Grammar (EBNF)::

    expr        := or_expr
    or_expr     := and_expr (("OR" | "or") and_expr)*
    and_expr    := not_expr (("AND" | "and") not_expr)*
    not_expr    := ("NOT" | "not") not_expr | primary
    primary     := comparison | "(" expr ")"
    comparison  := path op literal
    op          := "=" | "!=" | "<" | "<=" | ">" | ">="
    path        := JSONPath relative (must NOT start with "$")
    literal     := number | string | "true" | "false" | "null"
    number      := integer | decimal (parsed as int or Decimal, NEVER float)
    string      := single-quoted with escapes: \\', \\\\, \\n, \\r, \\t

Operator precedence: NOT > AND > OR.

Evaluation semantics:
    - Short-circuit: left-to-right, deterministic.
    - Wildcard ``[*]``: existential -- any match satisfies.
    - Missing path: comparisons return False, except ``!= null`` returns True
      only if path exists and value is not null.
    - Numeric comparisons require both operands numeric (Decimal/int).
    - String comparisons: lexicographic by Unicode code point.
    - Boolean: only ``=`` and ``!=`` supported.
    - null: only ``=`` and ``!=`` supported.

Compute accounting (section E.4):
    - +1 per path segment traversed
    - +W for wildcard expansions (W = number of expanded members)
    - +1 for the operator comparison
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Union

from mcp_artifact_gateway.query.jsonpath import (
    BudgetExceededError,
    IndexSegment,
    PropertySegment,
    RootSegment,
    Segment,
    WildcardSegment,
    parse_jsonpath,
)


# ---------------------------------------------------------------------------
# AST Nodes
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class LiteralValue:
    """A typed literal value from the where clause.

    Attributes:
        value: The Python value (int, Decimal, str, bool, or None).
        type: A tag string: ``"int"``, ``"decimal"``, ``"string"``,
            ``"boolean"``, or ``"null"``.
    """
    value: int | Decimal | str | bool | None
    type: str


@dataclass(frozen=True, slots=True)
class Comparison:
    """A comparison expression: ``path op literal``."""
    path: str
    op: str
    literal: LiteralValue


@dataclass(frozen=True, slots=True)
class NotExpr:
    """Logical NOT: ``NOT child``."""
    child: ASTNode


@dataclass(frozen=True, slots=True)
class AndExpr:
    """Logical AND: ``child1 AND child2 AND ...``."""
    children: tuple[ASTNode, ...]


@dataclass(frozen=True, slots=True)
class OrExpr:
    """Logical OR: ``child1 OR child2 OR ...``."""
    children: tuple[ASTNode, ...]


ASTNode = Union[Comparison, NotExpr, AndExpr, OrExpr]


# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------

# Token types
_TK_AND = "AND"
_TK_OR = "OR"
_TK_NOT = "NOT"
_TK_LPAREN = "("
_TK_RPAREN = ")"
_TK_OP = "OP"          # comparison operator
_TK_PATH = "PATH"      # relative JSONPath
_TK_STRING = "STRING"  # single-quoted string literal
_TK_NUMBER = "NUMBER"  # numeric literal
_TK_TRUE = "TRUE"
_TK_FALSE = "FALSE"
_TK_NULL = "NULL"
_TK_EOF = "EOF"

# Comparison operators, longest first for greedy match
_OPERATORS = ("!=", "<=", ">=", "=", "<", ">")


@dataclass(slots=True)
class _Token:
    """A lexer token."""
    type: str
    value: str
    pos: int


def _tokenize(clause: str) -> list[_Token]:
    """Tokenize a where clause string into a list of tokens.

    Raises ValueError on unrecognized input.
    """
    tokens: list[_Token] = []
    i = 0
    length = len(clause)

    while i < length:
        # Skip whitespace
        if clause[i].isspace():
            i += 1
            continue

        # Parentheses
        if clause[i] == "(":
            tokens.append(_Token(_TK_LPAREN, "(", i))
            i += 1
            continue
        if clause[i] == ")":
            tokens.append(_Token(_TK_RPAREN, ")", i))
            i += 1
            continue

        # Comparison operators (check multi-char before single-char)
        matched_op = False
        for op in _OPERATORS:
            if clause[i : i + len(op)] == op:
                # Make sure '!=' is not confused with a path starting with '!'
                tokens.append(_Token(_TK_OP, op, i))
                i += len(op)
                matched_op = True
                break
        if matched_op:
            continue

        # Single-quoted string literal
        if clause[i] == "'":
            start = i
            i += 1
            parts: list[str] = []
            while i < length and clause[i] != "'":
                if clause[i] == "\\":
                    if i + 1 >= length:
                        raise ValueError(
                            f"Trailing backslash in string literal at position {i}"
                        )
                    esc = clause[i + 1]
                    if esc == "'":
                        parts.append("'")
                    elif esc == "\\":
                        parts.append("\\")
                    elif esc == "n":
                        parts.append("\n")
                    elif esc == "r":
                        parts.append("\r")
                    elif esc == "t":
                        parts.append("\t")
                    else:
                        raise ValueError(
                            f"Invalid escape sequence '\\{esc}' in string "
                            f"literal at position {i}"
                        )
                    i += 2
                else:
                    parts.append(clause[i])
                    i += 1
            if i >= length:
                raise ValueError(
                    f"Unterminated string literal starting at position {start}"
                )
            i += 1  # skip closing quote
            tokens.append(_Token(_TK_STRING, "".join(parts), start))
            continue

        # Keywords and paths: read a "word" (anything that is not whitespace,
        # parens, comparison ops, or quotes). Paths can contain ., [, ], *, '
        # inside brackets, digits, letters, underscores.
        if clause[i] in (".", "[") or clause[i].isalpha() or clause[i] == "_":
            start = i
            # Read a path or keyword token
            word = _read_path_or_keyword(clause, i)
            i += len(word)

            # Check for keywords (case-insensitive matching but only AND/OR/NOT)
            upper = word.upper()
            if upper == "AND" and _is_keyword_boundary(clause, start, i):
                tokens.append(_Token(_TK_AND, word, start))
            elif upper == "OR" and _is_keyword_boundary(clause, start, i):
                tokens.append(_Token(_TK_OR, word, start))
            elif upper == "NOT" and _is_keyword_boundary(clause, start, i):
                tokens.append(_Token(_TK_NOT, word, start))
            elif word == "true":
                tokens.append(_Token(_TK_TRUE, word, start))
            elif word == "false":
                tokens.append(_Token(_TK_FALSE, word, start))
            elif word == "null":
                tokens.append(_Token(_TK_NULL, word, start))
            else:
                # It is a path
                tokens.append(_Token(_TK_PATH, word, start))
            continue

        # Numeric literal (possibly negative)
        if clause[i].isdigit() or (
            clause[i] == "-" and i + 1 < length and clause[i + 1].isdigit()
        ):
            start = i
            if clause[i] == "-":
                i += 1
            while i < length and clause[i].isdigit():
                i += 1
            # Check for decimal point
            if i < length and clause[i] == "." and i + 1 < length and clause[i + 1].isdigit():
                i += 1  # skip dot
                while i < length and clause[i].isdigit():
                    i += 1
            num_str = clause[start:i]
            tokens.append(_Token(_TK_NUMBER, num_str, start))
            continue

        raise ValueError(
            f"Unexpected character {clause[i]!r} at position {i} "
            f"in where clause"
        )

    tokens.append(_Token(_TK_EOF, "", length))
    return tokens


def _read_path_or_keyword(clause: str, start: int) -> str:
    """Read a path or keyword token starting at *start*.

    A path can contain: identifiers, dots, brackets with contents.
    A keyword is a bare identifier (AND, OR, NOT, true, false, null).

    We greedily consume characters that form a valid JSONPath or identifier.
    """
    i = start
    length = len(clause)

    while i < length:
        ch = clause[i]

        if ch == ".":
            # Dot notation: consume .identifier
            i += 1
            if i < length and (clause[i].isalpha() or clause[i] == "_"):
                while i < length and (clause[i].isalnum() or clause[i] == "_"):
                    i += 1
            else:
                # Trailing dot or dot followed by non-identifier -- stop before dot
                i -= 1
                break

        elif ch == "[":
            # Bracket notation: consume everything until matching ]
            i += 1
            if i < length and clause[i] == "'":
                # String key: consume until closing '
                i += 1
                while i < length:
                    if clause[i] == "\\":
                        i += 2  # skip escape
                    elif clause[i] == "'":
                        i += 1
                        break
                    else:
                        i += 1
            elif i < length and clause[i] == "*":
                i += 1  # consume *
            else:
                # Integer index
                while i < length and clause[i].isdigit():
                    i += 1

            if i < length and clause[i] == "]":
                i += 1
            else:
                # Malformed bracket -- let the JSONPath parser handle the error
                break

        elif ch.isalnum() or ch == "_":
            while i < length and (clause[i].isalnum() or clause[i] == "_"):
                i += 1

        else:
            break

    return clause[start:i]


def _is_keyword_boundary(clause: str, start: int, end: int) -> bool:
    """Check that the word at clause[start:end] is a standalone keyword.

    A keyword must not be followed by path continuation characters (., [).
    It also must be a bare word -- not part of a longer path segment.
    """
    # Check not followed by path continuation
    if end < len(clause) and clause[end] in (".", "["):
        return False
    # Check not preceded by path characters (unless at start of clause)
    if (start > 0 and clause[start - 1] in (".", "]", "_")) or (
        start > 0 and clause[start - 1].isalnum()
    ):
        return False
    return True


# ---------------------------------------------------------------------------
# Recursive Descent Parser
# ---------------------------------------------------------------------------

class _Parser:
    """Recursive descent parser for the where DSL."""

    def __init__(self, tokens: list[_Token]) -> None:
        self._tokens = tokens
        self._pos = 0

    def _peek(self) -> _Token:
        return self._tokens[self._pos]

    def _advance(self) -> _Token:
        tok = self._tokens[self._pos]
        self._pos += 1
        return tok

    def _expect(self, token_type: str) -> _Token:
        tok = self._peek()
        if tok.type != token_type:
            raise ValueError(
                f"Expected {token_type} but got {tok.type} ({tok.value!r}) "
                f"at position {tok.pos}"
            )
        return self._advance()

    def parse(self) -> ASTNode:
        """Parse the full expression and ensure all tokens are consumed."""
        node = self._parse_or_expr()
        if self._peek().type != _TK_EOF:
            tok = self._peek()
            raise ValueError(
                f"Unexpected token {tok.type} ({tok.value!r}) at position "
                f"{tok.pos} -- expected end of expression"
            )
        return node

    def _parse_or_expr(self) -> ASTNode:
        """or_expr := and_expr (("OR" | "or") and_expr)*"""
        children = [self._parse_and_expr()]
        while self._peek().type == _TK_OR:
            self._advance()
            children.append(self._parse_and_expr())
        if len(children) == 1:
            return children[0]
        return OrExpr(children=tuple(children))

    def _parse_and_expr(self) -> ASTNode:
        """and_expr := not_expr (("AND" | "and") not_expr)*"""
        children = [self._parse_not_expr()]
        while self._peek().type == _TK_AND:
            self._advance()
            children.append(self._parse_not_expr())
        if len(children) == 1:
            return children[0]
        return AndExpr(children=tuple(children))

    def _parse_not_expr(self) -> ASTNode:
        """not_expr := ("NOT" | "not") not_expr | primary"""
        if self._peek().type == _TK_NOT:
            self._advance()
            child = self._parse_not_expr()
            return NotExpr(child=child)
        return self._parse_primary()

    def _parse_primary(self) -> ASTNode:
        """primary := comparison | "(" expr ")" """
        if self._peek().type == _TK_LPAREN:
            self._advance()
            node = self._parse_or_expr()
            self._expect(_TK_RPAREN)
            return node

        return self._parse_comparison()

    def _parse_comparison(self) -> ASTNode:
        """comparison := path op literal"""
        path_tok = self._expect(_TK_PATH)
        path_str = path_tok.value

        # Validate that path is relative (must not start with $)
        if path_str.startswith("$"):
            raise ValueError(
                f"Where clause paths must be relative (must not start with "
                f"'$'): {path_str!r} at position {path_tok.pos}"
            )

        # Allow bare identifiers (e.g. "a") by treating them as ".a".
        if not path_str.startswith((".", "[")):
            path_str = "." + path_str

        # Validate the path by parsing it as an absolute path (prepend $)
        try:
            parse_jsonpath("$" + path_str)
        except ValueError as exc:
            raise ValueError(
                f"Invalid JSONPath in where clause at position {path_tok.pos}: "
                f"{exc}"
            ) from exc

        op_tok = self._expect(_TK_OP)
        literal = self._parse_literal()

        return Comparison(path=path_str, op=op_tok.value, literal=literal)

    def _parse_literal(self) -> LiteralValue:
        """literal := number | string | "true" | "false" | "null" """
        tok = self._peek()

        if tok.type == _TK_STRING:
            self._advance()
            return LiteralValue(value=tok.value, type="string")

        if tok.type == _TK_NUMBER:
            self._advance()
            return _parse_number(tok.value, tok.pos)

        if tok.type == _TK_TRUE:
            self._advance()
            return LiteralValue(value=True, type="boolean")

        if tok.type == _TK_FALSE:
            self._advance()
            return LiteralValue(value=False, type="boolean")

        if tok.type == _TK_NULL:
            self._advance()
            return LiteralValue(value=None, type="null")

        raise ValueError(
            f"Expected literal value but got {tok.type} ({tok.value!r}) "
            f"at position {tok.pos}"
        )


def _parse_number(text: str, pos: int) -> LiteralValue:
    """Parse a numeric literal as int or Decimal (never float).

    Integers have no decimal point. Decimals have a decimal point with at
    least one digit after it.
    """
    if "." in text:
        try:
            value = Decimal(text)
        except InvalidOperation as exc:
            raise ValueError(
                f"Invalid decimal literal {text!r} at position {pos}"
            ) from exc
        return LiteralValue(value=value, type="decimal")
    else:
        try:
            value = int(text)
        except ValueError as exc:
            raise ValueError(
                f"Invalid integer literal {text!r} at position {pos}"
            ) from exc
        return LiteralValue(value=value, type="int")


# ---------------------------------------------------------------------------
# Public parse API
# ---------------------------------------------------------------------------

def parse_where(clause: str) -> ASTNode:
    """Parse a where clause string into an AST.

    Args:
        clause: The where clause string.

    Returns:
        An ASTNode representing the parsed expression.

    Raises:
        ValueError: If the clause is syntactically invalid.
    """
    if not clause or not clause.strip():
        raise ValueError("Where clause must not be empty")
    tokens = _tokenize(clause)
    parser = _Parser(tokens)
    return parser.parse()


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------

def evaluate_where(
    node: ASTNode,
    record: Any,
    max_wildcard_expansion: int = 10000,
) -> tuple[bool, int]:
    """Evaluate a where clause AST against a record.

    Args:
        node: The parsed AST node.
        record: The Python object (dict/list) to evaluate against.
        max_wildcard_expansion: Maximum wildcard expansion per path evaluation.

    Returns:
        A tuple ``(matches, compute_steps)`` where *matches* is a boolean
        indicating whether the record satisfies the clause and *compute_steps*
        is the exact number of compute steps consumed.

    Raises:
        BudgetExceededError: If wildcard expansion exceeds the maximum.
    """
    return _eval_node(node, record, max_wildcard_expansion)


def _eval_node(
    node: ASTNode,
    record: Any,
    max_wildcard: int,
) -> tuple[bool, int]:
    """Recursively evaluate an AST node."""
    if isinstance(node, Comparison):
        return _eval_comparison(node, record, max_wildcard)
    elif isinstance(node, NotExpr):
        result, steps = _eval_node(node.child, record, max_wildcard)
        return (not result, steps)
    elif isinstance(node, AndExpr):
        return _eval_and(node, record, max_wildcard)
    elif isinstance(node, OrExpr):
        return _eval_or(node, record, max_wildcard)
    else:
        raise TypeError(f"Unknown AST node type: {type(node).__name__}")


def _eval_and(
    node: AndExpr,
    record: Any,
    max_wildcard: int,
) -> tuple[bool, int]:
    """Evaluate AND with left-to-right short-circuiting."""
    total_steps = 0
    for child in node.children:
        result, steps = _eval_node(child, record, max_wildcard)
        total_steps += steps
        if not result:
            # Short-circuit: AND fails on first False
            return (False, total_steps)
    return (True, total_steps)


def _eval_or(
    node: OrExpr,
    record: Any,
    max_wildcard: int,
) -> tuple[bool, int]:
    """Evaluate OR with left-to-right short-circuiting."""
    total_steps = 0
    for child in node.children:
        result, steps = _eval_node(child, record, max_wildcard)
        total_steps += steps
        if result:
            # Short-circuit: OR succeeds on first True
            return (True, total_steps)
    return (False, total_steps)


def _eval_comparison(
    node: Comparison,
    record: Any,
    max_wildcard: int,
) -> tuple[bool, int]:
    """Evaluate a comparison node against a record.

    Path evaluation semantics (section E.2):
    - Paths are relative to the record root.
    - Missing path: comparisons return False, except ``!= null`` returns True
      only if the path exists and the value is not null.
    - Wildcard ``[*]``: existential -- any match satisfies.
    """
    # Parse the relative path (prepend $ for parsing)
    segments = parse_jsonpath("$" + node.path)

    # Walk the path manually to count compute steps precisely
    resolved_values, steps = _resolve_path_with_accounting(
        record, segments, max_wildcard
    )

    # +1 for the comparison operator itself (counted once per comparison node,
    # regardless of how many resolved values we compare against)
    steps += 1

    if not resolved_values:
        # Path does not exist in the record
        if node.op == "!=" and node.literal.type == "null" and node.literal.value is None:
            # "path != null" on missing path returns True (the path is absent,
            # meaning it is not a present null).
            # Wait -- spec says: "Missing path: comparisons return False,
            # except != null returns True only if path exists and value is
            # not null." Since path is missing, != null returns False.
            return (False, steps)
        return (False, steps)

    # For wildcard paths, any match satisfies (existential semantics)
    for value in resolved_values:
        if _compare_value(value, node.op, node.literal):
            return (True, steps)

    return (False, steps)


def _resolve_path_with_accounting(
    record: Any,
    segments: list[Segment],
    max_wildcard: int,
) -> tuple[list[Any], int]:
    """Resolve a parsed path against a record, tracking compute steps.

    Returns:
        A tuple of (list_of_resolved_values, compute_steps).

    Compute accounting:
        - +1 per non-root segment traversed per active branch
        - +W for each wildcard expansion (W = number of expanded members)
    """
    # Start with the root; the RootSegment does not count as a step
    current_values: list[Any] = [record]
    steps = 0

    for seg in segments[1:]:  # skip RootSegment
        next_values: list[Any] = []

        for current in current_values:
            if isinstance(seg, PropertySegment):
                steps += 1
                if isinstance(current, dict) and seg.name in current:
                    next_values.append(current[seg.name])

            elif isinstance(seg, IndexSegment):
                steps += 1
                if isinstance(current, list) and 0 <= seg.index < len(current):
                    next_values.append(current[seg.index])

            elif isinstance(seg, WildcardSegment):
                if isinstance(current, dict):
                    keys = sorted(current.keys())
                    if len(keys) > max_wildcard:
                        raise BudgetExceededError(
                            f"Wildcard expansion on object with {len(keys)} "
                            f"keys exceeds maximum of {max_wildcard}"
                        )
                    steps += len(keys)
                    for key in keys:
                        next_values.append(current[key])
                elif isinstance(current, list):
                    if len(current) > max_wildcard:
                        raise BudgetExceededError(
                            f"Wildcard expansion on array with {len(current)} "
                            f"elements exceeds maximum of {max_wildcard}"
                        )
                    steps += len(current)
                    for item in current:
                        next_values.append(item)
                else:
                    # Wildcard on scalar: no expansion, but still costs 1 step
                    steps += 1

        current_values = next_values

    return (current_values, steps)


def _compare_value(value: Any, op: str, literal: LiteralValue) -> bool:
    """Compare a resolved value against a literal using the given operator.

    Type semantics (section E.3):
    - Numeric comparisons require both operands numeric (int or Decimal).
    - String comparisons: lexicographic by Unicode code point.
    - Boolean: only ``=`` and ``!=`` supported.
    - null: only ``=`` and ``!=`` supported.
    """
    lit_val = literal.value
    lit_type = literal.type

    # null comparisons
    if lit_type == "null":
        if op not in ("=", "!="):
            return False
        if op == "=":
            return value is None
        else:  # !=
            return value is not None

    # boolean comparisons
    if lit_type == "boolean":
        if op not in ("=", "!="):
            return False
        if not isinstance(value, bool):
            return False
        if op == "=":
            return value == lit_val
        else:
            return value != lit_val

    # string comparisons
    if lit_type == "string":
        if not isinstance(value, str):
            return False
        return _apply_ordered_op(value, op, lit_val)

    # numeric comparisons (int or decimal)
    if lit_type in ("int", "decimal"):
        # Value must be numeric. In Python, bool is a subclass of int,
        # so we must exclude booleans explicitly.
        if isinstance(value, bool):
            return False
        if isinstance(value, int):
            value_d = Decimal(value)
        elif isinstance(value, Decimal):
            value_d = value
        elif isinstance(value, float):
            # Convert float from JSON to Decimal for comparison
            value_d = Decimal(str(value))
        else:
            return False

        lit_d = Decimal(lit_val) if isinstance(lit_val, int) else lit_val
        return _apply_ordered_op(value_d, op, lit_d)

    return False


def _apply_ordered_op(left: Any, op: str, right: Any) -> bool:
    """Apply a comparison operator to two ordered values."""
    if op == "=":
        return left == right
    elif op == "!=":
        return left != right
    elif op == "<":
        return left < right
    elif op == "<=":
        return left <= right
    elif op == ">":
        return left > right
    elif op == ">=":
        return left >= right
    else:
        raise ValueError(f"Unknown comparison operator: {op!r}")
