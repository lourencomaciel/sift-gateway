"""Parse and evaluate where-DSL filter expressions.

Provide a small expression language supporting comparison
operators (eq, ne, gt, gte, lt, lte), IN/CONTAINS predicates,
EXISTS checks, and boolean combinators (AND, OR, NOT).
Expressions can be supplied as text strings or pre-parsed AST
dicts.  Key exports are ``parse_where_expression``,
``evaluate_where``, and ``canonicalize_where_ast``.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Callable, Mapping

from sidepouch_mcp.canon.rfc8785 import canonical_bytes
from sidepouch_mcp.query.jsonpath import (
    evaluate_jsonpath,
    parse_jsonpath,
)


class WhereDslError(ValueError):
    """Raised for invalid or unsupported where-DSL expressions.

    Covers tokenization errors, parse failures, type mismatches
    in comparisons, and unsupported operators.
    """


class WhereComputeLimitExceededError(WhereDslError):
    """Raised when where evaluation exceeds the compute step budget.

    Protects against denial-of-service from deeply nested or
    wildcard-heavy expressions.
    """


def _canonicalize(value: Any) -> Any:
    """Recursively canonicalize a where-AST value.

    Sort object keys alphabetically and sort AND/OR clause
    lists by their canonical byte representation for stable
    hashing.

    Args:
        value: Raw AST node (dict, list, or scalar).

    Returns:
        Canonical copy of the value.
    """
    if isinstance(value, dict):
        canonical = {k: _canonicalize(value[k]) for k in sorted(value)}
        op = canonical.get("op")
        clauses = canonical.get("clauses")
        if op in {"and", "or"} and isinstance(clauses, list):
            keyed = [(canonical_bytes(item), item) for item in clauses]
            canonical["clauses"] = [
                item for _, item in sorted(keyed, key=lambda pair: pair[0])
            ]
        return canonical
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    return value


def canonicalize_where_ast(
    where: Mapping[str, Any],
) -> dict[str, Any]:
    """Canonicalize a where-clause AST for deterministic hashing.

    Sort keys and commutative clause lists so that
    semantically equivalent expressions produce identical
    canonical forms.

    Args:
        where: Parsed where-clause AST dict.

    Returns:
        New dict with canonical key and clause ordering.
    """
    return _canonicalize(dict(where))


@dataclass(frozen=True)
class _Token:
    """Single lexical token from where-DSL tokenization.

    Attributes:
        kind: Token type (e.g. "OP", "STRING", "KEYWORD").
        value: Raw token text or parsed string content.
        offset: Character offset in the source string.
    """

    kind: str
    value: str
    offset: int


_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_KEYWORDS = {
    "AND",
    "OR",
    "NOT",
    "IN",
    "CONTAINS",
    "EXISTS",
    "TRUE",
    "FALSE",
    "NULL",
}
_OP_MAP = {
    "=": "eq",
    "==": "eq",
    "!=": "ne",
    ">": "gt",
    ">=": "gte",
    "<": "lt",
    "<=": "lte",
}

_SINGLE_CHAR_TOKENS: dict[str, str] = {
    "(": "LPAREN",
    ")": "RPAREN",
    "[": "LBRACKET",
    "]": "RBRACKET",
    ",": "COMMA",
    ".": "DOT",
    "*": "STAR",
}

_STRING_ESCAPE_MAP: dict[str, str] = {
    "n": "\n",
    "r": "\r",
    "t": "\t",
    "\\": "\\",
    "'": "'",
    '"': '"',
}


def _tokenize_string(
    where: str,
    start: int,
    quote: str,
) -> tuple[_Token, int]:
    """Parse a quoted string literal after the opening quote.

    Args:
        where: Full where-expression source string.
        start: Position immediately after the opening quote.
        quote: Quote character (single or double).

    Returns:
        Tuple of the STRING token and the new scan position.

    Raises:
        WhereDslError: If the string has an unterminated
            escape or missing closing quote.
    """
    i = start
    out: list[str] = []
    while i < len(where):
        cur = where[i]
        if cur == "\\":
            i += 1
            if i >= len(where):
                msg = "unterminated string escape"
                raise WhereDslError(msg)
            esc = where[i]
            if esc not in _STRING_ESCAPE_MAP:
                msg = f"unsupported string escape: \\{esc}"
                raise WhereDslError(msg)
            out.append(_STRING_ESCAPE_MAP[esc])
            i += 1
            continue
        if cur == quote:
            i += 1
            break
        out.append(cur)
        i += 1
    else:
        msg = "unterminated string literal"
        raise WhereDslError(msg)
    return _Token(kind="STRING", value="".join(out), offset=i), i


def _tokenize_number(
    where: str,
    start: int,
) -> tuple[_Token, int]:
    """Parse a numeric literal (integer or decimal).

    Args:
        where: Full where-expression source string.
        start: Position of the first digit or minus sign.

    Returns:
        Tuple of the NUMBER token and the new scan
        position.

    Raises:
        WhereDslError: If the numeric literal is malformed.
    """
    i = start + 1
    while i < len(where) and where[i].isdigit():
        i += 1
    if i < len(where) and where[i] == ".":
        i += 1
        if i >= len(where) or not where[i].isdigit():
            msg = "invalid numeric literal"
            raise WhereDslError(msg)
        while i < len(where) and where[i].isdigit():
            i += 1
    return (
        _Token(
            kind="NUMBER",
            value=where[start:i],
            offset=start,
        ),
        i,
    )


def _tokenize(where: str) -> list[_Token]:
    """Tokenize a where-DSL expression string.

    Produce a list of tokens including operators, keywords,
    identifiers, string/number literals, and punctuation,
    terminated by an EOF sentinel.

    Args:
        where: Raw where-expression source string.

    Returns:
        Token list ending with an EOF token.

    Raises:
        WhereDslError: If an unexpected character or
            malformed literal is encountered.
    """
    tokens: list[_Token] = []
    i = 0
    while i < len(where):
        ch = where[i]
        if ch.isspace():
            i += 1
            continue

        # Two-char operators
        if (
            where.startswith(">=", i)
            or where.startswith("<=", i)
            or where.startswith("!=", i)
            or where.startswith("==", i)
        ):
            tokens.append(
                _Token(
                    kind="OP",
                    value=where[i : i + 2],
                    offset=i,
                )
            )
            i += 2
            continue

        # Single-char operators
        if ch in {">", "<", "="}:
            tokens.append(_Token(kind="OP", value=ch, offset=i))
            i += 1
            continue

        # Single-char punctuation
        kind = _SINGLE_CHAR_TOKENS.get(ch)
        if kind is not None:
            tokens.append(_Token(kind=kind, value=ch, offset=i))
            i += 1
            continue

        # String literal
        if ch in {"'", '"'}:
            tok, i = _tokenize_string(where, i + 1, ch)
            tokens.append(tok)
            continue

        # Number literal
        if ch.isdigit() or (
            ch == "-" and i + 1 < len(where) and where[i + 1].isdigit()
        ):
            tok, i = _tokenize_number(where, i)
            tokens.append(tok)
            continue

        # Identifier or keyword
        match = _IDENT_RE.match(where, i)
        if match:
            raw = match.group(0)
            upper = raw.upper()
            if upper in _KEYWORDS:
                tokens.append(
                    _Token(
                        kind="KEYWORD",
                        value=upper,
                        offset=i,
                    )
                )
            else:
                tokens.append(_Token(kind="IDENT", value=raw, offset=i))
            i = match.end()
            continue

        msg = f"unexpected token at offset {i}"
        raise WhereDslError(msg)

    tokens.append(_Token(kind="EOF", value="", offset=len(where)))
    return tokens


def _escape_bracket_key(key: str) -> str:
    """Escape a key for bracket-quoted JSONPath notation.

    Args:
        key: Raw key string to escape.

    Returns:
        Escaped key safe for use inside ``['...']``.
    """
    return (
        key.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


def _merge_logical(
    op: str, left: dict[str, Any], right: dict[str, Any]
) -> dict[str, Any]:
    """Merge two AST nodes under a logical AND or OR.

    Flatten nested clauses of the same operator to avoid
    unnecessary nesting depth.

    Args:
        op: Logical operator (``"and"`` or ``"or"``).
        left: Left-hand AST node.
        right: Right-hand AST node.

    Returns:
        Combined AST node with a flat clauses list.
    """
    clauses: list[dict[str, Any]] = []
    if left.get("op") == op and isinstance(left.get("clauses"), list):
        clauses.extend(left["clauses"])
    else:
        clauses.append(left)
    if right.get("op") == op and isinstance(right.get("clauses"), list):
        clauses.extend(right["clauses"])
    else:
        clauses.append(right)
    return {"op": op, "clauses": clauses}


def _parse_bracket_segment(parser: _Parser) -> str:
    """Parse the contents of a bracket path segment.

    Args:
        parser: Active parser instance positioned after
            the opening bracket token.

    Returns:
        Bracket segment string (e.g. ``[*]``, ``[0]``,
        or ``['key']``).

    Raises:
        WhereDslError: If the bracket content is invalid.
    """
    if parser._match("STAR") is not None:
        return "[*]"
    if parser._peek().kind == "NUMBER":
        number = parser._advance().value
        if not number.isdigit():
            msg = "array index must be a non-negative integer"
            raise WhereDslError(msg)
        return f"[{number}]"
    if parser._peek().kind == "STRING":
        key = parser._advance().value
        return f"['{_escape_bracket_key(key)}']"
    cur = parser._peek()
    msg = f"invalid bracket path segment at offset {cur.offset}"
    raise WhereDslError(msg)


class _Parser:
    """Recursive-descent parser for where-DSL token streams.

    Consume tokens produced by ``_tokenize`` and build an AST
    dict representing the where expression with support for
    AND/OR/NOT combinators and leaf predicates.
    """

    def __init__(self, tokens: list[_Token]) -> None:
        """Initialize parser with a token list.

        Args:
            tokens: Token sequence from ``_tokenize``.
        """
        self._tokens = tokens
        self._pos = 0

    def _peek(self) -> _Token:
        """Return the current token without advancing.

        Returns:
            The token at the current position.
        """
        return self._tokens[self._pos]

    def _advance(self) -> _Token:
        """Consume and return the current token.

        Returns:
            The token that was at the current position.
        """
        tok = self._tokens[self._pos]
        self._pos += 1
        return tok

    def _match(self, kind: str, value: str | None = None) -> _Token | None:
        """Consume the current token if it matches kind and value.

        Args:
            kind: Expected token kind.
            value: Optional expected token value.

        Returns:
            The matched token, or None if no match.
        """
        tok = self._peek()
        if tok.kind != kind:
            return None
        if value is not None and tok.value != value:
            return None
        return self._advance()

    def _expect(self, kind: str, value: str | None = None) -> _Token:
        """Consume the current token or raise on mismatch.

        Args:
            kind: Expected token kind.
            value: Optional expected token value.

        Returns:
            The consumed token.

        Raises:
            WhereDslError: If the current token does not
                match.
        """
        tok = self._match(kind, value)
        if tok is None:
            cur = self._peek()
            expected = f"{kind} {value}" if value is not None else kind
            msg = f"expected {expected} at offset {cur.offset}"
            raise WhereDslError(msg)
        return tok

    def parse(self) -> dict[str, Any]:
        """Parse the full token stream into an AST dict.

        Returns:
            Root AST node for the where expression.

        Raises:
            WhereDslError: If tokens cannot be parsed or
                the stream does not end cleanly.
        """
        expr = self._parse_or()
        self._expect("EOF")
        return expr

    def _parse_or(self) -> dict[str, Any]:
        """Parse an OR-level expression.

        Returns:
            AST node, possibly wrapping OR clauses.
        """
        expr = self._parse_and()
        while self._match("KEYWORD", "OR") is not None:
            expr = _merge_logical("or", expr, self._parse_and())
        return expr

    def _parse_and(self) -> dict[str, Any]:
        """Parse an AND-level expression.

        Returns:
            AST node, possibly wrapping AND clauses.
        """
        expr = self._parse_not()
        while self._match("KEYWORD", "AND") is not None:
            expr = _merge_logical("and", expr, self._parse_not())
        return expr

    def _parse_not(self) -> dict[str, Any]:
        """Parse a NOT-prefixed expression.

        Returns:
            AST node, possibly wrapping a NOT clause.
        """
        if self._match("KEYWORD", "NOT") is not None:
            return {
                "op": "not",
                "clause": self._parse_not(),
            }
        return self._parse_primary()

    def _parse_primary(self) -> dict[str, Any]:
        """Parse a parenthesized group or leaf predicate.

        Returns:
            AST node for the primary expression.
        """
        if self._match("LPAREN") is not None:
            expr = self._parse_or()
            self._expect("RPAREN")
            return expr
        return self._parse_predicate()

    def _parse_predicate(self) -> dict[str, Any]:
        """Parse a leaf predicate (comparison, IN, etc.).

        Returns:
            AST dict with ``path``, ``op``, and optional
            ``value`` keys.

        Raises:
            WhereDslError: If the predicate syntax is
                invalid.
        """
        if self._match("KEYWORD", "EXISTS") is not None:
            self._expect("LPAREN")
            path = self._parse_path()
            self._expect("RPAREN")
            return {"path": path, "op": "exists"}

        path = self._parse_path()
        if self._match("KEYWORD", "IN") is not None:
            values = self._parse_array_literal()
            return {
                "path": path,
                "op": "in",
                "value": values,
            }
        if self._match("KEYWORD", "CONTAINS") is not None:
            value = self._parse_literal()
            return {
                "path": path,
                "op": "contains",
                "value": value,
            }

        op_token = self._expect("OP")
        op = _OP_MAP.get(op_token.value)
        if op is None:
            msg = f"unsupported comparison operator: {op_token.value}"
            raise WhereDslError(msg)
        value = self._parse_literal()
        return {"path": path, "op": op, "value": value}

    def _parse_path(self) -> str:
        """Parse a dotted/bracketed field path.

        Returns:
            Concatenated path string (e.g. ``foo.bar[0]``).

        Raises:
            WhereDslError: If the path is empty or
                malformed.
        """
        token = self._peek()
        if token.kind not in {"IDENT", "LBRACKET"}:
            msg = f"expected path at offset {token.offset}"
            raise WhereDslError(msg)

        parts: list[str] = []
        if self._match("IDENT") is not None:
            parts.append(self._tokens[self._pos - 1].value)

        while True:
            if self._match("DOT") is not None:
                ident = self._expect("IDENT")
                parts.append(f".{ident.value}")
                continue
            if self._match("LBRACKET") is not None:
                segment = _parse_bracket_segment(self)
                self._expect("RBRACKET")
                parts.append(segment)
                continue
            break

        if not parts:
            msg = "empty path is not allowed"
            raise WhereDslError(msg)
        return "".join(parts)

    def _parse_array_literal(self) -> list[Any]:
        """Parse a bracket-delimited array of literals.

        Returns:
            List of parsed literal values.

        Raises:
            WhereDslError: If bracket syntax is invalid.
        """
        self._expect("LBRACKET")
        values: list[Any] = []
        if self._match("RBRACKET") is not None:
            return values
        while True:
            values.append(self._parse_literal())
            if self._match("COMMA") is None:
                break
        self._expect("RBRACKET")
        return values

    def _parse_literal(self) -> Any:
        """Parse a scalar literal value.

        Returns:
            A string, int, float, bool, or None.

        Raises:
            WhereDslError: If the token is not a valid
                literal.
        """
        token = self._peek()
        if token.kind == "STRING":
            return self._advance().value
        if token.kind == "NUMBER":
            raw = self._advance().value
            if "." in raw:
                return float(raw)
            return int(raw)
        if token.kind == "KEYWORD":
            keyword = token.value
            if keyword == "TRUE":
                self._advance()
                return True
            if keyword == "FALSE":
                self._advance()
                return False
            if keyword == "NULL":
                self._advance()
                return None
        msg = f"expected literal at offset {token.offset}"
        raise WhereDslError(msg)


def parse_where_expression(where: str) -> dict[str, Any]:
    """Parse a textual where expression into an AST dict.

    Tokenize and parse the expression using a recursive-
    descent parser that supports AND, OR, NOT, comparison
    operators, IN, CONTAINS, and EXISTS predicates.

    Args:
        where: Non-empty where-expression string.

    Returns:
        Parsed AST dict suitable for ``evaluate_where``
        or ``canonicalize_where_ast``.

    Raises:
        WhereDslError: If the string is empty or contains
            syntax errors.
    """
    if not where.strip():
        msg = "where expression string must be non-empty"
        raise WhereDslError(msg)
    parser = _Parser(_tokenize(where))
    return parser.parse()


def _absolute_path(path: str) -> str:
    """Prepend ``$`` or ``$.`` to make a path absolute.

    Args:
        path: Relative or absolute JSONPath string.

    Returns:
        Absolute JSONPath starting with ``$``.
    """
    if path.startswith("$"):
        return path
    if path.startswith("["):
        return f"${path}"
    return f"$.{path}"


def _is_numeric(value: Any) -> bool:
    """Return True if value is int or float but not bool.

    Args:
        value: Value to test.

    Returns:
        True for numeric non-bool values.
    """
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _strict_eq(left: Any, right: Any) -> bool:
    """Compare two values with type-strict equality.

    Prevent Python's implicit bool/int coercion so that
    ``True != 1`` in filter expressions.

    Args:
        left: First value.
        right: Second value.

    Returns:
        True if the values are equal and type-compatible.
    """
    if isinstance(left, bool) != isinstance(right, bool):
        return False
    return left == right


def _any_in_match(values: list[Any], right: Any) -> bool:
    """Check if any value matches any item in the set.

    Use type-strict equality to avoid bool/int coercion.

    Args:
        values: Candidate values from the document.
        right: Collection to match against.

    Returns:
        True if at least one strict-equal pair exists.
    """
    for left in values:
        for item in right:
            if _strict_eq(left, item):
                return True
    return False


def _ordered_compare(left: Any, right: Any, op: str) -> bool:
    """Perform an ordered comparison (gt, gte, lt, lte).

    Args:
        left: Left operand.
        right: Right operand.
        op: One of ``"gt"``, ``"gte"``, ``"lt"``, ``"lte"``.

    Returns:
        Result of the comparison.
    """
    if op == "gt":
        return left > right
    if op == "gte":
        return left >= right
    if op == "lt":
        return left < right
    return left <= right


def _values_at_path(
    record: Any,
    path: str,
    *,
    consume_steps: Callable[[int], None],
    max_wildcard_expansion: int,
) -> list[Any]:
    """Evaluate a path and return matched values.

    Convert the path to absolute form, parse and evaluate
    it, then charge compute steps for segments and results.

    Args:
        record: JSON-compatible document to query.
        path: Relative or absolute JSONPath string.
        consume_steps: Callback to charge compute steps.
        max_wildcard_expansion: Max values from wildcards.

    Returns:
        List of matched values (may be empty).

    Raises:
        WhereDslError: If wildcard expansion exceeds the
            configured limit.
    """
    absolute = _absolute_path(path)
    segments = parse_jsonpath(absolute)
    consume_steps(len(segments))
    values = evaluate_jsonpath(record, absolute)
    wildcard_count = sum(1 for seg in segments if seg.kind == "wildcard")
    if wildcard_count > 0 and len(values) > max_wildcard_expansion:
        msg = "where wildcard expansion exceeds configured limit"
        raise WhereDslError(msg)
    consume_steps(len(values))
    return values


def _eval_predicate(
    record: Any,
    expr: Mapping[str, Any],
    *,
    consume_steps: Callable[[int], None],
    max_wildcard_expansion: int,
) -> bool:
    """Evaluate a single leaf predicate against a record.

    Dispatch to the appropriate comparison logic based on
    the ``op`` field: eq, ne, gt, gte, lt, lte, in,
    contains, or exists.

    Args:
        record: JSON-compatible document to test.
        expr: Predicate AST with path, op, and value.
        consume_steps: Callback to charge compute steps.
        max_wildcard_expansion: Max values from wildcards.

    Returns:
        True if the predicate matches the record.

    Raises:
        WhereDslError: If the predicate is malformed,
            operand types are incompatible, or the operator
            is unsupported.
    """
    path = expr.get("path")
    op = expr.get("op")
    if not isinstance(path, str) or not isinstance(op, str):
        msg = "predicate requires string path and op"
        raise WhereDslError(msg)

    if not path:
        msg = "predicate path must be non-empty"
        raise WhereDslError(msg)

    values = _values_at_path(
        record,
        path,
        consume_steps=consume_steps,
        max_wildcard_expansion=max_wildcard_expansion,
    )
    exists = len(values) > 0
    right = expr.get("value")
    consume_steps(1)

    if op == "exists":
        return exists
    if op == "eq":
        return any(_strict_eq(left, right) for left in values)
    if op == "ne":
        if not exists:
            return right is None
        return any(not _strict_eq(left, right) for left in values)
    if op in {"gt", "gte", "lt", "lte"}:
        if not exists:
            return False
        if _is_numeric(right):
            if any(not _is_numeric(left) for left in values):
                msg = f"{op} operator requires numeric operands"
                raise WhereDslError(msg)
            return any(_ordered_compare(left, right, op) for left in values)
        if isinstance(right, str):
            if any(not isinstance(left, str) for left in values):
                msg = f"{op} operator requires string operands"
                raise WhereDslError(msg)
            return any(_ordered_compare(left, right, op) for left in values)
        msg = f"{op} operator requires numeric or string comparison value"
        raise WhereDslError(msg)
    if op == "in":
        if not isinstance(right, (list, tuple, set)):
            msg = "in operator requires array/set value"
            raise WhereDslError(msg)
        return _any_in_match(values, right)
    if op == "contains":
        if not exists:
            return False
        for left in values:
            if (
                isinstance(left, str)
                and isinstance(right, str)
                and right in left
            ):
                return True
            if isinstance(left, (list, tuple, set)) and right in left:
                return True
        return False

    msg = f"unsupported where op: {op}"
    raise WhereDslError(msg)


def evaluate_where(
    record: Any,
    where: Mapping[str, Any] | str,
    *,
    max_compute_steps: int = 1_000_000,
    max_wildcard_expansion: int = 10_000,
) -> bool:
    """Evaluate a where expression against a JSON record.

    Accept either a pre-parsed AST dict or a raw expression
    string.  Walk the AST recursively, enforcing compute
    step and wildcard expansion budgets.

    Args:
        record: JSON-compatible document to filter.
        where: Where expression as an AST dict or string.
        max_compute_steps: Total compute step budget.
        max_wildcard_expansion: Max values per wildcard.

    Returns:
        True if the record matches the expression.

    Raises:
        WhereDslError: If the expression is malformed.
        WhereComputeLimitExceededError: If the compute step
            budget is exhausted.
    """
    where_expr: Mapping[str, Any]
    if isinstance(where, str):
        where_expr = parse_where_expression(where)
    elif isinstance(where, Mapping):
        where_expr = where
    else:
        msg = "where expression must be an object or string"
        raise WhereDslError(msg)

    steps = 0

    def consume_steps(amount: int = 1) -> None:
        nonlocal steps
        steps += max(amount, 0)
        if steps > max_compute_steps:
            msg = "where compute step budget exceeded"
            raise WhereComputeLimitExceededError(msg)

    def walk(expr: Mapping[str, Any]) -> bool:
        consume_steps(1)

        op = expr.get("op")
        if op == "and":
            clauses = expr.get("clauses")
            if not isinstance(clauses, list):
                msg = "and requires clauses list"
                raise WhereDslError(msg)
            if any(not isinstance(clause, Mapping) for clause in clauses):
                msg = "and clauses must contain objects"
                raise WhereDslError(msg)
            return all(walk(clause) for clause in clauses)
        if op == "or":
            clauses = expr.get("clauses")
            if not isinstance(clauses, list):
                msg = "or requires clauses list"
                raise WhereDslError(msg)
            if any(not isinstance(clause, Mapping) for clause in clauses):
                msg = "or clauses must contain objects"
                raise WhereDslError(msg)
            return any(walk(clause) for clause in clauses)
        if op == "not":
            clause = expr.get("clause")
            if not isinstance(clause, Mapping):
                msg = "not requires clause object"
                raise WhereDslError(msg)
            return not walk(clause)

        return _eval_predicate(
            record,
            expr,
            consume_steps=consume_steps,
            max_wildcard_expansion=max_wildcard_expansion,
        )

    return walk(where_expr)
