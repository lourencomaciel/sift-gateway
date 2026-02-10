"""Minimal where-DSL evaluator."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Callable, Mapping

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.query.jsonpath import evaluate_jsonpath, parse_jsonpath


class WhereDslError(ValueError):
    """Raised for invalid where DSL expressions."""


class WhereComputeLimitExceeded(WhereDslError):
    """Raised when compute steps exceed configured limit."""


def _canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        canonical = {k: _canonicalize(value[k]) for k in sorted(value)}
        op = canonical.get("op")
        clauses = canonical.get("clauses")
        if op in {"and", "or"} and isinstance(clauses, list):
            keyed = [(canonical_bytes(item), item) for item in clauses]
            canonical["clauses"] = [item for _, item in sorted(keyed, key=lambda pair: pair[0])]
        return canonical
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    return value


def canonicalize_where_ast(where: Mapping[str, Any]) -> dict[str, Any]:
    return _canonicalize(dict(where))


@dataclass(frozen=True)
class _Token:
    kind: str
    value: str
    offset: int


_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_KEYWORDS = {"AND", "OR", "NOT", "IN", "CONTAINS", "EXISTS", "TRUE", "FALSE", "NULL"}
_OP_MAP = {
    "=": "eq",
    "==": "eq",
    "!=": "ne",
    ">": "gt",
    ">=": "gte",
    "<": "lt",
    "<=": "lte",
}


def _tokenize(where: str) -> list[_Token]:
    tokens: list[_Token] = []
    i = 0
    while i < len(where):
        ch = where[i]
        if ch.isspace():
            i += 1
            continue
        if (
            where.startswith(">=", i)
            or where.startswith("<=", i)
            or where.startswith("!=", i)
            or where.startswith("==", i)
        ):
            tokens.append(_Token(kind="OP", value=where[i : i + 2], offset=i))
            i += 2
            continue
        if ch in {">", "<", "="}:
            tokens.append(_Token(kind="OP", value=ch, offset=i))
            i += 1
            continue
        if ch == "(":
            tokens.append(_Token(kind="LPAREN", value=ch, offset=i))
            i += 1
            continue
        if ch == ")":
            tokens.append(_Token(kind="RPAREN", value=ch, offset=i))
            i += 1
            continue
        if ch == "[":
            tokens.append(_Token(kind="LBRACKET", value=ch, offset=i))
            i += 1
            continue
        if ch == "]":
            tokens.append(_Token(kind="RBRACKET", value=ch, offset=i))
            i += 1
            continue
        if ch == ",":
            tokens.append(_Token(kind="COMMA", value=ch, offset=i))
            i += 1
            continue
        if ch == ".":
            tokens.append(_Token(kind="DOT", value=ch, offset=i))
            i += 1
            continue
        if ch == "*":
            tokens.append(_Token(kind="STAR", value=ch, offset=i))
            i += 1
            continue
        if ch in {"'", '"'}:
            quote = ch
            i += 1
            out: list[str] = []
            while i < len(where):
                cur = where[i]
                if cur == "\\":
                    i += 1
                    if i >= len(where):
                        msg = "unterminated string escape"
                        raise WhereDslError(msg)
                    esc = where[i]
                    escape_map = {
                        "n": "\n",
                        "r": "\r",
                        "t": "\t",
                        "\\": "\\",
                        "'": "'",
                        '"': '"',
                    }
                    if esc not in escape_map:
                        msg = f"unsupported string escape: \\{esc}"
                        raise WhereDslError(msg)
                    out.append(escape_map[esc])
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
            tokens.append(_Token(kind="STRING", value="".join(out), offset=i))
            continue
        if ch.isdigit() or (ch == "-" and i + 1 < len(where) and where[i + 1].isdigit()):
            start = i
            i += 1
            while i < len(where) and where[i].isdigit():
                i += 1
            if i < len(where) and where[i] == ".":
                i += 1
                if i >= len(where) or not where[i].isdigit():
                    msg = "invalid numeric literal"
                    raise WhereDslError(msg)
                while i < len(where) and where[i].isdigit():
                    i += 1
            tokens.append(_Token(kind="NUMBER", value=where[start:i], offset=start))
            continue

        match = _IDENT_RE.match(where, i)
        if match:
            raw = match.group(0)
            upper = raw.upper()
            if upper in _KEYWORDS:
                tokens.append(_Token(kind="KEYWORD", value=upper, offset=i))
            else:
                tokens.append(_Token(kind="IDENT", value=raw, offset=i))
            i = match.end()
            continue

        msg = f"unexpected token at offset {i}"
        raise WhereDslError(msg)

    tokens.append(_Token(kind="EOF", value="", offset=len(where)))
    return tokens


def _escape_bracket_key(key: str) -> str:
    return (
        key.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


def _merge_logical(op: str, left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
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


class _Parser:
    def __init__(self, tokens: list[_Token]) -> None:
        self._tokens = tokens
        self._pos = 0

    def _peek(self) -> _Token:
        return self._tokens[self._pos]

    def _advance(self) -> _Token:
        tok = self._tokens[self._pos]
        self._pos += 1
        return tok

    def _match(self, kind: str, value: str | None = None) -> _Token | None:
        tok = self._peek()
        if tok.kind != kind:
            return None
        if value is not None and tok.value != value:
            return None
        return self._advance()

    def _expect(self, kind: str, value: str | None = None) -> _Token:
        tok = self._match(kind, value)
        if tok is None:
            cur = self._peek()
            expected = f"{kind} {value}" if value is not None else kind
            msg = f"expected {expected} at offset {cur.offset}"
            raise WhereDslError(msg)
        return tok

    def parse(self) -> dict[str, Any]:
        expr = self._parse_or()
        self._expect("EOF")
        return expr

    def _parse_or(self) -> dict[str, Any]:
        expr = self._parse_and()
        while self._match("KEYWORD", "OR") is not None:
            expr = _merge_logical("or", expr, self._parse_and())
        return expr

    def _parse_and(self) -> dict[str, Any]:
        expr = self._parse_not()
        while self._match("KEYWORD", "AND") is not None:
            expr = _merge_logical("and", expr, self._parse_not())
        return expr

    def _parse_not(self) -> dict[str, Any]:
        if self._match("KEYWORD", "NOT") is not None:
            return {"op": "not", "clause": self._parse_not()}
        return self._parse_primary()

    def _parse_primary(self) -> dict[str, Any]:
        if self._match("LPAREN") is not None:
            expr = self._parse_or()
            self._expect("RPAREN")
            return expr
        return self._parse_predicate()

    def _parse_predicate(self) -> dict[str, Any]:
        if self._match("KEYWORD", "EXISTS") is not None:
            self._expect("LPAREN")
            path = self._parse_path()
            self._expect("RPAREN")
            return {"path": path, "op": "exists"}

        path = self._parse_path()
        if self._match("KEYWORD", "IN") is not None:
            values = self._parse_array_literal()
            return {"path": path, "op": "in", "value": values}
        if self._match("KEYWORD", "CONTAINS") is not None:
            value = self._parse_literal()
            return {"path": path, "op": "contains", "value": value}

        op_token = self._expect("OP")
        op = _OP_MAP.get(op_token.value)
        if op is None:
            msg = f"unsupported comparison operator: {op_token.value}"
            raise WhereDslError(msg)
        value = self._parse_literal()
        return {"path": path, "op": op, "value": value}

    def _parse_path(self) -> str:
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
                if self._match("STAR") is not None:
                    segment = "[*]"
                elif self._peek().kind == "NUMBER":
                    number = self._advance().value
                    if not number.isdigit():
                        msg = "array index must be a non-negative integer"
                        raise WhereDslError(msg)
                    segment = f"[{number}]"
                elif self._peek().kind == "STRING":
                    key = self._advance().value
                    segment = f"['{_escape_bracket_key(key)}']"
                else:
                    cur = self._peek()
                    msg = f"invalid bracket path segment at offset {cur.offset}"
                    raise WhereDslError(msg)
                self._expect("RBRACKET")
                parts.append(segment)
                continue
            break

        if not parts:
            msg = "empty path is not allowed"
            raise WhereDslError(msg)
        return "".join(parts)

    def _parse_array_literal(self) -> list[Any]:
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
    """Parse textual where expression into AST."""
    if not where.strip():
        msg = "where expression string must be non-empty"
        raise WhereDslError(msg)
    parser = _Parser(_tokenize(where))
    return parser.parse()


def _absolute_path(path: str) -> str:
    if path.startswith("$"):
        return path
    if path.startswith("["):
        return f"${path}"
    return f"$.{path}"


def _is_numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _strict_eq(left: Any, right: Any) -> bool:
    """Type-strict equality: prevents Python bool/int coercion (1 == True)."""
    if isinstance(left, bool) != isinstance(right, bool):
        return False
    return left == right


def _ordered_compare(left: Any, right: Any, op: str) -> bool:
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
        return any(any(_strict_eq(left, item) for item in right) for left in values)
    if op == "contains":
        if not exists:
            return False
        for left in values:
            if isinstance(left, str) and isinstance(right, str) and right in left:
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
            raise WhereComputeLimitExceeded(msg)

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
