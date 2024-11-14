import re
from collections.abc import Generator
from enum import Enum, auto
from typing import Any, NamedTuple, Optional


class InfixOperator(Enum):
    #     lb  rb
    AND = (3, 4)
    OR = (1, 2)


class PrefixOperator(Enum):
    NOT = 5


Operator = InfixOperator | PrefixOperator


class TokenType(Enum):
    LPAR = auto()
    RPAR = auto()
    OPERATOR = auto()
    INFIX_OPERATOR = auto()
    FAULT = auto()


class Token(NamedTuple):
    token_type: TokenType
    value: Any


class TokenStream:
    def __init__(self, tokens: list[Token]):
        self.tokens = tokens
        self.idx = 0

    def __next__(self) -> Token:
        if self.idx >= len(self.tokens):
            raise StopIteration()
        value = self.tokens[self.idx]
        self.idx += 1
        return value

    def peek(self) -> Optional[Token]:
        if self.idx < len(self.tokens):
            return self.tokens[self.idx]
        return None


def lex(expression: str) -> TokenStream:
    i = 0
    tokens = []
    while i < len(expression):
        if expression[i].isspace():
            i += 1
            continue
        elif expression[i] == "&":
            tokens.append(Token(TokenType.OPERATOR, InfixOperator.AND))
            i += 1
        elif expression[i] == "|":
            tokens.append(Token(TokenType.OPERATOR, InfixOperator.OR))
            i += 1
        elif expression[i] == "!":
            tokens.append(Token(TokenType.INFIX_OPERATOR, PrefixOperator.NOT))
            i += 1
        elif expression[i] == "(":
            tokens.append(Token(TokenType.LPAR, None))
            i += 1
        elif expression[i] == ")":
            tokens.append(Token(TokenType.RPAR, None))
            i += 1
        else:
            fault_name = re.match(r"^[a-zA-Z0-9\-_: ]+", expression[i:])
            if not fault_name:
                raise ValueError(f"Invalid search string {expression}")
            tokens.append(Token(TokenType.FAULT, fault_name.group(0).strip()))
            i += len(fault_name.group(0))
    return TokenStream(tokens)


ExpressionTree = str | dict[Operator, str | tuple["ExpressionTree", "ExpressionTree"]]


def parse(expression: str) -> ExpressionTree:
    tokens = lex(expression)

    def expr_binding_power(token_iterator: TokenStream, min_binding_power: int):
        token = next(token_iterator)

        match token:
            case Token(token_type=TokenType.LPAR):
                inner = expr_binding_power(token_iterator, 0)
                if next(token_iterator).token_type != TokenType.RPAR:
                    raise ValueError(f"Invalid search expression {expression}")
                lhs = inner
            case Token(token_type=TokenType.INFIX_OPERATOR, value=op):
                lhs = {op: expr_binding_power(token_iterator, op.value)}
            case Token(token_type=TokenType.FAULT, value=fault_name):
                lhs = fault_name
            case _:
                raise ValueError(f"Invalid search expression {expression}")

        while True:
            match token_iterator.peek():
                case Token(token_type=TokenType.RPAR) | None:
                    break
                case Token(token_type=TokenType.OPERATOR, value=op):
                    operator = op
                case _:
                    raise ValueError(f"Invalid search expression {expression}")

            (left_bind_power, right_bind_power) = op.value
            if left_bind_power < min_binding_power:
                break

            next(token_iterator)

            rhs = expr_binding_power(token_iterator, right_bind_power)

            lhs = {operator: (lhs, rhs)}
        return lhs

    return expr_binding_power(tokens, 0)


def to_sql(
    query: str,
    magnitude_bounds: tuple[Optional[float], Optional[float]] = (None, None),
    rate_bounds: tuple[Optional[float], Optional[float]] = (None, None),
    limit: float = 100,
    fault_count_limit: int = None,
) -> tuple[str, tuple[Any, ...]]:
    expression = parse(query)

    def expression_to_sql(expression: ExpressionTree) -> str:
        match expression:
            case {InfixOperator.AND: (lhs, rhs)}:
                return f"({expression_to_sql(lhs)}) AND ({expression_to_sql(rhs)})"
            case {InfixOperator.OR: (lhs, rhs)}:
                return f"({expression_to_sql(lhs)}) OR ({expression_to_sql(rhs)})"
            case {PrefixOperator.NOT: expr} if isinstance(expr, str) or isinstance(
                expr, ExpressionTree
            ):
                return f"(NOT {expression_to_sql(expr)})"
            case expression if isinstance(expression, str):
                return "SUM(CASE WHEN parent_fault.name = ? THEN 1 ELSE 0 END) > 0"
            case _:
                raise ValueError("Invalid expression")

    def query_parameters(expression: ExpressionTree) -> Generator[str]:
        match expression:
            case {InfixOperator.AND: (lhs, rhs)}:
                yield from query_parameters(lhs)
                yield from query_parameters(rhs)
            case {InfixOperator.OR: (lhs, rhs)}:
                yield from query_parameters(lhs)
                yield from query_parameters(rhs)
            case {PrefixOperator.NOT: expr} if isinstance(expr, str) or isinstance(
                expr, ExpressionTree
            ):
                yield from query_parameters(expr)
            case fault_name if isinstance(fault_name, str):
                yield fault_name
            case _:
                raise ValueError("Invalid expression")

    fault_count_expression = ""
    if fault_count_limit:
        fault_count_expression = "AND COUNT(DISTINCT parent_fault.parent_id) <= ?"
    sql_expression = f"""SELECT
     rupture.rupture_id, ANY_VALUE(rupture.magnitude), ANY_VALUE(rupture.area), ANY_VALUE(rupture.len), ANY_VALUE(rupture.rate)
    FROM rupture
    JOIN
        rupture_faults ON rupture.rupture_id = rupture_faults.rupture_id
    JOIN
        fault ON rupture_faults.fault_id = fault.fault_id
    JOIN
        parent_fault ON fault.parent_id = parent_fault.parent_id
    WHERE rupture.rate IS NOT NULL AND rupture.magnitude >= ? AND rupture.magnitude <= ? AND rupture.rate >= ? AND rupture.rate <= ?
    GROUP BY rupture.rupture_id
    HAVING ({expression_to_sql(expression)}) {fault_count_expression}
    ORDER BY ANY_VALUE(rupture.rate)
    DESC NULLS LAST
    LIMIT ?
    """
    parameters = (
        magnitude_bounds[0] or 6,
        magnitude_bounds[1] or 10,
        rate_bounds[0] or -20,
        rate_bounds[1] or 0,
    ) + tuple(query_parameters(expression))
    if fault_count_limit:
        parameters = parameters + (fault_count_limit,)
    parameters = parameters + (limit,)

    return (sql_expression, parameters)
