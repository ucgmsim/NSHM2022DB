"""
This module provides functionality for parsing and lexing query expressions into tokens,
constructing expression trees, and converting these trees into SQL queries.
"""

import re
from collections.abc import Generator
from enum import Enum, auto
from typing import Any, NamedTuple, Optional


class InfixOperator(Enum):
    """Infix operators permitted in a query expression.

    Operators have an enum tuple of binding powers: (left power, right
    power). Operators with higher binding power have higher
    precedence. If left power < right power, then the operator is
    left-associative, otherwise the operator is right associative.
    """

    AND = (3, 4)
    OR = (1, 2)


class UnaryOperator(Enum):
    """Unary operators permitted in a query expression.

    Unary operator values are binding powers (see `InfixOperator`).
    """

    NOT = 5


Operator = InfixOperator | UnaryOperator


class TokenType(Enum):
    """The different token types permitted in a query."""

    LPAR = auto()
    RPAR = auto()
    UNARY_OPERATOR = auto()
    INFIX_OPERATOR = auto()
    FAULT = auto()


class Token(NamedTuple):
    """A single token in a lexed query expression."""

    token_type: TokenType
    """The type of the token"""
    value: Any
    """A value (for example, the kind of operator or the name of a fault)."""


class TokenStream:
    """A token iterator with support for peeking the next token."""

    def __init__(self, tokens: list[Token]):
        """Initialise a token stream from a list of tokens.

        Parameters
        ----------
        tokens : list[Token]
            The list of tokens.
        """
        self.tokens = tokens
        self.idx = 0

    def __next__(self) -> Token:
        """Return the next token in the token stream.

        Returns
        -------
        Token
            The next token in the token stream.

        Raises
        ------
        StopIteration
            If there are no more tokens to read.
        """
        if self.idx >= len(self.tokens):
            raise StopIteration()
        value = self.tokens[self.idx]
        self.idx += 1
        return value

    def peek(self) -> Optional[Token]:
        """Peek the next token in the stream.

        Unlike `TokenStream.__next__`, this does not advance the
        stream index, and will not crash if the stream is exhausted.

        Returns
        -------
        Optional[Token]
            The next token in the stream, or None if the token stream is exhausted.
        """
        if self.idx < len(self.tokens):
            return self.tokens[self.idx]
        return None


def lex(expression: str) -> TokenStream:
    """Lex a query expression into a token stream.

    Parameters
    ----------
    expression : str
        The query expression to lex.


    Returns
    -------
    TokenStream
        A stream of tokens extracted from the query expression.

    Raises
    ------
    ValueError
        If the query expression contains forbidden characters.
    """
    i = 0
    tokens = []
    while i < len(expression):
        if expression[i].isspace():
            i += 1
            continue
        elif expression[i] == "&":
            tokens.append(Token(TokenType.UNARY_OPERATOR, InfixOperator.AND))
            i += 1
        elif expression[i] == "|":
            tokens.append(Token(TokenType.UNARY_OPERATOR, InfixOperator.OR))
            i += 1
        elif expression[i] == "!":
            tokens.append(Token(TokenType.INFIX_OPERATOR, UnaryOperator.NOT))
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
    """Parse an expression string into an expression tree.

    Parameters
    ----------
    expression : str
        The query expression to parse

    Returns
    -------
    ExpressionTree
        The parsed query tree.

    Raises
    ------
    ValueError
        If the query expression is invalid.
    """
    tokens = lex(expression)

    def expr_binding_power(token_iterator: TokenStream, min_binding_power: int):
        """ """
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
                case Token(token_type=TokenType.UNARY_OPERATOR, value=op):
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
    limit: int = 100,
    fault_count_limit: Optional[int] = None,
) -> tuple[str, tuple[Any, ...]]:
    """Construct a DuckDB SQL query using a rich expression language and variable bounds.

    The query parameter is expected to be a string that expresses the
    logical inclusion of some faults in the desired ruptures.

    Parameters
    ----------
    query : str
        The query string.
    magnitude_bounds : tuple[Optional[float], Optional[float]]
        Optional bounds on the magnitude of the ruptures.
    rate_bounds : tuple[Optional[float], Optional[float]]
        Optional bounds on the annual rate of the ruptures.
    limit : int
        The limit on the returned number of ruptures.
    fault_count_limit : Optional[int]
        An optional limit on the number of faults in the rupture.
        Useful obtaining a rupture containing precisely the specified
        faults in the query.

    Returns
    -------
    sql_query
        The query compiled to DuckDB compatible SQL.
    parameters
        The query parameters to be supplied.

    Raises
    ------
    ValueError
        If the query provided is invalid.
    """
    expression = parse(query)

    def expression_to_sql(expression: ExpressionTree) -> str:
        match expression:
            case {InfixOperator.AND: (lhs, rhs)}:
                return f"({expression_to_sql(lhs)}) AND ({expression_to_sql(rhs)})"
            case {InfixOperator.OR: (lhs, rhs)}:
                return f"({expression_to_sql(lhs)}) OR ({expression_to_sql(rhs)})"
            case {UnaryOperator.NOT: expr} if isinstance(expr, str) or isinstance(
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
            case {UnaryOperator.NOT: expr} if isinstance(expr, str) or isinstance(
                expr, ExpressionTree
            ):
                yield from query_parameters(expr)
            case fault_name if isinstance(fault_name, str):
                yield fault_name
            case _:
                raise ValueError("Invalid expression")

    parameters: list[Any] = []

    magnitude_expression = ""
    if magnitude_bounds[0]:
        magnitude_expression += "AND rupture.magnitude >= ?"
        parameters.append(magnitude_bounds[0])
    if magnitude_bounds[1]:
        magnitude_expression += "AND rupture.magnitude <= ?"
        parameters.append(magnitude_bounds[1])

    rate_expression = ""
    if rate_bounds[0]:
        rate_expression += "AND rupture.rate >= ?"
        parameters.append(rate_bounds[0])
    if rate_bounds[1]:
        rate_expression += "AND rupture.rate <= ?"
        parameters.append(rate_bounds[1])

    if fault_count_limit:
        fault_count_expression = "COUNT(DISTINCT parent_fault.parent_id) <= ? AND "
        parameters.append(fault_count_limit)
    sql_expression = f"""SELECT
     rupture.rupture_id, ANY_VALUE(rupture.magnitude), ANY_VALUE(rupture.area), ANY_VALUE(rupture.len), ANY_VALUE(rupture.rate)
    FROM rupture
    JOIN
        rupture_faults ON rupture.rupture_id = rupture_faults.rupture_id
    JOIN
        fault ON rupture_faults.fault_id = fault.fault_id
    JOIN
        parent_fault ON fault.parent_id = parent_fault.parent_id
    WHERE rupture.rate IS NOT NULL {magnitude_expression} {rate_expression}
    GROUP BY rupture.rupture_id
    HAVING {fault_count_expression} ({expression_to_sql(expression)})
    ORDER BY ANY_VALUE(rupture.rate)
    DESC NULLS LAST
    LIMIT ?
    """
    parameters.extend(query_parameters(expression))
    parameters.append(limit)

    return (sql_expression, parameters)
