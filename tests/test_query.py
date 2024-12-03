import pytest
from nshmdb.query import (
    InfixOperator,
    Token,
    TokenStream,
    TokenType,
    UnaryOperator,
    lex,
    parse,
    to_sql,
)


def test_lex_basic():
    expression = "fault1 & fault2 | !fault3"
    token_stream = lex(expression)
    expected = [
        Token(TokenType.FAULT, "fault1"),
        Token(TokenType.UNARY_OPERATOR, InfixOperator.AND),
        Token(TokenType.FAULT, "fault2"),
        Token(TokenType.UNARY_OPERATOR, InfixOperator.OR),
        Token(TokenType.INFIX_OPERATOR, UnaryOperator.NOT),
        Token(TokenType.FAULT, "fault3"),
    ]
    assert token_stream.tokens == expected


def test_lex_invalid_character():
    with pytest.raises(ValueError, match=r"Invalid search string fault1 & invalid\$"):
        lex("fault1 & invalid$")


def test_token_stream_peek():
    tokens = [
        Token(TokenType.FAULT, "fault1"),
        Token(TokenType.UNARY_OPERATOR, InfixOperator.AND),
    ]
    token_stream = TokenStream(tokens)
    assert token_stream.peek() == Token(TokenType.FAULT, "fault1")
    next(token_stream)
    assert token_stream.peek() == Token(TokenType.UNARY_OPERATOR, InfixOperator.AND)


def test_token_stream_iteration():
    tokens = [
        Token(TokenType.FAULT, "fault1"),
        Token(TokenType.UNARY_OPERATOR, InfixOperator.AND),
    ]
    token_stream = TokenStream(tokens)
    assert token_stream.tokens == tokens


def test_parse_basic():
    expression = "fault1 & (fault2 | !fault3)"
    tree = parse(expression)
    expected_tree = {
        InfixOperator.AND: (
            "fault1",
            {InfixOperator.OR: ("fault2", {UnaryOperator.NOT: "fault3"})},
        ),
    }
    assert tree == expected_tree


def test_parse_invalid_expression():
    with pytest.raises(
        ValueError, match=r"Invalid search expression fault1 & \(fault2 \| !fault3"
    ):
        parse("fault1 & (fault2 | !fault3")


def test_to_sql_basic():
    query = "fault1 & (fault2 | !fault3)"
    sql_query, parameters = to_sql(query, magnitude_bounds=(5.0, 7.0), limit=10)
    assert "SELECT" in sql_query
    assert "HAVING" in sql_query
    assert "LIMIT ?" in sql_query
    assert parameters == [5.0, 7.0, "fault1", "fault2", "fault3", 10]


def test_to_sql_with_bounds_and_limits():
    query = "fault1 | fault2"
    sql_query, parameters = to_sql(
        query,
        magnitude_bounds=(4.0, None),
        rate_bounds=(None, 0.5),
        fault_count_limit=3,
    )
    assert "rupture.magnitude >= ?" in sql_query
    assert "rupture.rate <= ?" in sql_query
    assert "COUNT(DISTINCT parent_fault.parent_id) <= ?" in sql_query
    assert parameters == [4.0, 0.5, 3, "fault1", "fault2", 100]


def test_to_sql_invalid_query():
    with pytest.raises(
        ValueError, match="Invalid search expression fault1 & invalid & fault!"
    ):
        to_sql("fault1 & invalid & fault!")
