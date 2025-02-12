from __future__ import annotations

from datetime import date
from typing import Any

import pytest

import polars as pl
from polars.testing import assert_frame_equal
from polars.utils._parse_expr_input import _first_input_to_list, parse_as_expression
from polars.utils._wrap import wrap_expr


def assert_expr_equal(result: pl.Expr, expected: pl.Expr) -> None:
    """
    Evaluate the given expressions in a simple context to assert equality.

    WARNING: This is not a fully featured function - it's just to evaluate the tests in
    this module. Do not use it elsewhere.
    """
    df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
    assert_frame_equal(df.select(result), df.select(expected))


def test_first_input_to_list_empty() -> None:
    assert _first_input_to_list([]) == []


def test_first_input_to_list_none() -> None:
    with pytest.deprecated_call():
        assert _first_input_to_list(None) == []


@pytest.mark.parametrize(
    "input",
    [5, 2.0, "a", pl.Series([1, 2, 3]), pl.lit(4)],
)
def test_first_input_to_list_single(input: Any) -> None:
    assert _first_input_to_list(input) == [input]


@pytest.mark.parametrize(
    "input",
    [[5], ["a", "b"], (1, 2, 3), ["a", 5, 3.2]],
)
def test_first_input_to_list_multiple(input: Any) -> None:
    assert _first_input_to_list(input) == list(input)


@pytest.mark.parametrize("input", [5, 2.0, pl.Series([1, 2, 3]), date(2022, 1, 1)])
def test_parse_as_expression_lit(input: Any) -> None:
    result = wrap_expr(parse_as_expression(input))
    expected = pl.lit(input)
    assert_expr_equal(result, expected)


def test_parse_as_expression_col() -> None:
    result = wrap_expr(parse_as_expression("a"))
    expected = pl.col("a")
    assert_expr_equal(result, expected)


@pytest.mark.parametrize("input", [pl.lit(4), pl.col("a")])
def test_parse_as_expression_expr(input: pl.Expr) -> None:
    result = wrap_expr(parse_as_expression(input))
    expected = input
    assert_expr_equal(result, expected)


@pytest.mark.parametrize(
    "input", [pl.when(True).then(1), pl.when(True).then(1).when(False).then(0)]
)
def test_parse_as_expression_whenthen(input: Any) -> None:
    result = wrap_expr(parse_as_expression(input))
    expected = input.otherwise(None)
    assert_expr_equal(result, expected)


def test_parse_as_expression_list() -> None:
    result = wrap_expr(parse_as_expression([1, 2, 3]))
    expected = pl.lit(pl.Series([[1, 2, 3]]))
    assert_expr_equal(result, expected)


def test_parse_as_expression_str_as_lit() -> None:
    result = wrap_expr(parse_as_expression("a", str_as_lit=True))
    expected = pl.lit("a")
    assert_expr_equal(result, expected)


def test_parse_as_expression_structify() -> None:
    result = wrap_expr(parse_as_expression(pl.col("a", "b"), structify=True))
    expected = pl.struct("a", "b")
    assert_expr_equal(result, expected)


def test_parse_as_expression_structify_multiple_outputs() -> None:
    # note: this only works because assert_expr_equal evaluates on a dataframe with
    # columns "a" and "b"
    result = wrap_expr(parse_as_expression(pl.col("*"), structify=True))
    expected = pl.struct("a", "b")
    assert_expr_equal(result, expected)
