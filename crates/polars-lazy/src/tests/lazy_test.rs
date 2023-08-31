use std::ops::{Add, Div};

use polars_core::export::arrow::util::lexical_to_bytes;
use polars_core::schema;
use polars_core::utils::arrow::compute::comparison::lt;
use polars_plan::dsl::Expr::Literal;

use super::*;
#[test]
fn test_simple() -> PolarsResult<()> {
    let df = df!("a" => [1,2,3], "b" => [1,2,3])?;
    let lazy_frame = df.lazy();
    let f = lazy_frame.select([col("c")]);
    f.group_by([col("a")]);
    Ok(())
}

#[test]
fn test_over() -> PolarsResult<()> {
    let df = df!("a" => [1i64], "b" => [1.2], "c" => [2.1])?;
    let result = df
        .lazy()
        .with_columns([
            col("b") - col("b").mean().over([col("a")]),
            //col("c") - col("c").mean().over([col("a")]),
        ])
        .collect()?;
    dbg!(&result);
    Ok(())
}

#[test]
fn test_sum() -> PolarsResult<()> {
    let df = df!("a" => [1,1,2,2], "b" => [1,2,3,4], "c" => [2,3,4,5])?;
    let res = df
        .lazy()
        .group_by(["a"])
        .agg([
            when(col("b").is_null().all(false))
                .then(Expr::Literal(LiteralValue::Null))
                .otherwise(lit(1))
                .alias("b"),
            when(col("c").is_null().all(false))
                .then(Expr::Literal(LiteralValue::Null))
                .otherwise(lit(1))
                .alias("c"),
        ])
        .with_comm_subexpr_elim(true)
        .collect()?;
    dbg!(&res);
    Ok(())
}

#[test]
fn test_group_cse() -> PolarsResult<()> {
    let df = df!("a" => [1,1,2,2], "b" => [1,2,3,4], "c" => [2,3,4,5])?;
    let res = df
        .lazy()
        .group_by(["a"])
        .agg([col("b").sum() + col("b").sum()])
        .with_comm_subexpr_elim(true)
        .with_streaming(false)
        .collect()?;
    dbg!(&res);
    Ok(())
}

#[test]
fn test_over_cse() -> PolarsResult<()> {
    let df = df![
        "a" => [1, 2, 3],
        "b" => [3, 2, 1],
    ]?;
    let res = df
        .lazy()
        .select([
            col("b").sum() + col("b").sum(), // + col("b").sum().over([col("a")])
        ])
        .with_comm_subexpr_elim(true)
        .collect();
    dbg!(&res);
    Ok(())
}

#[test]
fn test_filter() -> PolarsResult<()> {
    let df = df!(
        "a" => [None, None, Some(3), None, Some(5), Some(0), Some(0), Some(0), Some(9), Some(10)], "b" => [1, 1, 2, 2, 3, 3, 4, 4, 5, 5]
    )?;
    let df1 = df
        .lazy()
        .filter((col("a").null_count().lt(count())).over([col("b")]))
        .filter(((col("a").eq(lit(0))).sum().lt(count())).over([col("b")]))
        .with_comm_subexpr_elim(true)
        .with_predicate_pushdown(true)
        .collect()?;
    dbg!(&df1);
    Ok(())
}

#[test]
fn test_cols() -> PolarsResult<()> {
    let df = df![
        "col1" => [1, 2, 3],
        "col2" => [4, 5, 6],
        "val1" => ["a", "b", "c"],
        "val2" => ["A", "B", "C"],
    ]?;

    let res = df
        .lazy()
        .with_columns([col("^col.*$").prefix("p_"), col("val1").prefix("p_")])
        .collect()?;
    // let res = df.lazy().select([cols(["col1","val1","col2"]).exclude(["col2"])]).collect()?;
    /*let res = df
    .lazy()
    .select([cols(["^col.*$"]).exclude(["col2"]) + cols(["^val.*$"]).exclude(["col1"])])
    .collect()?;*/
    dbg!(&res);
    Ok(())
}

#[test]
fn test_explode() -> PolarsResult<()> {
    /*let es: &[Option<&str>] = &[None];
    let s = Series::new(
        "a",
        [
            Utf8Chunked::from_slice_options("a", es).into_series(),
            Series::new("a2", ["b"]),
        ],
    );
    let df = df!(
        "a" => s
    )?;
    dbg!(&df);
    let df1 = df
        .lazy()
        .select([col("a")
            .list()
            .eval(col("").filter(col("").is_not_null()), false)])
        .collect()?;
    dbg!(df1.explode(["a"]));*/
    Ok(())
}

#[test]
fn test_min_arg() -> PolarsResult<()> {
    let x = Series::new("a", [None, Some(true)].as_ref());
    //dbg!(x.min::<i32>());
    //dbg!(x.max::<i32>());
    dbg!(x.arg_min());
    //dbg!(x.arg_max());
    Ok(())
}

#[test]
fn test_eq() -> PolarsResult<()> {
    let s1 = Series::new("s1", [1i32]);
    let s2 = Series::new("s2", [1i32, 2i32]);
    let s = Series::new("a", [s1.clone(), s2]);
    dbg!(&s);
    dbg!(s.equal(&s1)?.into_series());
    Ok(())
}

/*#[test]
fn test_is_first() -> PolarsResult<()> {
    let s1 = Series::new("s1", [1i32]);
    let s2 = Series::new("s2", [1i32, 2i32]);
    let s = Series::new("a", [s1.clone(), s2]);
    dbg!(&s);
    let res = s
        .into_frame()
        .lazy()
        .select([col("a").is_first()])
        .collect()?;
    dbg!(&res);
    Ok(())
}*/

#[test]
fn test_empty() -> PolarsResult<()> {
    let v = col("a").div(col("b"));
    let magic = when(v.clone().gt(lit(0))).then(lit(f32::NAN)).otherwise(v);
    let df =
        df![
                "a" => [1.],
                "b" => [1.],
            ]?
        .lazy().select([magic]);
    let res = df.with_comm_subexpr_elim(true).collect()?;
    dbg!(&res);
    Ok(())
}
