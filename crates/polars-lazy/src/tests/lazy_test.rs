use super::*;

#[test]
fn test_join() -> PolarsResult<()>{
    let data_1 = df!["A" => [1], "B" => [11]]?.lazy();
    let data_2 = df!{"A" => [1], "B" => [22]}?.lazy();

    let query = df!{"A" => [1]}?.lazy();

    let empty_join_1 = query.clone().join(data_1, [col("A")], [col("A")],JoinArgs::new(JoinType::Inner) );
    let empty_join_2 = query.clone().join(data_2, [col("A")], [col("A")],JoinArgs::new(JoinType::Inner) );

    let empty_concat = concat([empty_join_1, empty_join_2], UnionArgs::default());

    let res = empty_concat?.with_streaming(true).collect()?;
    dbg!(res);
    Ok(())
}