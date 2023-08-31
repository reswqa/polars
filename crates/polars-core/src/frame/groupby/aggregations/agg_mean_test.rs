#[cfg(test)]
mod tests {
    use polars_error::PolarsResult;

    use crate::prelude::*;

    #[test]
    fn test_agg_mean() -> PolarsResult<()> {
        let series_a = Series::new("a", &[1, 1, 2, 2]);
        let mut series_b = Series::new("b", &[3.0, 3.0]);
        series_b.append(&Series::new("b", &[4.0, 4.0]));
        let df = DataFrame::new(vec![series_a, series_b])?;
        let mean = df.groupby(["a"])?.mean();
        dbg!(&mean?);
        Ok(())
    }
}
