use polars_error::PolarsResult;
use crate::prelude::{AnyValue, DataFrame, DataType, NamedFromOwned, Series};

    #[test]
    fn test_serde() -> PolarsResult<()>{
        let row_1 = AnyValue::List(Series::from_vec("a", Vec::<i32>::new()));

        let s = Series::from_any_values_and_dtype("item", &[row_1], &DataType::List(Box::new(DataType::Int32)), false)
            .unwrap();
        let df = DataFrame::new(vec![s]).unwrap();

        let df_str = serde_json::to_string(&df).unwrap();
        println!("{}", &df_str);
        let out = serde_json::from_str::<DataFrame>(&df_str).unwrap();
        println!("{:?}", out.schema());
        Ok(())
    }

    #[test]
    fn test_from() -> PolarsResult<()>{
        let s = Series::from_any_values("aa", &[], true)?;
        // println!("{}", s.len());
        let new_s = s.cast(&DataType::List(Box::new(DataType::Utf8)))?;
        // println!("{}", new_s.len());
        let df_str = serde_json::to_string(&new_s).unwrap();
        println!("{}", &df_str);
        let de_s: Series = serde_json::from_str(&df_str).unwrap();
        println!("{}", de_s.dtype());
        Ok(())
    }
