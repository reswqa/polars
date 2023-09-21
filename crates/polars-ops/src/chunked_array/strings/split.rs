#[cfg(feature = "dtype-struct")]
use polars_arrow::export::arrow::array::{MutableArray, MutableUtf8Array};
use polars_core::chunked_array::ops::arity::binary_elementwise_for_each;

use super::*;

#[cfg(feature = "dtype-struct")]
pub fn split_to_struct<'a, F, I>(
    ca: &'a Utf8Chunked,
    n: usize,
    op: F,
) -> PolarsResult<StructChunked>
where
    F: Fn(&'a str) -> I,
    I: Iterator<Item = &'a str>,
{
    let mut arrs = (0..n)
        .map(|_| MutableUtf8Array::<i64>::with_capacity(ca.len()))
        .collect::<Vec<_>>();

    ca.for_each(|opt_s| match opt_s {
        None => {
            for arr in &mut arrs {
                arr.push_null()
            }
        },
        Some(s) => {
            let mut arr_iter = arrs.iter_mut();
            let split_iter = op(s);
            (split_iter)
                .zip(&mut arr_iter)
                .for_each(|(splitted, arr)| arr.push(Some(splitted)));
            // fill the remaining with null
            for arr in arr_iter {
                arr.push_null()
            }
        },
    });

    let fields = arrs
        .into_iter()
        .enumerate()
        .map(|(i, mut arr)| {
            Series::try_from((format!("field_{i}").as_str(), arr.as_box())).unwrap()
        })
        .collect::<Vec<_>>();

    StructChunked::new(ca.name(), &fields)
}

pub fn split(ca: &Utf8Chunked, by: &Utf8Chunked) -> ListChunked {
    if by.len() == 1 {
        if let Some(by) = by.get(0) {
            split_literal(ca, by)
        } else {
            ListChunked::full_null_with_dtype(ca.name(), ca.len(), &DataType::Utf8)
        }
    } else {
        split_many(ca, by)
    }
}

pub fn split_inclusive(ca: &Utf8Chunked, by: &Utf8Chunked) -> ListChunked {
    if by.len() == 1 {
        if let Some(by) = by.get(0) {
            split_inclusive_literal(ca, by)
        } else {
            ListChunked::full_null_with_dtype(ca.name(), ca.len(), &DataType::Utf8)
        }
    } else {
        split_inclusive_many(ca, by)
    }
}

fn split_literal(ca: &Utf8Chunked, by: &str) -> ListChunked {
    let mut builder = ListUtf8ChunkedBuilder::new(ca.name(), ca.len(), ca.get_values_size());

    ca.for_each(|opt_v| match opt_v {
        Some(val) => {
            let iter = val.split(by);
            builder.append_values_iter(iter)
        },
        _ => builder.append_null(),
    });
    builder.finish()
}

fn split_many(ca: &Utf8Chunked, by: &Utf8Chunked) -> ListChunked {
    let mut builder = ListUtf8ChunkedBuilder::new(ca.name(), ca.len(), ca.get_values_size());

    binary_elementwise_for_each(ca, by, |opt_s, opt_by| match (opt_s, opt_by) {
        (Some(s), Some(by)) => {
            let iter = s.split(by);
            builder.append_values_iter(iter);
        },
        _ => builder.append_null(),
    });

    builder.finish()
}

fn split_inclusive_literal(ca: &Utf8Chunked, by: &str) -> ListChunked {
    let mut builder = ListUtf8ChunkedBuilder::new(ca.name(), ca.len(), ca.get_values_size());

    ca.for_each(|opt_v| match opt_v {
        Some(val) => {
            let iter = val.split_inclusive(by);
            builder.append_values_iter(iter)
        },
        _ => builder.append_null(),
    });
    builder.finish()
}

fn split_inclusive_many(ca: &Utf8Chunked, by: &Utf8Chunked) -> ListChunked {
    let mut builder = ListUtf8ChunkedBuilder::new(ca.name(), ca.len(), ca.get_values_size());

    binary_elementwise_for_each(ca, by, |opt_s, opt_by| match (opt_s, opt_by) {
        (Some(s), Some(by)) => {
            let iter = s.split_inclusive(by);
            builder.append_values_iter(iter);
        },
        _ => builder.append_null(),
    });

    builder.finish()
}
