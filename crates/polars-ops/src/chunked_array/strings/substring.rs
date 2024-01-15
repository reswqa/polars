use polars_core::prelude::arity::{binary_elementwise, ternary_elementwise, unary_elementwise};
use polars_core::prelude::{Int64Chunked, StringChunked, UInt64Chunked};

fn substring_ternary(
    opt_str_val: Option<&str>,
    opt_start: Option<i64>,
    opt_length: Option<u64>,
) -> Option<&str> {
    match (opt_str_val, opt_start) {
        (Some(str_val), Some(start)) => {
            // If `offset` is negative, it counts from the end of the string.
            let offset = if start >= 0 {
                start as usize
            } else {
                let offset = (0i64 - start) as usize;
                str_val
                    .char_indices()
                    .rev()
                    .nth(offset)
                    .map(|(idx, _)| idx + 1)
                    .unwrap_or(0)
            };

            let mut iter_chars = str_val.char_indices();
            if let Some((offset_idx, _)) = iter_chars.nth(offset) {
                let len_end = str_val.len() - offset_idx;

                // Slice to end of str if no length given.
                let length = if let Some(length) = opt_length {
                    length as usize
                } else {
                    len_end
                };

                if length == 0 {
                    return Some("");
                }

                let end_idx = iter_chars
                    .nth(length.saturating_sub(1))
                    .map(|(idx, _)| idx)
                    .unwrap_or(str_val.len());

                Some(&str_val[offset_idx..end_idx])
            } else {
                Some("")
            }
        },
        _ => None,
    }
}

pub(super) fn substring(
    ca: &StringChunked,
    start: &Int64Chunked,
    length: &UInt64Chunked,
) -> StringChunked {
    match (ca.len(), start.len(), length.len()) {
        (1, 1, _) => {
            // SAFETY: index `0` is in bound.
            let str_val = unsafe { ca.get_unchecked(0) };
            // SAFETY: index `0` is in bound.
            let start = unsafe { start.get_unchecked(0) };
            unary_elementwise(length, |length| substring_ternary(str_val, start, length))
                .with_name(ca.name())
        },
        (_, 1, 1) => {
            // SAFETY: index `0` is in bound.
            let start = unsafe { start.get_unchecked(0) };
            // SAFETY: index `0` is in bound.
            let length = unsafe { length.get_unchecked(0) };
            unary_elementwise(ca, |str_val| substring_ternary(str_val, start, length))
        },
        (1, _, 1) => {
            // SAFETY: index `0` is in bound.
            let str_val = unsafe { ca.get_unchecked(0) };
            // SAFETY: index `0` is in bound.
            let length = unsafe { length.get_unchecked(0) };
            unary_elementwise(start, |start| substring_ternary(str_val, start, length))
                .with_name(ca.name())
        },
        (1, len_b, len_c) if len_b == len_c => {
            // SAFETY: index `0` is in bound.
            let str_val = unsafe { ca.get_unchecked(0) };
            binary_elementwise(start, length, |start, length| {
                substring_ternary(str_val, start, length)
            })
        },
        (len_a, 1, len_c) if len_a == len_c => {
            fn infer<F: for<'a> FnMut(Option<&'a str>, Option<u64>) -> Option<&'a str>>(f: F) -> F where
            {
                f
            }
            // SAFETY: index `0` is in bound.
            let start = unsafe { start.get_unchecked(0) };
            binary_elementwise(
                ca,
                length,
                infer(|str_val, length| substring_ternary(str_val, start, length)),
            )
        },
        (len_a, len_b, 1) if len_a == len_b => {
            fn infer<F: for<'a> FnMut(Option<&'a str>, Option<i64>) -> Option<&'a str>>(f: F) -> F where
            {
                f
            }
            // SAFETY: index `0` is in bound.
            let length = unsafe { length.get_unchecked(0) };
            binary_elementwise(
                ca,
                start,
                infer(|str_val, start| substring_ternary(str_val, start, length)),
            )
        },
        _ => ternary_elementwise(ca, start, length, substring_ternary),
    }
}
