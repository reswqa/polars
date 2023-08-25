use std::hash::Hash;
use std::process::id;

use arrow::array::BooleanArray;
use arrow::bitmap::MutableBitmap;
use polars_arrow::utils::CustomIterTools;
use polars_core::prelude::*;
use polars_core::with_match_physical_integer_polars_type;

use crate::series::ops::bit;

fn is_first_numeric<T>(ca: &ChunkedArray<T>) -> BooleanChunked
where
    T: PolarsNumericType,
    T::Native: Hash + Eq,
{
    let mut unique = PlHashSet::new();
    let chunks = ca.downcast_iter().map(|arr| -> BooleanArray {
        arr.into_iter()
            .map(|opt_v| unique.insert(opt_v))
            .collect_trusted()
    });

    BooleanChunked::from_chunk_iter(ca.name(), chunks)
}

fn is_first_bin(ca: &BinaryChunked) -> BooleanChunked {
    let mut unique = PlHashSet::new();
    let chunks = ca.downcast_iter().map(|arr| -> BooleanArray {
        arr.into_iter()
            .map(|opt_v| unique.insert(opt_v))
            .collect_trusted()
    });

    BooleanChunked::from_chunk_iter(ca.name(), chunks)
}

fn is_first_boolean(ca: &BooleanChunked) -> BooleanChunked {
    if ca.is_empty() {
        return BooleanChunked::full_null(ca.name(), 0);
    }
    let mut out = MutableBitmap::with_capacity(ca.len());
    out.extend_constant(ca.len(), false);

    if ca.null_count() == ca.len() {
        out.set(0, true);
    } else if ca.null_count() == 0 && ca.chunks().len() == 1{
        // fast path
        let arr = ca.downcast_iter().next().unwrap();
        let mask = arr.values();
        out.set(bit::first_set_bit(mask), true);
        out.set(bit::first_unset_bit(mask), true);
    } else{
        let mut first_true_found = false;
        let mut first_false_found = false;
        let mut first_null_found = false;
        let mut all_found = false;
        ca.into_iter()
            .enumerate()
            .find_map(|(idx, val)| match val {
                Some(true) if !first_true_found=> {
                    first_true_found = true;
                    all_found &= first_true_found;
                    out.set(idx, true);
                    if all_found {Some(())} else {  None}
                },
                Some(false) if !first_false_found => {
                    first_false_found = true;
                    all_found &= first_false_found;
                    out.set(idx, true);
                    if all_found {Some(())} else {  None}
                },
                None if !first_null_found=> {
                    first_null_found = true;
                    all_found &= first_null_found;
                    out.set(idx, true);
                    if all_found {Some(())} else {  None}
                },
                _ => None,
            });
    }

    let arr = BooleanArray::new(ArrowDataType::Boolean, out.into(), None);
    BooleanChunked::with_chunk(ca.name(), arr)
}

#[cfg(feature = "dtype-struct")]
fn is_first_struct(s: &Series) -> PolarsResult<BooleanChunked> {
    let groups = s.group_tuples(true, false)?;
    let first = groups.take_group_firsts();
    let mut out = MutableBitmap::with_capacity(s.len());
    out.extend_constant(s.len(), false);

    for idx in first {
        // Group tuples are always in bounds
        unsafe { out.set_unchecked(idx as usize, true) }
    }

    let arr = BooleanArray::new(ArrowDataType::Boolean, out.into(), None);
    Ok(BooleanChunked::with_chunk(s.name(), arr))
}

#[cfg(feature = "group_by_list")]
fn is_first_list(ca: &ListChunked) -> PolarsResult<BooleanChunked> {
    let groups = ca.group_tuples(true, false)?;
    let first = groups.take_group_firsts();
    let mut out = MutableBitmap::with_capacity(ca.len());
    out.extend_constant(ca.len(), false);

    for idx in first {
        // Group tuples are always in bounds
        unsafe { out.set_unchecked(idx as usize, true) }
    }

    let arr = BooleanArray::new(ArrowDataType::Boolean, out.into(), None);
    Ok(BooleanChunked::with_chunk(ca.name(), arr))
}

pub fn is_first(s: &Series) -> PolarsResult<BooleanChunked> {
    let s = s.to_physical_repr();

    use DataType::*;
    let out = match s.dtype() {
        Boolean => {
            let ca = s.bool().unwrap();
            is_first_boolean(ca)
        },
        Binary => {
            let ca = s.binary().unwrap();
            is_first_bin(ca)
        },
        Utf8 => {
            let s = s.cast(&Binary).unwrap();
            return is_first(&s);
        },
        Float32 => {
            let ca = s.bit_repr_small();
            is_first_numeric(&ca)
        },
        Float64 => {
            let ca = s.bit_repr_large();
            is_first_numeric(&ca)
        },
        dt if dt.is_numeric() => {
            with_match_physical_integer_polars_type!(s.dtype(), |$T| {
                let ca: &ChunkedArray<$T> = s.as_ref().as_ref().as_ref();
                is_first_numeric(ca)
            })
        },
        #[cfg(feature = "dtype-struct")]
        Struct(_) => return is_first_struct(&s),
        #[cfg(feature = "group_by_list")]
        List(inner) if inner.is_numeric() => {
            let ca = s.list().unwrap();
            return is_first_list(ca);
        },
        dt => polars_bail!(opq = is_first, dt),
    };
    Ok(out)
}
