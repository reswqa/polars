use polars_arrow::utils::CustomIterTools;
use polars_ops::chunked_array::list::*;

use super::*;

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ListFunction {
    Concat,
    #[cfg(feature = "is_in")]
    Contains,
    Slice,
    Get,
    #[cfg(feature = "list_take")]
    Take(bool),
    #[cfg(feature = "list_count")]
    CountMatches,
    Sum,
    #[cfg(feature = "list_sets")]
    SetOperation(SetOperation),
    #[cfg(feature = "list_any_all")]
    Any,
    #[cfg(feature = "list_any_all")]
    All,
    Join,
}

impl Display for ListFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use ListFunction::*;

        let name = match self {
            Concat => "concat",
            #[cfg(feature = "is_in")]
            Contains => "contains",
            Slice => "slice",
            Get => "get",
            #[cfg(feature = "list_take")]
            Take(_) => "take",
            #[cfg(feature = "list_count")]
            CountMatches => "count",
            Sum => "sum",
            #[cfg(feature = "list_sets")]
            SetOperation(s) => return write!(f, "{s}"),
            #[cfg(feature = "list_any_all")]
            Any => "any",
            #[cfg(feature = "list_any_all")]
            All => "all",
            Join => "join",
        };
        write!(f, "{name}")
    }
}

#[cfg(feature = "is_in")]
pub(super) fn contains(args: &mut [Series]) -> PolarsResult<Option<Series>> {
    let list = &args[0];
    let is_in = &args[1];

    polars_ops::prelude::is_in(is_in, list).map(|mut ca| {
        ca.rename(list.name());
        Some(ca.into_series())
    })
}

pub(super) fn slice(args: &mut [Series]) -> PolarsResult<Option<Series>> {
    let s = &args[0];
    let list_ca = s.list()?;
    let offset_s = &args[1];
    let length_s = &args[2];


    out.rename(s.name());
    Ok(Some(out.into_series()))
}

pub(super) fn concat(s: &mut [Series]) -> PolarsResult<Option<Series>> {
    let mut first = std::mem::take(&mut s[0]);
    let other = &s[1..];

    let mut first_ca = match first.list().ok() {
        Some(ca) => ca,
        None => {
            first = first.reshape(&[-1, 1]).unwrap();
            first.list().unwrap()
        },
    }
    .clone();

    if first_ca.len() == 1 && !other.is_empty() {
        let max_len = other.iter().map(|s| s.len()).max().unwrap();
        if max_len > 1 {
            first_ca = first_ca.new_from_index(0, max_len)
        }
    }

    first_ca.lst_concat(other).map(|ca| Some(ca.into_series()))
}

pub(super) fn get(s: &mut [Series]) -> PolarsResult<Option<Series>> {
    let ca = s[0].list()?;
    let index = s[1].cast(&DataType::Int64)?;
    let index = index.i64().unwrap();

    match index.len() {
        1 => {
            let index = index.get(0);
            if let Some(index) = index {
                ca.lst_get(index).map(Some)
            } else {
                polars_bail!(ComputeError: "unexpected null index received in `arr.get`")
            }
        },
        len if len == ca.len() => {
            let ca = ca.rechunk();
            let arr = ca.downcast_iter().next().unwrap();
            let offsets = arr.offsets().as_slice();

            let take_by = index
                .into_iter()
                .enumerate()
                .map(|(i, opt_idx)| {
                    opt_idx.and_then(|idx| {
                        let (start, end) =
                            unsafe { (*offsets.get_unchecked(i), *offsets.get_unchecked(i + 1)) };
                        let offset = if idx >= 0 { start + idx } else { end + idx };
                        if offset >= end || offset < start || start == end {
                            None
                        } else {
                            Some(offset as IdxSize)
                        }
                    })
                })
                .collect::<IdxCa>();
            let s = Series::try_from((ca.name(), arr.values().clone())).unwrap();
            unsafe { s.take_unchecked(&take_by) }.map(Some)
        },
        len => polars_bail!(
            ComputeError:
            "`arr.get` expression got an index array of length {} while the list has {} elements",
            len, ca.len()
        ),
    }
}

#[cfg(feature = "list_take")]
pub(super) fn take(args: &[Series], null_on_oob: bool) -> PolarsResult<Series> {
    let ca = &args[0];
    let idx = &args[1];
    let ca = ca.list()?;

    if idx.len() == 1 && null_on_oob {
        // fast path
        let idx = idx.get(0)?.try_extract::<i64>()?;
        let out = ca.lst_get(idx)?;
        // make sure we return a list
        out.reshape(&[-1, 1])
    } else {
        ca.lst_take(idx, null_on_oob)
    }
}

#[cfg(feature = "list_count")]
pub(super) fn count_matches(args: &[Series]) -> PolarsResult<Series> {
    let s = &args[0];
    let element = &args[1];
    polars_ensure!(
        element.len() == 1,
        ComputeError: "argument expression in `arr.count` must produce exactly one element, got {}",
        element.len()
    );
    let ca = s.list()?;
    list_count_matches(ca, element.get(0).unwrap())
}

pub(super) fn sum(s: &Series) -> PolarsResult<Series> {
    Ok(s.list()?.lst_sum())
}

#[cfg(feature = "list_sets")]
pub(super) fn set_operation(s: &[Series], set_type: SetOperation) -> PolarsResult<Series> {
    let s0 = &s[0];
    let s1 = &s[1];
    list_set_operation(s0.list()?, s1.list()?, set_type).map(|ca| ca.into_series())
}

#[cfg(feature = "list_any_all")]
pub(super) fn lst_any(s: &Series) -> PolarsResult<Series> {
    s.list()?.lst_any()
}

#[cfg(feature = "list_any_all")]
pub(super) fn lst_all(s: &Series) -> PolarsResult<Series> {
    s.list()?.lst_all()
}

pub(super) fn join(s: &[Series]) -> PolarsResult<Series> {
    let ca = s[0].list()?;
    let separator = s[1].utf8()?;
    Ok(ca.lst_join(separator)?.into_series())
}
