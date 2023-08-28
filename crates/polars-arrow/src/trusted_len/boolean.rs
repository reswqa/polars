use arrow::array::BooleanArray;
use arrow::bitmap::MutableBitmap;
use arrow::datatypes::DataType;

use crate::array::default_arrays::FromData;
use crate::bit_util::{set_bit_raw, unset_bit_raw};
use crate::trusted_len::{FromIteratorReversed, TrustedLen};
use crate::utils::FromTrustedLenIterator;
impl FromTrustedLenIterator<Option<bool>> for BooleanArray {
    fn from_iter_trusted_length<I: IntoIterator<Item = Option<bool>>>(iter: I) -> Self
    where
        I::IntoIter: TrustedLen,
    {
        // Soundness
        // Trait system bounded to TrustedLen
        unsafe { BooleanArray::from_trusted_len_iter_unchecked(iter.into_iter()) }
    }
}
impl FromTrustedLenIterator<bool> for BooleanArray {
    fn from_iter_trusted_length<I: IntoIterator<Item = bool>>(iter: I) -> Self
    where
        I::IntoIter: TrustedLen,
    {
        // Soundness
        // Trait system bounded to TrustedLen
        unsafe {
            BooleanArray::from_data_default(
                MutableBitmap::from_trusted_len_iter_unchecked(iter.into_iter()).into(),
                None,
            )
        }
    }
}

impl FromIteratorReversed<bool> for BooleanArray {
    fn from_trusted_len_iter_rev<I: TrustedLen<Item = bool>>(iter: I) -> Self {
        let size = iter.size_hint().1.unwrap();

        let mut vals: Vec<bool> = Vec::with_capacity(size);
        unsafe {
            // Set to end of buffer.
            let mut ptr = vals.as_mut_ptr().add(size);

            iter.for_each(|item| {
                ptr = ptr.sub(1);
                std::ptr::write(ptr, item);
            });
            vals.set_len(size)
        }
        BooleanArray::new(DataType::Boolean, vals.into(), None)
    }
}

impl FromIteratorReversed<Option<bool>> for BooleanArray {
    fn from_trusted_len_iter_rev<I: TrustedLen<Item = Option<bool>>>(iter: I) -> Self {
        let size = iter.size_hint().1.unwrap();

        let vals = MutableBitmap::from_len_zeroed(size);
        let mut validity = MutableBitmap::with_capacity(size);
        validity.extend_constant(size, true);
        let validity_ptr = validity.as_slice().as_ptr() as *mut u8;
        let vals_ptr = vals.as_slice().as_ptr() as *mut u8;
        unsafe {
            let mut offset = size;

            iter.for_each(|opt_item| {
                offset -= 1;
                match opt_item {
                    Some(item) => {
                        if item {
                            // Set value (validity bit is already true).
                            set_bit_raw(vals_ptr, offset);
                        }
                    },
                    None => {
                        // Unset validity bit.
                        unset_bit_raw(validity_ptr, offset)
                    },
                }
            });
        }
        BooleanArray::new(DataType::Boolean, vals.into(), Some(validity.into()))
    }
}
