use arrow::bitmap::utils::{BitChunkIterExact, BitChunksExact};
use arrow::bitmap::Bitmap;
use super::*;

#[inline]
fn get_leading_zeroes(chunk: u64) -> u32 {
    if cfg!(target_endian = "little") {
        chunk.trailing_zeros()
    } else {
        chunk.leading_zeros()
    }
}

#[inline]
fn get_leading_ones(chunk: u64) -> u32 {
    if cfg!(target_endian = "little") {
        chunk.trailing_ones()
    } else {
        chunk.leading_ones()
    }
}

fn first_set_bit_impl<I>(mut mask_chunks: I) -> usize
where
    I: BitChunkIterExact<u64>,
{
    let mut total = 0usize;
    let size = 64;
    for chunk in &mut mask_chunks {
        let pos = get_leading_zeroes(chunk);
        if pos != size {
            return total + pos as usize;
        } else {
            total += size as usize
        }
    }
    if let Some(pos) = mask_chunks.remainder_iter().position(|v| v) {
        total += pos;
        return total;
    }
    // all null, return the first
    0
}

pub fn first_set_bit(mask: &Bitmap) -> usize {
    if mask.unset_bits() == 0 || mask.unset_bits() == mask.len() {
        return 0;
    }
    let (slice, offset, length) = mask.as_slice();
    if offset == 0 {
        let mask_chunks = BitChunksExact::<u64>::new(slice, length);
        first_set_bit_impl(mask_chunks)
    } else {
        let mask_chunks = mask.chunks::<u64>();
        first_set_bit_impl(mask_chunks)
    }
}

fn first_unset_bit_impl<I>(mut mask_chunks: I) -> usize
where
    I: BitChunkIterExact<u64>,
{
    let mut total = 0usize;
    let size = 64;
    for chunk in &mut mask_chunks {
        let pos = get_leading_ones(chunk);
        if pos != size {
            return total + pos as usize;
        } else {
            total += size as usize
        }
    }
    if let Some(pos) = mask_chunks.remainder_iter().position(|v| !v) {
        total += pos;
        return total;
    }
    // all null, return the first
    0
}

pub fn first_unset_bit(mask: &Bitmap) -> usize {
    if mask.unset_bits() == 0 || mask.unset_bits() == mask.len() {
        return 0;
    }
    let (slice, offset, length) = mask.as_slice();
    if offset == 0 {
        let mask_chunks = BitChunksExact::<u64>::new(slice, length);
        first_unset_bit_impl(mask_chunks)
    } else {
        let mask_chunks = mask.chunks::<u64>();
        first_unset_bit_impl(mask_chunks)
    }
}
