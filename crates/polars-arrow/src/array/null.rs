use std::any::Any;

use arrow::array::{Array, MutableArray, NullArray};
use arrow::bitmap::MutableBitmap;
use arrow::datatypes::DataType;

#[derive(Debug, Default)]
pub struct MutableNullArray {
    len: usize,
}

impl MutableArray for MutableNullArray {
    fn data_type(&self) -> &DataType {
        &DataType::Null
    }

    fn len(&self) -> usize {
        self.len
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        None
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        dbg!(self.len);
        Box::new(NullArray::new_null(DataType::Null, self.len))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn push_null(&mut self) {
        dbg!("push null");
        self.len += 1;
    }

    fn reserve(&mut self, _additional: usize) {
        // no-op
    }

    fn shrink_to_fit(&mut self) {
        // no-op
    }
}

impl MutableNullArray {
    pub fn extend_nulls(&mut self, null_count: usize){
        dbg!(null_count);
        self.len += null_count;
    }
}
