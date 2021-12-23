use std::sync::Arc;

use crate::common::TransformContext32;
// use arrow::compute::{gt_eq_scalar, filter_record_batch};
use arrow::array::ArrayRef;
use arrow::{
    array::{Array, Int64Array, PrimitiveArray, StructArray},
    compute::unary,
    datatypes::{DataType, Field, Int64Type, Schema},
    record_batch::RecordBatch,
    ipc::{self, reader::StreamReader}
};
use crate::types::*;
use arrow::ipc::writer::StreamWriter;
use std::io::Cursor;
use std::ops::Deref;

#[no_mangle]
pub extern "C" fn transform(ctx: u32) {
    let ctx = ctx as *mut TransformContext32;
    let ctx = unsafe { &mut *ctx };
    let input_result = ctx.input();
    // Build the record batch from the input
    let array = input_result.array_ref.unwrap();
    let as_structarray = array.as_any().downcast_ref::<StructArray>().unwrap();
    let input = RecordBatch::from(as_structarray);

    // // Filter transformation
    // // let filter_index = input.schema().index_of(filter_col).unwrap();
    // // let columns: &[ArrayRef] = input.columns();
    // // let filter_column = columns[0].data();
    // // let filter_array = Int64Array::from(filter_column.clone());

    // // let bool_arr = gt_eq_scalar::<Int64Type>(&filter_array, 500000).unwrap();
    // let num_rows = input.num_rows();
    // let bool_arr = BooleanArray::from(vec![true; num_rows]);

    // let result = filter_record_batch(&input, &bool_arr);

    // // Filter transformation
    // let col_a = input
    //     .column(0)
    //     .as_any()
    //     .downcast_ref::<Int64Array>()
    //     .unwrap();

    // let col_b = input
    //     .column(1)
    //     .as_any()
    //     .downcast_ref::<Int64Array>()
    //     .unwrap();
    // // Perform a transformation
    // let negated_a = arrow::compute::negate(col_a).unwrap();
    // let zero_b: PrimitiveArray<Int64Type> = unary(col_b, |_x| 0);

    // let projected_schema = Schema::new(vec![
    //     Field::new("a", DataType::Int64, false),
    //     Field::new("b", DataType::Int64, false),
    // ]);
    // // Build the transformed record batch
    // let result = RecordBatch::try_new(
    //     Arc::new(projected_schema),
    //     vec![Arc::new(negated_a), Arc::new(zero_b)],
    // );

    // Convert the transformed record batch to ffi schema and array
    let transformed_record = transform_record_batch(input);
    // let transformed_record = result.unwrap();
    let struct_array: StructArray = transformed_record.into();
    let (out_array, out_schema) = struct_array.to_raw().unwrap();

    // Set the output
    ctx.out_schema = out_schema as u32;
    ctx.out_array = out_array as u32;
}

pub fn transform_record_batch(record_in: RecordBatch) -> RecordBatch {
    let num_cols = record_in.num_columns();
    let num_rows = record_in.num_rows();
    // Build a zero array
    let struct_array = Int64Array::from(vec![0; num_rows]);
    let new_column = Arc::new(struct_array);
    // Get the columns except the last column
    let columns: &[ArrayRef] = record_in.columns();
    let first_columns = columns[0..num_cols-1].to_vec();
    // Create a new array with the same columns expect the last where it will be zero column
    let new_array = [first_columns, vec![new_column]].concat();
    // Create a transformed record batch with the same schema and the new array
    let transformed_record = RecordBatch::try_new(
        record_in.schema(),
        new_array
    ).unwrap();
    transformed_record
}

#[no_mangle]
pub fn create_tuple_ptr(elem1: u32, elem2: u32) -> u32 {
    let ret_tuple = Tuple(elem1, elem2);
    let ret_tuple_ptr = Pointer::new(ret_tuple).into();
    ret_tuple_ptr
}

 //////////IPC related functions//////////

#[no_mangle]
pub fn read_transform_write_from_bytes(bytes_ptr: u32, bytes_len: u32) -> u32 {
    // Read the byte array in the given address and length
    let bytes_array: Vec<u8> = unsafe{ Vec::from_raw_parts(bytes_ptr as *mut _, bytes_len as usize, bytes_len as usize) };
    let cursor = Cursor::new(bytes_array);
    let reader = StreamReader::try_new(cursor).unwrap();
    let mut ret_ptr = 0;
    reader.for_each(|batch| {
        let batch = batch.unwrap();
        // Transform the record batch
        let transformed = transform_record_batch(batch);

        // Write the transformed record batch uing IPC
        let schema = transformed.schema();
        let vec = Vec::new();
        let mut writer = StreamWriter::try_new(vec, &schema).unwrap();
        writer.write(&transformed).unwrap();
        writer.finish().unwrap();
        let mut bytes_array = writer.into_inner().unwrap();
        bytes_array.shrink_to_fit();
        let bytes_ptr = bytes_array.as_mut_ptr();
        let bytes_len = bytes_array.len();
        std::mem::forget(bytes_array);
        ret_ptr =  create_tuple_ptr(bytes_ptr as u32, bytes_len as u32);
    });
    ret_ptr
}

#[no_mangle]
pub fn get_first_of_tuple(tuple_ptr: u32) -> u32 {
    let tuple = Into::<Pointer<Tuple>>::into(tuple_ptr).borrow();
    (*tuple).0
}

#[no_mangle]
pub fn get_second_of_tuple(tuple_ptr: u32) -> u32 {
    let tuple = Into::<Pointer<Tuple>>::into(tuple_ptr).borrow();
    (*tuple).1
}

#[no_mangle]
pub fn drop_tuple(tuple_ptr: u32) {
    let tuple = Into::<Pointer<Tuple>>::into(tuple_ptr);
    let tuple = tuple.deref();
    unsafe {
        drop(Vec::from_raw_parts(tuple.0 as *mut u8, tuple.1 as usize, tuple.1 as usize));
    };
}