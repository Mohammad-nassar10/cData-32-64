use std::sync::Arc;

use crate::common::TransformContext32;
// use arrow::compute::{gt_eq_scalar, filter_record_batch};
use arrow::{
    array::{Array, Int64Array, PrimitiveArray, StructArray},
    compute::unary,
    datatypes::{DataType, Field, Int64Type, Schema},
    record_batch::RecordBatch,
};

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
    let col_a = input
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    let col_b = input
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    // Perform a transformation
    let negated_a = arrow::compute::negate(col_a).unwrap();
    let zero_b: PrimitiveArray<Int64Type> = unary(col_b, |_x| 0);

    let projected_schema = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]);
    // Build the transformed record batch
    let result = RecordBatch::try_new(
        Arc::new(projected_schema),
        vec![Arc::new(negated_a), Arc::new(zero_b)],
    );

    // Convert the transformed record batch to ffi schema and array
    let transformed_record = result.unwrap();
    let struct_array: StructArray = transformed_record.into();
    let (out_array, out_schema) = struct_array.to_raw().unwrap();

    // Set the output
    ctx.out_schema = out_schema as u32;
    ctx.out_array = out_array as u32;
}