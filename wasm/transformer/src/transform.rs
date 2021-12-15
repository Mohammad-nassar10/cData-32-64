use std::sync::Arc;

use crate::common::TransformContext;
use crate::common::TransformContext32;
use arrow::compute::{gt_eq_scalar, filter_record_batch};
use arrow::ffi::FFI_ArrowArray;
use arrow::{array::{Int32Array, Int64Array, PrimitiveArray, StructArray, Array, ArrayRef, BooleanArray, make_array_from_raw}, compute::unary, datatypes::{DataType, Field, Int32Type, Int64Type, Schema}, record_batch::RecordBatch, util::pretty::print_batches};


#[no_mangle]
pub extern "C" fn transform(ctx: u32) {
    let ctx = ctx as *mut TransformContext32;
    let ctx = unsafe{ &mut *ctx };
    println!("wasm side transform ctx = {:?}", ctx);
    let schema = ctx.in_schema;
    // unsafe { println!("{:?}", (*schema)); }
    let input_result = ctx.input();
    // Build the record batch from the input
    let array = input_result.array_ref.unwrap();
    println!("transform wasm calling input()");
    // let struct_array = Arc::from_raw(array);
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

    // let _ = print_batches(&[input]);
    let col_a = input.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    // println!("col a {:?}", col_a);
    
    let col_b = input.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    // Perform a transformation  
    let negated_a = arrow::compute::negate(col_a).unwrap();
    let zero_b: PrimitiveArray<Int64Type> = unary(col_b, |x| 0);
    
    // unsafe { drop((*input_result.ffi_schema).clone()) };
    // unsafe { drop((*(input_result.ffi_array as *mut FFI_ArrowArray)).clone()) };
    
    let projected_schema = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]);
    // Build the transformed record batch
    let result = RecordBatch::try_new(
        Arc::new(projected_schema),
        vec![
            Arc::new(negated_a),
            Arc::new(zero_b),
        ],
    );
    // let result = input;
    // println!("transform result = {:?}", result);

    // Convert the transformed record batch to ffi schema and array
    // let transformed_record = result;
    let transformed_record = result.unwrap();
    let struct_array: StructArray = transformed_record.into();
    let (out_array, out_schema) = struct_array.to_raw().unwrap();
    unsafe { println!("after transform schema ptr = {:?},, {:?}, array ptr = {:?},, {:?}", out_schema, *out_schema, out_array, *out_array);
    }

    //////
    // unsafe { drop((*out_array).clone()) };
    // unsafe { drop((*out_schema).clone()) };
    //////

    // Set the output
    ctx.out_schema = out_schema as u32;
    ctx.out_array = out_array as u32;
    // let out_record = unsafe { make_array_from_raw(out_array, out_schema) };
    // println!("wasm side out record = {:?}", out_record);
}



// #[no_mangle]
// pub extern "C" fn transform(ctx: u32) {
//     let ctx = ctx as *mut TransformContext;
//     let ctx = unsafe{ &*ctx };
//     println!("wasm side transform ctx = {:?}", ctx);
//     // let schema = ctx.input_schema();
//     // println!("{}", schema.unwrap());
//     println!("transform wasm calling input()");
//     let array = ctx.input().unwrap();
//     let as_structarray = array.as_any().downcast_ref::<StructArray>().unwrap();
//     let input = RecordBatch::from(as_structarray);
//     // let _ = print_batches(&[input]);

//     let col_a = input.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
//     println!("col a {:?}", col_a);

//     let col_b = input.column(1).as_any().downcast_ref::<Int32Array>().unwrap();   
//     let negated_a = arrow::compute::negate(col_a).unwrap();
//     let zero_b: PrimitiveArray<Int32Type> = unary(col_b, |x| 0);

//     let projected_schema = Schema::new(vec![
//         Field::new("a", DataType::Int32, false),
//         Field::new("b", DataType::Int32, false),
//     ]);

//     let result = RecordBatch::try_new(
//         Arc::new(projected_schema),
//         vec![
//             Arc::new(negated_a),
//             Arc::new(zero_b),
//         ],
//     );

//     // ctx.in_array   

// }
