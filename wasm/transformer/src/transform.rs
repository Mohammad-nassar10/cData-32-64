use std::sync::Arc;

use crate::common::TransformContext;
use crate::common::TransformContext32;
use arrow::{array::{Int32Array, Int64Array, PrimitiveArray, StructArray, Array, make_array_from_raw}, compute::unary, datatypes::{DataType, Field, Int32Type, Int64Type, Schema}, record_batch::RecordBatch, util::pretty::print_batches};


#[no_mangle]
pub extern "C" fn transform(ctx: u32) {
    let ctx = ctx as *mut TransformContext32;
    let ctx = unsafe{ &mut *ctx };
    println!("wasm side transform ctx = {:?}", ctx);
    let schema = ctx.in_schema;
    // unsafe { println!("{:?}", (*schema)); }
    let array = ctx.input().unwrap();
    println!("transform wasm calling input()");
    let as_structarray = array.as_any().downcast_ref::<StructArray>().unwrap();
    let input = RecordBatch::from(as_structarray);
    // let _ = print_batches(&[input]);

    let col_a = input.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    // println!("col a {:?}", col_a);

    let col_b = input.column(1).as_any().downcast_ref::<Int64Array>().unwrap();   
    let negated_a = arrow::compute::negate(col_a).unwrap();
    let zero_b: PrimitiveArray<Int64Type> = unary(col_b, |x| 0);

    let projected_schema = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]);

    let result = RecordBatch::try_new(
        Arc::new(projected_schema),
        vec![
            Arc::new(negated_a),
            Arc::new(zero_b),
        ],
    );
    println!("transform result = {:?}", result);

    // Convert the trabsformed record batch to ffi schema and array
    let transformed_record = result.unwrap();
    let struct_array: StructArray = transformed_record.into();
    let (out_array, out_schema) = struct_array.to_raw().unwrap();
    unsafe { println!("after transform schema ptr = {:?},, {:?}, array ptr = {:?},, {:?}", out_schema, *out_schema, out_array, *out_array); 
        let a32 = &*out_array;
    

    // // let a32_buffers_array = to64(base, a32.buffers) as *const u32;
    // let a64_buffers_array: () = (0..a32.n_buffers as usize)
    //     .map(|i| {
    //         let a32_buffer = unsafe { a32.buffers.add(i) };
    //         unsafe { println!("n_buffers = {:?}", a32.n_buffers); }
    //         let a32_buffer = unsafe { *a32_buffer };
    //         // let a64_buffer = to64(base, a32_buffer);
    //         // // mem::forget(a64_buffer);
    //         unsafe { println!("buffer 32 = {:?}", a32_buffer); }
    //         // a64_buffer
    //     })
    //     .collect();
    }


    ctx.out_schema = out_schema as u32;
    ctx.out_array = out_array as u32;
    // let out_record = unsafe { make_array_from_raw(out_array, out_schema) };
    // println!("wasm side out record = {:?}", out_record);


    // ctx.in_array   

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
