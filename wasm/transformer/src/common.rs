// use std::convert::TryInto;
// use arrow::ffi::FFI_ArrowSchema;

use std::convert::TryFrom;
use std::sync::Arc;
use std::os::raw::c_char;
use std::mem::size_of;
use core::ffi::c_void;

use arrow::array::{Array, ArrayRef, Int32Array, StructArray, make_array_from_raw};
use arrow::datatypes::Schema;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;

use crate::array::{FFI32_ArrowArray, FFI64_ArrowArray};
use crate::schema::{FFI32_ArrowSchema, FFI64_ArrowSchema};


extern "C" {
    fn release_func(schema: u32);
}
// pub unsafe extern "C" fn release_func(schema: u32) {
//     println!("release func wasm extern");
//     let mut schema = schema as *mut FFI_ArrowSchema_helper;
//     println!("release func schema = {:?}", *schema);
//     (*schema).release = None;
// }

#[repr(C)]
#[derive(Debug)]
pub(crate) struct TransformContext {
    base: u64,
    in_schema: *mut FFI64_ArrowSchema,
    in_array: *mut FFI64_ArrowArray,
    out_schema: *const FFI64_ArrowSchema,
    out_array: *const FFI64_ArrowArray,
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct TransformContext32 {
    base: u64,
    pub in_schema: u32,
    in_array: *mut FFI32_ArrowArray,
    out_schema: *const FFI_ArrowSchema,
    out_array: *const FFI_ArrowArray,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct FFI_ArrowSchema_helper {
    format: *const c_char,
    name: *const c_char,
    metadata: *const c_char,
    flags: i64,
    n_children: i64,
    children: *mut *mut FFI_ArrowSchema,
    dictionary: *mut FFI_ArrowSchema,
    release: Option<unsafe extern "C" fn(arg1: u32)>,
    private_data: *mut c_void,
}


impl TransformContext {
    pub fn input_schema(&self) -> Option<Schema> {
        let schema = unsafe {&mut*self.in_schema};
        let schema = FFI32_ArrowSchema::from(self.base, schema);
        let schema = Arc::into_raw(Arc::new(schema)) as *const FFI_ArrowSchema;
        let schema = unsafe{&*schema};
        println!("format {}", schema.format());
        Schema::try_from(schema).ok()
        // None
    }

    pub fn input(&self) -> Option<ArrayRef> {
        let schema = unsafe {&mut*self.in_schema};
        println!("input common = {:?}", schema);
        let schema = FFI32_ArrowSchema::from(self.base, schema);
        let schema = Arc::into_raw(Arc::new(schema)) as *const FFI_ArrowSchema;
        
        let array = unsafe {&mut*self.in_array};
        let array = FFI32_ArrowArray::from(self.base, array);
        let array = Arc::into_raw(Arc::new(array)) as *const FFI_ArrowArray;
        
        let result = unsafe {make_array_from_raw(array, schema)};
        result.ok()
    }

    // pub fn output(&self, rb: RecordBatch) {
    //     // let array = Int32Array::from(vec![Some(1), None, Some(3)]);
    //     // array.to_raw();
    //     let as_struct = Arc::new(StructArray::from(rb));
    //     let (array,  schema)= as_struct.to_raw().unwrap();
    //     self.out_schema = FFI64_ArrowSchema::from(self.base, schema as &mut FFI32_ArrowSchema).to_raw() ;
    //     self.out_array = array;
    // }
}

fn ffi32Toffi(schema: FFI32_ArrowSchema) -> *mut FFI_ArrowSchema {
    let res = FFI_ArrowSchema::empty();
    // res.format = schema.format as *const c_char;

    let res_ptr = &res as *const _ as *mut FFI_ArrowSchema;
    std::mem::forget(res_ptr);
    res_ptr
}

impl TransformContext32 {
    // pub fn input_schema(&self) -> Option<Schema> {
    //     let schema = unsafe {&mut*self.in_schema};
    //     let schema = FFI32_ArrowSchema::from(self.base, schema);
    //     let schema = Arc::into_raw(Arc::new(schema)) as *const FFI_ArrowSchema;
    //     let schema = unsafe{&*schema};
    //     println!("format {}", schema.format());
    //     Schema::try_from(schema).ok()
    //     // None
    // }

    pub fn input(&self) -> Option<ArrayRef> {
        println!("gg size of FFI32_ArrowSchema wasm = {:?}, size of release = {:?}, i64 = {:?}", size_of::<FFI32_ArrowSchema>(), size_of::<Option<u32>>(), size_of::<i64>());
        // println!("gg size of FFI_ArrowSchema wasm = {:?}", size_of::<FFI_ArrowSchema>());
        // let tmp = unsafe { self.in_schema as *mut FFI32_ArrowSchema };
        // unsafe { println!("input tmp = {:?}", *tmp); }
        let mut schema = unsafe { ((self.in_schema as *mut FFI_ArrowSchema_helper)) };
        // schema.release = Some(123456);
        // schema.dictionary = 123456;
        println!("input common = {:?}", schema);
        // let schema = &schema as *const _ as *mut FFI_ArrowSchema_helper;
        // let mut schema = unsafe { *schema };
        // let private_data = (*schema).private_data;
        // let tmp = &schema as *const _ as *mut FFI_ArrowSchema_tmp;
        // let tmp: FFI_ArrowSchema_tmp = unsafe { std::mem::transmute(schema) };
        unsafe {
            // println!("input common tmp = {:?}, {:?}", (tmp), private_data);
            // let tmp = tmp as *mut _;
            // let tmp1 = (tmp as u32 + 16) as *const u32;
            // println!("ptr read = {:?}, tmp = {:?}", std::ptr::read(tmp1), tmp);
            // let schema2 = &tmp as *const _ as *mut FFI_ArrowSchema_tmp2;
            // let schema2 = &schema as *const _ as *mut FFI_ArrowSchema_helper;
            // (*schema2).private_data = private_data as *mut c_void;
            (*schema).release = Some(release_func);
            // (*schema2).release = None;
            let fun_ptr = release_func as *const () as u64;
            println!("pointer to function = {:?}", fun_ptr);
            println!("input common tmp2 = {:?}", schema);
        
        
            // let schema = FFI32_ArrowSchema::from(self.base, schema);
            let schema = schema as *const _ as *mut FFI_ArrowSchema;
            // let schema = Arc::into_raw(Arc::new(schema)) as *mut FFI_ArrowSchema;
            // let schema = ffi32Toffi(schema);
            unsafe { println!("input common2 = {:?}", (*schema)); }
            let mut array = unsafe { (*self.in_array).clone() };
            println!("columns before from = {:?}", array.buffers);

            // let array = FFI32_ArrowArray::from(self.base, array);
            let array = Arc::into_raw(Arc::new(array)) as *const FFI_ArrowArray;
            unsafe { println!("input common3 = {:?}", (*array)); }
            

            let result = unsafe { make_array_from_raw(array, schema) };
            println!("input result = {:?}", result);
            
            println!("schema after make array = {:?}", *schema);
            unsafe { drop((*schema).clone()) };

            // let schema3 = unsafe { self.in_schema };
            // unsafe { println!("schema = {:?}, release = {:?}", schema, (*schema).release); }
            // let release_func = unsafe { std::mem::transmute::<*const (), fn(u32)>((*schema).release.unwrap() as *const()) };
            // match (*schema2).release {
            //     None => (),
            //     Some(release) => unsafe { release(schema as u32) },
            // };
            // unsafe { release_func(schema as u32); }
            println!("after release");
            result.ok()
            // None
        }
    }

    // pub fn output(&self, rb: RecordBatch) {
    //     // let array = Int32Array::from(vec![Some(1), None, Some(3)]);
    //     // array.to_raw();
    //     let as_struct = Arc::new(StructArray::from(rb));
    //     let (array,  schema)= as_struct.to_raw().unwrap();
    //     self.out_schema = FFI64_ArrowSchema::from(self.base, schema as &mut FFI32_ArrowSchema).to_raw() ;
    //     self.out_array = array;
    // }
}

#[no_mangle]
pub extern "C" fn prepare_transform(base: u64) -> u32 {
    let ctx = TransformContext {
        base,
        in_schema: Arc::into_raw(Arc::new(FFI64_ArrowSchema::empty())) as *mut FFI64_ArrowSchema,
        in_array: Arc::into_raw(Arc::new(FFI64_ArrowArray::empty())) as *mut FFI64_ArrowArray,
        out_schema: std::ptr::null_mut(),
        out_array: std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(ctx)) as u32
}

#[no_mangle]
pub unsafe extern "C" fn finalize_tansform(ctx: u32) {
    let ctx = ctx as *mut TransformContext;
    let ctx = Box::from_raw(ctx);
    Arc::from_raw(ctx.in_schema);
    Arc::from_raw(ctx.in_array);
}
