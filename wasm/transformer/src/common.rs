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
use crate::allocator::{GLOBAL_ALLOC_SIZE, GLOBAL_DEALLOC_SIZE};


extern "C" {
    fn release_func(schema: u32);
    fn release_array_func(array: u32);
}
// pub unsafe extern "C" fn release_func(schema: u32) {
//     println!("release func wasm extern");
//     let mut schema = schema as *mut FFI_ArrowSchema_helper;
//     println!("release func schema = {:?}", *schema);
//     (*schema).release = None;
// }

// pub unsafe extern "C" fn release_array_func(array: u32) {
//     println!("array release func wasm extern");
//     let mut array = array as *mut FFI_ArrowArray_helper;
//     println!("release func array = {:?}", *array);
//     (*array).release = None;
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
    pub(crate) in_schema: u32,
    in_array: u32,
    pub(crate) out_schema: u32,
    pub(crate) out_array: u32,
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct Result {
    pub(crate) array_ref: Option<ArrayRef>,
    pub(crate) ffi_schema: u32,
    pub(crate) ffi_array: u32,
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

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FFI_ArrowArray_helper {
    pub(crate) length: i64,
    pub(crate) null_count: i64,
    pub(crate) offset: i64,
    pub(crate) n_buffers: i64,
    pub(crate) n_children: i64,
    pub(crate) buffers: *mut *const c_void,
    children: *mut *mut FFI_ArrowArray,
    dictionary: *mut FFI_ArrowArray,
    release: Option<unsafe extern "C" fn(arg1: u32)>,
    // When exported, this MUST contain everything that is owned by this array.
    // for example, any buffer pointed to in `buffers` must be here, as well
    // as the `buffers` pointer itself.
    // In other words, everything in [FFI_ArrowArray] must be owned by
    // `private_data` and can assume that they do not outlive `private_data`.
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
        
        let result = unsafe { make_array_from_raw(array, schema) };
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
    // Gets the input ffi schema and ffi array (32bit) and returns the array ref to build the record batch
    pub fn input(&self) -> Result {
        // println!("gg size of FFI32_ArrowArray wasm = {:?}, size of release = {:?}, i64 = {:?}", size_of::<FFI32_ArrowArray>(), size_of::<Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowArray)>>(), size_of::<i64>());
        // println!("gg size of FFI64_ArrowSchema wasm = {:?}", size_of::<FFI64_ArrowSchema>());
        // Get the schema and change its release function to the imported release function
        // Due to the fact that the release field is private, we use a helper struct which has the same fields as FFI_ArrowSchema
        let mut schema_helper = (self.in_schema as *mut FFI_ArrowSchema_helper);
        println!("input common = {:?}", schema_helper);
        unsafe {
            (*schema_helper).release = Some(release_func);
            // let fun_ptr = release_func as *const () as u64;
            let schema = schema_helper as *const _ as *mut FFI_ArrowSchema;
            println!("input common2 = {:?}", (*schema));

            // The same for the array, to change the release field
            let mut array_helper = self.in_array as *mut FFI_ArrowArray_helper;
            (*array_helper).release = Some(release_array_func);
            let array = array_helper as *const _ as *mut FFI_ArrowArray;
            unsafe { println!("input common3 = {:?}", (*array)); }

        // let s = *schema_helper;
        // let a = *array_helper;
        // let schema = Arc::into_raw(Arc::new(s)) as *const FFI_ArrowSchema;
        // let array = Arc::into_raw(Arc::new(a)) as *const FFI_ArrowArray;

            // Build the array from c-data interface
            let result = unsafe { make_array_from_raw(array, schema) };
            let result = result.ok();
            println!("after make array, {:?}", result);
            let res = Result {
                array_ref: result,
                // array_ref: None,
                ffi_schema: schema as u32,
                ffi_array: array as u32,
            };
            res
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
    println!("prepare transform 1");
    let in_schema = Arc::into_raw(Arc::new(FFI64_ArrowSchema::empty())) as *mut FFI64_ArrowSchema;
    println!("prepare transform 2");
    let in_array = Arc::into_raw(Arc::new(FFI64_ArrowArray::empty())) as *mut FFI64_ArrowArray;
    println!("prepare transform 3");
    let ctx = TransformContext {
        base,
        in_schema,
        in_array,
        out_schema: std::ptr::null_mut(),
        out_array: std::ptr::null_mut(),
    };
    println!("prepare transform 4");
    Box::into_raw(Box::new(ctx)) as u32
}

#[no_mangle]
pub unsafe extern "C" fn new_ffi_schema() -> u32 {
    // let new_schema = Arc::into_raw(Arc::new(FFI_ArrowSchema::empty()));
    // Arc::from_raw(new_schema as *mut FFI_ArrowSchema);
    let new_schema = Arc::into_raw(Arc::new(FFI_ArrowSchema::empty()));
    println!("new ffi schema = {:?}", new_schema);
    new_schema as u32
}

#[no_mangle]
pub extern "C" fn new_ffi_array() -> u32 {
    let new_array = Arc::into_raw(Arc::new(FFI_ArrowArray::empty()));
    new_array as u32
}

#[no_mangle]
pub unsafe extern "C" fn delete_ffi_schema(schema: u32) {
    println!("delete ffi schema = {:?}, {:?}", schema, *(schema as *mut FFI_ArrowSchema));
    Arc::from_raw(schema as *mut FFI_ArrowSchema);
}

#[no_mangle]
pub unsafe extern "C" fn delete_ffi_array(array: u32) {
    Arc::from_raw(array as *mut FFI_ArrowArray);
}

#[no_mangle]
pub unsafe extern "C" fn finalize_tansform(ctx: u32, schema_ptr: u32, array_ptr: u32) {
    let ctx = ctx as *mut TransformContext;
    let ctx = Box::from_raw(ctx);
    // println!("finalize, schema = {:?}, {:?}", ctx.in_schema, *(ctx.in_schema as *mut FFI_ArrowSchema));
    // Arc::from_raw(ctx.in_schema as *mut FFI_ArrowSchema);
    // Arc::from_raw(ctx.in_array as *mut FFI32_ArrowArray);
    Arc::from_raw(schema_ptr as *mut FFI64_ArrowSchema);
    Arc::from_raw(array_ptr as *mut FFI64_ArrowArray);
}

#[no_mangle]
pub unsafe extern "C" fn release_schema32(schema32: u32) {
    println!("Wasm release 32, schema32 ptr = {:?}", schema32);
    let schema_ptr = schema32 as *mut FFI_ArrowSchema;
    // let schema_helper_ptr = schema32 as *mut FFI_ArrowSchema_helper;
    // let schema_helper = unsafe { &*schema_helper_ptr };
    // println!("Wasm release 32 schema = {:?}", schema_helper);
    // match schema_helper.release {
    //     None => (),
    //     Some(release) => unsafe { release(schema_ptr as u32) },
    // };
    unsafe { drop((*schema_ptr).clone()) };
}

#[no_mangle]
pub unsafe extern "C" fn release_array32(array32: u32) {
    println!("Wasm release 32, array32 ptr = {:?}", array32);
    let array_ptr = array32 as *mut FFI_ArrowArray;
    println!("**** Wasm release 32, array32 = {:?}", *array_ptr);
    // let array_helper_ptr = array32 as *mut FFI_ArrowArray_helper;
    // let array_helper = unsafe { &*array_helper_ptr };
    // println!("Wasm release 32 array = {:?}", array_helper);
    // match array_helper.release {
    //     None => (),
    //     Some(release) => unsafe { release(array_ptr as u32) },
    // };
    unsafe { drop((*array_ptr).clone()) };
}

#[no_mangle]
pub unsafe extern "C" fn allocated_size() -> u64 {
    GLOBAL_ALLOC_SIZE
}

#[no_mangle]
pub unsafe extern "C" fn released_size() -> u64 {
    GLOBAL_DEALLOC_SIZE
}