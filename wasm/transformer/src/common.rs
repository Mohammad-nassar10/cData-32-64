use crate::array::FFI64_ArrowArray;
use crate::schema::FFI64_ArrowSchema;
use arrow::array::{make_array_from_raw, ArrayRef};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use core::ffi::c_void;
use std::os::raw::c_char;
use std::sync::Arc;

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

impl TransformContext32 {
    // Gets the input ffi schema and ffi array (32bit) and returns the array ref to build the record batch
    pub fn input(&self) -> Result {
        // Get the schema and change its release function to the imported release function
        // Due to the fact that the release field is private, we use a helper struct which has the same fields as FFI_ArrowSchema
        let mut schema_helper = self.in_schema as *mut FFI_ArrowSchema_helper;
        unsafe {
            (*schema_helper).release = Some(release_func);
            let schema = schema_helper as *const _ as *mut FFI_ArrowSchema;
            println!("input common2 = {:?}", (*schema));

            // The same for the array, to change the release field
            let mut array_helper = self.in_array as *mut FFI_ArrowArray_helper;
            (*array_helper).release = Some(release_array_func);
            let array = array_helper as *const _ as *mut FFI_ArrowArray;

            // Build the array from c-data interface
            let result = make_array_from_raw(array, schema);
            let result = result.ok();
            println!("after make array, {:?}", result);
            let res = Result {
                array_ref: result,
                ffi_schema: 0,
                ffi_array: 0,
            };
            res
        }
    }
}

#[no_mangle]
pub extern "C" fn prepare_transform(base: u64) -> u32 {
    let in_schema = Arc::into_raw(Arc::new(FFI64_ArrowSchema::empty())) as *mut FFI64_ArrowSchema;
    let in_array = Arc::into_raw(Arc::new(FFI64_ArrowArray::empty())) as *mut FFI64_ArrowArray;
    let ctx = TransformContext32 {
        base,
        in_schema: in_schema as u32,
        in_array: in_array as u32,
        out_schema: 0,
        out_array: 0,
    };
    Box::into_raw(Box::new(ctx)) as u32
}

#[no_mangle]
pub unsafe extern "C" fn new_ffi_schema() -> u32 {
    let new_schema = Arc::into_raw(Arc::new(FFI_ArrowSchema::empty()));
    new_schema as u32
}

#[no_mangle]
pub extern "C" fn new_ffi_array() -> u32 {
    let new_array = Arc::into_raw(Arc::new(FFI_ArrowArray::empty()));
    new_array as u32
}

#[no_mangle]
pub unsafe extern "C" fn finalize_tansform(_ctx: u32, schema_ptr: u32, array_ptr: u32) {
    // let ctx = ctx as *mut TransformContext32;
    // let ctx = Box::from_raw(ctx);
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
    let schema_helper_ptr = schema32 as *mut FFI_ArrowSchema_helper;
    let schema_helper = &*schema_helper_ptr;
    match schema_helper.release {
        None => (),
        Some(release) => release(schema_ptr as u32),
    };
}

#[no_mangle]
pub unsafe extern "C" fn release_array32(array32: u32) {
    println!("Wasm release 32, array32 ptr = {:?}", array32);
    let array_ptr = array32 as *mut FFI_ArrowArray;
    let array_helper_ptr = array32 as *mut FFI_ArrowArray_helper;
    let array_helper = &*array_helper_ptr;
    match array_helper.release {
        None => (),
        Some(release) => release(array_ptr as u32),
    };
}
