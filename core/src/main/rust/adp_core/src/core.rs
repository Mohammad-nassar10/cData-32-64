use std::ptr::NonNull;
use std::{io, mem};
use std::sync::Arc;
use arrow::buffer::Buffer;
use wasmer::{Function, NativeFunc, imports};
use wasmer::{Cranelift, Instance, Module, Store, Universal};
use wasmer_wasi::WasiState;
use arrow::ffi::{self, ArrowArrayRef, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::array::{Array, make_array_from_raw};

use crate::arch::{FFI32_ArrowArray, FFI32_ArrowSchema, FFI64_ArrowArray, FFI64_ArrowSchema, GLOBAL_ENV, release_exported_array, release_exported_schema, release_exported_schema64, release_exported_array64, to32, to64};
use crate::types::{Pointer, jptr};

use dhat::Dhat;

#[repr(C)]
#[derive(Clone)]
pub struct CoreInstance {
    instance: Instance,
    // size -> buffer ptr
    pub allocate_buffer_func: NativeFunc<u32, u32>,
    // (buffer ptr, size) -> void
    pub deallocate_buffer_func: NativeFunc<(u32, u32), ()>,
    // void -> context ptr
    pub prepare_transform_func: NativeFunc<u64, u32>,
    // context ptr -> void
    pub transform_func: NativeFunc<u32, ()>,
    // context ptr -> void
    pub finalize_tansform_func: NativeFunc<u32, ()>,
    // schema32 ptr -> void
    pub release_schema32_func: NativeFunc<u32, ()>,
    // array32 ptr -> void
    pub release_array32_func: NativeFunc<u32, ()>,
    pub allocated_size: NativeFunc<(), u64>,
    pub released_size: NativeFunc<(), u64>,
}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_TransformContext {
    pub base: u64,
    pub in_schema: u32,
    pub in_array: u32,
    pub out_schema: u32,
    pub out_array: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_TransformOutput {
    pub out_schema: jptr,
    pub out_array: jptr,
}

impl CoreInstance {
    /// create a new [`Ffi_ArrowSchema`]. This fails if the fields' [`DataType`] is not supported.
    pub fn try_new(module_bytes: &[u8]) -> Result<Self, io::Error> {
        // let _dhat = Dhat::start_heap_profiling();

        let store = Store::new(&Universal::new(Cranelift::default()).engine());
        let module = Module::new(&store, module_bytes).unwrap();
        // let mut wasi_env = WasiState::new("transformer").finalize().unwrap();
        let release_native_schema = Function::new_native(&store, release_exported_schema);
        let release_native_array = Function::new_native(&store, release_exported_array);
        let import_object = imports! { 
            "env" => {
                "release_func" => release_native_schema,
                "release_array_func" => release_native_array,
            },
        };
        // let import_object = wasi_env.import_object(&module).unwrap();

        let instance = Instance::new(&module, &import_object).unwrap();
        
        let allocate_buffer_func = instance
            .exports
            .get_native_function::<u32, u32>("allocate_buffer")
            .unwrap();

        let deallocate_buffer_func = instance
            .exports
            .get_native_function::<(u32, u32), ()>("deallocate_buffer")
            .unwrap();

        let prepare_transform_func = instance
            .exports
            .get_native_function::<u64, u32>("prepare_transform")
            .unwrap();

        let transform_func = instance
            .exports
            .get_native_function::<u32, ()>("transform")
            .unwrap();

        let finalize_tansform_func = instance
            .exports
            .get_native_function::<u32, ()>("finalize_tansform")
            .unwrap();

        let release_schema32_func = instance
            .exports
            .get_native_function::<u32, ()>("release_schema32")
            .unwrap();
            
        let release_array32_func = instance
            .exports
            .get_native_function::<u32, ()>("release_array32")
            .unwrap();
        let allocated_size = instance
            .exports
            .get_native_function::<(), (u64)>("allocated_size")
            .unwrap();
        let released_size = instance
            .exports
            .get_native_function::<(), (u64)>("released_size")
            .unwrap();

        Ok(Self {
            instance,
            allocate_buffer_func,
            deallocate_buffer_func,
            prepare_transform_func,
            transform_func,
            finalize_tansform_func, 
            release_schema32_func,
            release_array32_func,
            allocated_size,
            released_size
        })
    }

    pub fn context(&self, context: u64) -> &FFI_TransformContext {
        let base = self.allocator_base();
        let ctx = (base + context) as *const FFI_TransformContext;
        let ctx = unsafe { &*ctx };
        ctx    
    }

    pub fn allocator_base(&self) -> u64 {
        let memory = self.instance.exports.get_memory("memory").unwrap();
        let mem_ptr = memory.data_ptr();
        mem_ptr as u64
    }
    
    pub fn allocate_buffer(&self, size: u32) -> u32 {
        self.allocate_buffer_func.call(size).unwrap()
    }

    pub fn deallocate_buffer(&self, buffer_ptr: u32, size: u32) {
        self.deallocate_buffer_func.call(buffer_ptr, size).unwrap();
    }

    pub fn prepare_transform(&self) -> u32 {
        let base = self.allocator_base();
        self.prepare_transform_func.call(base).unwrap()
    }

    pub fn transform(&self, context: u32) -> jptr {
        let base = self.allocator_base();
        println!("transform rust side {:?}, {:?}", context, to64(base, context));
        // Get the schema and arrow of 64 bit from the context
        let ctx = to64(base, context) as *mut FFI_TransformContext;
        let ctx = unsafe{ &mut *ctx };
        let in_schema64 = (ctx.in_schema as u64 + ctx.base) as *mut FFI64_ArrowSchema;
        let in_array64 = (ctx.in_array as u64 + ctx.base) as *mut FFI64_ArrowArray;
        // Allocate new, empty arrow and schema of 32 bit
        let schema32 = FFI32_ArrowSchema::new(&self);
        let array32 = FFI32_ArrowArray::new(&self);
        unsafe { 
            // Convert the 64 bit schema to a 32 bit schema
            (*schema32).from(self, &mut *in_schema64);
            println!("schema After arch.rs from {:?}", *schema32);
            // Convert the 64 bit array to a 32 bit array
            (*array32).from(self, &mut *in_array64);
            println!("array After arch.rs from {:?}", *array32);
        }
        // Update the contex with the 32 bit schema and array
        ctx.in_schema = to32(ctx.base, schema32 as u64);
        ctx.in_array = to32(ctx.base, array32 as u64);
        // Set a global variable with the schema and array in order to release them afterwards
        unsafe { GLOBAL_ENV.schema = schema32 as u64 };
        unsafe { GLOBAL_ENV.array = array32 as u64 };

        // let result = unsafe { make_array_from_raw(array, schema) };
        // println!("result = {:?}", result);

        // Call the Wasm function that performs the transformation
        self.transform_func.call(context).unwrap();


        // Convert back from 32 to 64 after transformation
        let out_schema32 = (ctx.out_schema as u64 + ctx.base) as *mut FFI32_ArrowSchema;
        let out_array32 = (ctx.out_array as u64 + ctx.base) as *mut FFI32_ArrowArray;
        unsafe { println!("transformed jni schema = {:?}", *out_schema32); }
        // Allocate new, empty arrow and schema of 64 bit
        let mut out_schema64 = FFI64_ArrowSchema::new();
        let mut out_array64 = FFI64_ArrowArray::new();
        unsafe { 
            println!("out schema64 = {:?},, out schema 32 = {:?}", &out_schema64 as *const _ as u64, *out_schema32);
            // Convert the 32 bit schema to a 64 bit schema
            out_schema64.from(self, &mut *out_schema32);
            println!("schema After arch.rs from 32 to 64 {:?}", out_schema64);
            // Convert the 32 bit array to a 64 bit array
            out_array64.from(self, &mut *out_array32);
            println!("array After arch.rs from 32 to 64 {:?}", out_array64);
            // println!("buffers of array After arch.rs from 32 to 64, buffers ptr = {:?}", out_array64.buffers, /*(*(out_array64.buffers as *const Vec<u64>)).get(0)*/);
        }
        // Set a global variable with the schema and array in order to release them afterwards
        let out_schema64_ptr: jptr = Pointer::new(out_schema64).into();
        let out_array64_ptr: jptr = Pointer::new(out_array64).into();
        unsafe { GLOBAL_ENV.schema = out_schema64_ptr as u64 };
        unsafe { GLOBAL_ENV.array = out_array64_ptr as u64 };
        // The result is pointers to the transformed schema and array
        let result = FFI_TransformOutput {
            out_schema: Pointer::new(out_schema64).into(),
            out_array: Pointer::new(out_array64).into(),
        };
        // let test_schema = result.out_schema as *mut FFI_ArrowSchema;
        // let test_array = &out_array64 as *const _ as u64 as *mut FFI_ArrowArray;
        // unsafe { println!("rust outschema = {:?}, array = {:?}", *(result.out_schema as *mut FFI64_ArrowSchema), *(result.out_array as *mut FFI64_ArrowArray));
            // let array = ffi::ArrowArray::try_from_raw(test_array, test_schema).unwrap();
            // // let buf = create_buffer(Arc::new((*test_array).clone()), &*(result.out_array as *mut FFI64_ArrowArray), 0, 100);
            // println!("core.rs create buf = {:?}", array.buffers());
        // }
        // let out_record = unsafe { make_array_from_raw(test_array, test_schema) };
        // unsafe { println!("test schema = {:?}, array = {:?}\nrecord = {:?}", *test_schema, *test_array, out_record); }
        // unsafe { println!("rust res schema ptr = {:?}, array = {:?}", result.out_schema, result.out_array); }
        
                // Delete allocated schema32 and array 32
                // FFI32_ArrowSchema::delete(&self, to32(base, schema32 as u64));
                // FFI32_ArrowArray::delete(&self, to32(base, array32 as u64));
        // unsafe { mem::forget(*(result.out_schema as *mut FFI64_ArrowSchema)); }
        // mem::forget(result.out_array);
        let res = Pointer::new(result);
        res.into()

        // let result = FFI_TransformOutput {
        //     out_schema: 0,
        //     out_array: 0,
        // };
        // let res = Pointer::new(result);
        // res.into()
    }

    pub fn finalize_tansform(&self, context: u32) {
        // release_exported_schema64(0);
        // release_exported_array64(0);
        // self.deallocate_buffer(10000, 100000);
        self.finalize_tansform_func.call(context).unwrap();
    }

    pub fn release_schema32(&self, schema32: u32) {
        self.release_schema32_func.call(schema32).unwrap();
    }

    pub fn release_array32(&self, array32: u32) {
        self.release_array32_func.call(array32).unwrap();
    }

    pub fn allocated_size(&self) {
        self.allocated_size.call().unwrap();
    }

    pub fn released_size(&self) {
        self.released_size.call().unwrap();
    }
}
