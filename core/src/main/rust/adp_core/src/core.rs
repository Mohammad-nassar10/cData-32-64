use std::io;
use std::sync::Arc;
use wasmer::{Function, NativeFunc, imports};
use wasmer::{Cranelift, Instance, Module, Store, Universal};
use wasmer_wasi::WasiState;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::array::{make_array_from_raw};

use crate::arch::{FFI32_ArrowSchema, FFI64_ArrowSchema, FFI64_ArrowArray, FFI32_ArrowArray, release_exported_schema, to32, GLOBAL_ENV};

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



impl CoreInstance {
    /// create a new [`Ffi_ArrowSchema`]. This fails if the fields' [`DataType`] is not supported.
    pub fn try_new(module_bytes: &[u8]) -> Result<Self, io::Error> {
        let store = Store::new(&Universal::new(Cranelift::default()).engine());
        let module = Module::new(&store, module_bytes).unwrap();
        // let mut wasi_env = WasiState::new("transformer").finalize().unwrap();
        let release_native = Function::new_native(&store, release_exported_schema);
        let import_object = imports! { 
            "env" => {
                "release_func" => release_native,
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

        Ok(Self {
            instance,
            allocate_buffer_func,
            deallocate_buffer_func,
            prepare_transform_func,
            transform_func,
            finalize_tansform_func
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

    pub fn transform(&self, context: u32) {
        println!("transform rust side {:?}, {:?}", context, context as u64 + self.allocator_base());
        
        let ctx = (context as u64 + self.allocator_base()) as *mut FFI_TransformContext;
        let ctx = unsafe{ &mut *ctx };
        println!("ctx = {:?}", ctx);

        let in_schema64 = (ctx.in_schema as u64 + ctx.base) as *mut FFI_ArrowSchema;
        unsafe { println!("transform jni schema = {:?}", *in_schema64); }
        let in_schema64 = (ctx.in_schema as u64 + ctx.base) as *mut FFI64_ArrowSchema;
        unsafe { println!("transform jni schema 64 = {:?}", *in_schema64); }
        let in_array64 = (ctx.in_array as u64 + ctx.base) as *mut FFI64_ArrowArray;
        let schema32 = FFI32_ArrowSchema::new(&self);
        let array32 = FFI32_ArrowArray::new(&self);
        unsafe { 
            println!("schema64 = {:?},, schema 32 = {:?}", (*in_schema64), (schema32));
            println!("array64 = {:?},, array 32 = {:?}", (*in_array64), (*array32));
            // let schema = Arc::into_raw(Arc::new(*in_schema64)) as *const FFI_ArrowSchema;
            // let array = Arc::into_raw(Arc::new(*in_array64)) as *const FFI_ArrowArray;
            // let result = unsafe {make_array_from_raw(array, schema)};
            // println!("transform rust side res = {:?}", result);

            // println!("array64 = {:?},,", *((*in_array64).buffers as *const ));
            // let s64 = &mut *in_schema64;
            // let s64_children_array = s64.children as *mut *mut FFI64_ArrowSchema;
            // let s64_child_item = unsafe { s64_children_array.add(0) };
            // let s64_child = unsafe { &mut *(s64_child_item as *mut FFI64_ArrowSchema) };
            // println!("children number = {:?}", s64_child);
    

            (*schema32).from(self, &mut *in_schema64);
            println!("schema After arch.rs from {:?}", *schema32);
            (*array32).from(self, &mut *in_array64);
            println!("array After arch.rs from {:?}", *array32);
            // let schema = Arc::into_raw(Arc::new(*in_schema64)) as *const FFI_ArrowSchema;
            // let array = Arc::into_raw(Arc::new(*in_array64)) as *const FFI_ArrowArray;
            // // println!("release field = {:?}", (*in_schema64).format);
            // let result = unsafe {make_array_from_raw(array, schema)};
            // println!("transform rust side res = {:?}\nrelease field = {:?}", result, (*in_schema64).format);
        }
        ctx.in_schema = to32(ctx.base, schema32 as u64);
        ctx.in_array = to32(ctx.base, array32 as u64);
        unsafe { GLOBAL_ENV.schema = schema32 as u64 };

        self.transform_func.call(context).unwrap();
    }

    pub fn finalize_tansform(&self, context: u32) {
        self.finalize_tansform_func.call(context).unwrap();
    }
}
