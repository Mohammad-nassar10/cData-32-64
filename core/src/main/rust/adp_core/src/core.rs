use std::io;
use wasmer::{imports, Function, NativeFunc};
use wasmer::{Cranelift, Instance, Module, Store, Universal};
use wasmer_wasi::WasiState;

use crate::arch::{
    release_exported_array, release_exported_array64, release_exported_schema,
    release_exported_schema64, to32, to64, FFI32_ArrowArray, FFI32_ArrowSchema, FFI64_ArrowArray,
    FFI64_ArrowSchema, GLOBAL_ENV,
};
use crate::types::{jptr, Pointer};

// use dhat::Dhat;

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
    pub finalize_tansform_func: NativeFunc<(u32, u32, u32), ()>,
    // schema32 ptr -> void
    pub release_schema32_func: NativeFunc<u32, ()>,
    // array32 ptr -> void
    pub release_array32_func: NativeFunc<u32, ()>,
    // void -> schema32
    pub new_ffi_schema: NativeFunc<(), u32>,
    // void -> array32
    pub new_ffi_array: NativeFunc<(), u32>,
    pub get_first_elem_of_tuple: NativeFunc<u32, u32>,
    pub get_second_elem_of_tuple: NativeFunc<u32, u32>,
    pub drop_tuple: NativeFunc<u32, ()>,
    pub transformation_ipc: NativeFunc<(u32, u32), u32>,
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
    /// create a new [`ffi_ArrowSchema`]. This fails if the fields' [`DataType`] is not supported.
    pub fn try_new(module_bytes: &[u8]) -> Result<Self, io::Error> {
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
            .get_native_function::<(u32, u32, u32), ()>("finalize_tansform")
            .unwrap();

        let release_schema32_func = instance
            .exports
            .get_native_function::<u32, ()>("release_schema32")
            .unwrap();

        let release_array32_func = instance
            .exports
            .get_native_function::<u32, ()>("release_array32")
            .unwrap();

        let new_ffi_schema = instance
            .exports
            .get_native_function::<(), u32>("new_ffi_schema")
            .unwrap();

        let new_ffi_array = instance
            .exports
            .get_native_function::<(), u32>("new_ffi_array")
            .unwrap();

        let get_first_elem_of_tuple = instance
            .exports
            .get_native_function::<u32, u32>("get_first_of_tuple")
            .unwrap();
        let get_second_elem_of_tuple = instance
            .exports
            .get_native_function::<u32, u32>("get_second_of_tuple")
            .unwrap();
        let drop_tuple = instance
            .exports
            .get_native_function::<u32, ()>("drop_tuple")
            .unwrap();
        let transformation_ipc = instance
            .exports
            .get_native_function::<(u32, u32), u32>("read_transform_write_from_bytes")
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
            new_ffi_schema,
            new_ffi_array,
            get_first_elem_of_tuple,
            get_second_elem_of_tuple,
            drop_tuple,
            transformation_ipc
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
        // Get the schema and arrow of 64 bit from the context
        let ctx = to64(base, context) as *mut FFI_TransformContext;
        let ctx = unsafe { &mut *ctx };
        let in_schema64 = (ctx.in_schema as u64 + ctx.base) as *mut FFI64_ArrowSchema;
        let in_array64 = (ctx.in_array as u64 + ctx.base) as *mut FFI64_ArrowArray;
        // Allocate new, empty arrow and schema of 32 bit
        let schema32 = FFI32_ArrowSchema::new_root(&self);
        let array32 = FFI32_ArrowArray::new_root(&self);
        unsafe {
            // Convert the 64 bit schema to a 32 bit schema
            (*schema32).from(self, &mut *in_schema64);
            // println!("schema After arch.rs from {:?}", *schema32);
            // Convert the 64 bit array to a 32 bit array
            (*array32).from(self, &mut *in_array64);
            // println!("array After arch.rs from {:?}", *array32);
        }
        // Update the contex with the 32 bit schema and array
        ctx.in_schema = to32(ctx.base, schema32 as u64);
        ctx.in_array = to32(ctx.base, array32 as u64);
        // Set a global variable with the schema and array in order to release them afterwards
        unsafe { GLOBAL_ENV.schema32 = schema32 as u64 };
        unsafe { GLOBAL_ENV.array32 = array32 as u64 };
        // println!("schema 32 = {:?}, array 32 = {:?}", schema32, array32);

        // Call the Wasm function that performs the transformation
        self.transform_func.call(context).unwrap();
        // println!("schema 32 = {:?}, array 32 = {:?}", schema32, array32);

        // Convert back from 32 to 64 after transformation
        let out_schema32 = (ctx.out_schema as u64 + ctx.base) as *mut FFI32_ArrowSchema;
        let out_array32 = (ctx.out_array as u64 + ctx.base) as *mut FFI32_ArrowArray;
        
        // Allocate new, empty arrow and schema of 64 bit
        let mut out_schema64 = FFI64_ArrowSchema::new();
        let mut out_array64 = FFI64_ArrowArray::new();
        unsafe {
            // Convert the 32 bit schema to a 64 bit schema
            out_schema64.from(self, &mut *out_schema32);
            // println!("schema After arch.rs from 32 to 64 {:?}", out_schema64);
            // Convert the 32 bit array to a 64 bit array
            out_array64.from(self, &mut *out_array32);
            // println!("array After arch.rs from 32 to 64 {:?}", out_array64);
            // println!("buffers of array After arch.rs from 32 to 64, buffers ptr = {:?}", out_array64.buffers, /*(*(out_array64.buffers as *const Vec<u64>)).get(0)*/);
        }
        // Set a global variable with the schema and array in order to release them afterwards
        let out_schema64_ptr: jptr = Pointer::new(out_schema64).into();
        let out_array64_ptr: jptr = Pointer::new(out_array64).into();
        unsafe { GLOBAL_ENV.schema64 = out_schema64_ptr as u64 };
        unsafe { GLOBAL_ENV.array64 = out_array64_ptr as u64 };
        // The result is pointers to the transformed schema and array
        let result = FFI_TransformOutput {
            out_schema: Pointer::new(out_schema64).into(),
            out_array: Pointer::new(out_array64).into(),
        };
        let res = Pointer::new(result);
        res.into()

        // let result = FFI_TransformOutput {
        //     out_schema: 0,
        //     out_array: 0,
        // };
        // let res = Pointer::new(result);
        // res.into()
    }

    pub fn finalize_tansform(&self, context: u32, schema_ptr: u32, array_ptr: u32) {
        // println!("finalize transform");
        release_exported_schema64(0);
        release_exported_array64(0);
        self.finalize_tansform_func
            .call(context, schema_ptr, array_ptr)
            .unwrap();
    }

    pub fn release_schema32(&self, schema32: u32) {
        self.release_schema32_func.call(schema32).unwrap();
    }

    pub fn release_array32(&self, array32: u32) {
        self.release_array32_func.call(array32).unwrap();
    }

    pub fn new_ffi_schema(&self) -> u32 {
        self.new_ffi_schema.call().unwrap()
    }

    pub fn new_ffi_array(&self) -> u32 {
        self.new_ffi_array.call().unwrap()
    }

    #[no_mangle]
    pub extern "system" fn TransformationIPC(&self, address: u32, length: u32) -> u32 {
        // Get the wasm module instance and the functions we want to use
        // let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
        // let instance = &wasm_module.instance;
        // let read_transform_write_from_bytes_wasm = instance.exports.get_function("read_transform_write_from_bytes").unwrap().native::<(i64, i64), i64>().unwrap();
        
        // Call the function that read the bytes in the `address` parameter and getting the appropriate record batch
        // Then, it makes a transformation, writes back the transformed record batch, and returns a tuple of `(address, len)` of the transformed batch
        let transformed_tuple = self.transformation_ipc.call(address, length).unwrap();
        transformed_tuple
    }

    #[no_mangle]
    pub extern "system" fn GetFirstElemOfTuple(&self, tuple_ptr: u32) -> u32 {
        // // Get the wasm module instance and the functions we want to use
        // let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
        // let instance = &wasm_module.instance;
        // let get_first_of_tuple = instance.exports.get_function("get_first_of_tuple").unwrap().native::<i64, i64>().unwrap();
        self.get_first_elem_of_tuple.call(tuple_ptr).unwrap()
    }

    #[no_mangle]
    pub extern "system" fn GetSecondElemOfTuple(&self, tuple_ptr: u32) -> u32 {
        // Get the wasm module instance and the functions we want to use
        // let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
        // let instance = &wasm_module.instance;
        // let get_second_of_tuple = instance.exports.get_function("get_second_of_tuple").unwrap().native::<i64, i64>().unwrap();
        self.get_second_elem_of_tuple.call(tuple_ptr).unwrap()
    }

    #[no_mangle]
    pub extern "system" fn DropTuple(&self, tuple_ptr: u32) {
        // Get the wasm module instance and the functions we want to use
        // let wasm_module = Into::<Pointer<WasmModule>>::into(wasm_module_ptr).borrow();
        // let instance = &wasm_module.instance;
        // let drop_tuple_wasm = instance.exports.get_function("drop_tuple").unwrap().native::<i64, ()>().unwrap();
        self.drop_tuple.call(tuple_ptr).unwrap();
    }


}
