use std::convert::TryInto;

use crate::arch::FFI64_ArrowSchema;
use crate::core::{CoreInstance, FFI_TransformContext, FFI_TransformOutput};
use crate::types::jptr;
use crate::types::Pointer;
use jni::objects::JObject;
use jni::sys::{jbyteArray, jlong};
use jni::JNIEnv;

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_newInstance(
    jre: JNIEnv,
    _object: JObject,
    module_bytes: jbyteArray,
) -> jptr {
    let bytes = jre.convert_byte_array(module_bytes).unwrap();
    let instance = CoreInstance::try_new(&bytes);
    let instance = Pointer::new(instance.unwrap());
    instance.into()
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_dropInstance(
    _jre: JNIEnv,
    _object: JObject,
    instance_ptr: jptr,
) {
    let _: Pointer<CoreInstance> = instance_ptr.into();
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_prepare(
    _jre: JNIEnv,
    _object: JObject,
    instance_ptr: jptr,
) -> jlong {
    let instance = Into::<Pointer<CoreInstance>>::into(instance_ptr).borrow();
    instance.prepare_transform() as jlong
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_transform(
    _jre: JNIEnv,
    _object: JObject,
    instance_ptr: jptr,
    context: jlong,
) -> jptr {
    let instance = Into::<Pointer<CoreInstance>>::into(instance_ptr).borrow();
    let result = instance.transform(context.try_into().unwrap());
    // unsafe { println!("rust outschema ptr = {:?}, schema = {:?}", *(result.out_schema as *mut FFI64_ArrowSchema), result.out_array) };
    // let result = Pointer::new(result);
    // result.into()
    result
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_finish(
    _jre: JNIEnv,
    _object: JObject,
    instance_ptr: jptr,
    context: jlong,
) {
    let instance = Into::<Pointer<CoreInstance>>::into(instance_ptr).borrow();
    instance.finalize_tansform(context.try_into().unwrap());
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_getInputSchema(
    _jre: JNIEnv,
    _object: JObject,
    instance_ptr: jptr,
    context: jlong,
) -> jlong {
    let instance = Into::<Pointer<CoreInstance>>::into(instance_ptr).borrow();
    // let ctx = (instance.allocator_base() + context as u64) as *const FFI_TransformContext;
    // let ctx = unsafe { &*ctx };
    let ctx = instance.context(context as u64);
    ctx.in_schema as jlong
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_getOutputSchema2(
    _jre: JNIEnv,
    _object: JObject,
    instance_ptr: jptr,
    context: jlong,
) -> jlong {
    let instance = Into::<Pointer<CoreInstance>>::into(instance_ptr).borrow();
    let ctx = (instance.allocator_base() + context as u64) as *const FFI_TransformContext;
    let ctx = unsafe { &*ctx };
    ctx.out_schema as jlong
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_getOutputSchema(
    _jre: JNIEnv,
    _object: JObject,
    transform_output: jlong,
) -> jlong {
    let out = Into::<Pointer<FFI_TransformOutput>>::into(transform_output).borrow();
    // let out = unsafe { &*out };
    out.out_schema as jlong
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_getInputArray(
    _jre: JNIEnv,
    _object: JObject,
    instance_ptr: jptr,
    context: jlong,
) -> jlong {
    let instance = Into::<Pointer<CoreInstance>>::into(instance_ptr).borrow();
    // let ctx = context as *const FFI_TransformContext;
    // let ctx = unsafe { &*ctx };
    let ctx = instance.context(context as u64);
    ctx.in_array as jlong
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_getOutputArray2(
    _jre: JNIEnv,
    _object: JObject,
    instance_ptr: jptr,
    context: jlong,
) -> jlong {
    let instance = Into::<Pointer<CoreInstance>>::into(instance_ptr).borrow();
    let ctx = instance.context(context as u64);
    // let ctx = context as *const FFI_TransformContext;
    // let ctx = unsafe { &*ctx };
    ctx.out_array as jlong
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_getOutputArray(
    _jre: JNIEnv,
    _object: JObject,
    transform_output: jlong,
) -> jlong {
    let out = Into::<Pointer<FFI_TransformOutput>>::into(transform_output).borrow();
    // let out = unsafe { &*out };
    out.out_array as jlong
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_wasmAlloc(
    _jre: JNIEnv,
    _object: JObject,
    instance_ptr: jptr,
    size: jlong,
) -> jlong {
    let instance = Into::<Pointer<CoreInstance>>::into(instance_ptr).borrow();
    instance.allocate_buffer(size.try_into().unwrap()) as jlong
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_wasmMemPtr(
    _jre: JNIEnv,
    _object: JObject,
    instance_ptr: jptr,
) -> jlong {
    let instance = Into::<Pointer<CoreInstance>>::into(instance_ptr).borrow();
    instance.allocator_base() as jlong
}

#[no_mangle]
pub extern "C" fn Java_io_fybrik_adp_core_jni_JniWrapper_wasmDealloc(
    _jre: JNIEnv,
    _object: JObject,
    instance_ptr: jptr,
    offset: jlong,
    size: jlong,
) {
    let instance = Into::<Pointer<CoreInstance>>::into(instance_ptr).borrow();
    instance.deallocate_buffer(offset.try_into().unwrap(), size.try_into().unwrap());
}
