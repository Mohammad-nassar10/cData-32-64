use jni::sys::jlong;
use wasmer::{Instance, NativeFunc};
use std::ops::Deref;


#[allow(non_camel_case_types)]
pub type jptr = jlong;

#[derive(Debug)]
pub struct Pointer<Kind> {
    value: Box<Kind>,
}

impl<Kind> Pointer<Kind> {
    pub fn new(value: Kind) -> Self {
        Pointer {
            value: Box::new(value),
        }
    }

    pub fn borrow<'a>(self) -> &'a mut Kind {
        Box::leak(self.value)
    }
}

impl<Kind> From<Pointer<Kind>> for jptr {
    fn from(pointer: Pointer<Kind>) -> Self {
        Box::into_raw(pointer.value) as _
    }
}

impl<Kind> From<jptr> for Pointer<Kind> {
    fn from(pointer: jptr) -> Self {
        Self {
            value: unsafe { Box::from_raw(pointer as *mut Kind) },
        }
    }
}

impl<Kind> Deref for Pointer<Kind> {
    type Target = Kind;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

pub struct WasmModule {
    pub instance: Pointer<Instance>,
    pub alloc_func: NativeFunc<i64, i32>,
    pub dealloc_func: NativeFunc<(i64, i64)>,
}

#[repr(C)]
#[derive(Debug)]
pub struct Tuple (pub i64, pub i64 );