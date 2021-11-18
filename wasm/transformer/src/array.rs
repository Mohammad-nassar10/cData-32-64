use std::{convert::TryInto, mem};
use core::ffi::c_void;
use arrow::buffer::Buffer;


#[repr(C)]
#[derive(Debug, Clone)]
pub(crate) struct FFI32_ArrowArray {
    pub(crate) length: i64,
    pub(crate) null_count: i64,
    pub(crate) offset: i64,
    pub(crate) n_buffers: i64,
    pub(crate) n_children: i64,
    pub(crate) buffers: u32,
    children: u32,
    dictionary: u32,
    release: Option<u16>,
    private_data: u32,
}

#[repr(C)]
#[derive(Debug, Clone)]
pub(crate) struct FFI64_ArrowArray {
    pub(crate) length: i64,
    pub(crate) null_count: i64,
    pub(crate) offset: i64,
    pub(crate) n_buffers: i64,
    pub(crate) n_children: i64,
    pub(crate) buffers: u64,
    children: u64,
    dictionary: u64,
    release: Option<u64>,
    private_data: u64,
}

fn to32(base: u64, ptr: u64) -> u32 {
    if ptr == 0 {
        return 0;
    }
    (ptr - base).try_into().unwrap()
} 

impl FFI32_ArrowArray {
    pub(crate) fn empty() -> Self {
        Self {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: 0,
            children: 0,
            dictionary: 0,
            release: None,
            private_data: 0,
        }
    }

    pub fn from(base: u64, s64: &mut FFI64_ArrowArray) -> Self {
        println!("array 64 from");
        let mut root = Self::empty();

        root.length = s64.length;
        root.null_count = s64.null_count;
        root.offset = s64.offset;
        root.n_buffers = s64.n_buffers;
        root.n_children = s64.n_children;

        // root.buffers
        let buffers_array = to32(base, s64.buffers) as *const u64;
        let buffers_data: Vec<u32> = (0..s64.n_buffers as usize)
            .map(|i| {
                let buffer = unsafe { buffers_array.add(i) };
                unsafe { println!("n_buffers = {:?}", s64.n_buffers); }
                let buffer = unsafe { *buffer };
                let buffer = to32(base, buffer);
                unsafe { println!("buffer = {:?}", *(buffer as *const Buffer)); }
                buffer
            })
            .collect();
        let buffers_ptr = buffers_data
            .into_iter()
            .map(Box::new)
            .map(Box::into_raw)
            .collect::<Box<_>>();
        root.buffers = buffers_ptr.as_ptr() as u32;
        mem::forget(buffers_ptr);

        // root.children
        let children_array = to32(base, s64.children) as *const u64;
        let child_data: Vec<FFI32_ArrowArray> = (0..s64.n_children as usize)
            .map(|i| {
                let child = unsafe { children_array.add(i) };
                let child = unsafe { *child };
                let child = to32(base, child);
                let child = unsafe {&mut*(child as *mut FFI64_ArrowArray)};
                FFI32_ArrowArray::from(base, child)
            })
            .collect();
        let children_ptr = child_data
            .into_iter()
            .map(Box::new)
            .map(Box::into_raw)
            .collect::<Box<_>>();
        root.children = children_ptr.as_ptr() as u32;
        mem::forget(children_ptr);

        // root.dictionary
        // if s64.dictionary != 0 {
        //     let dictionary = to32(base, s64.dictionary) as *mut FFI64_ArrowArray;
        //     let dictionary = unsafe { &mut*dictionary };
        //     let dictionary = FFI32_ArrowArray::from(base, dictionary);
        //     let dictionary = Box::from(dictionary);
        //     let dictionary = Box::into_raw(dictionary);
        //     root.dictionary = dictionary as u32;
        // }

        // root.release
        // root.release = Some(s64.release.unwrap() as u32);

        // root.private_data

        // Move old array
        // s64.release = None;

        root

    }
}

impl FFI64_ArrowArray {
    pub(crate) fn empty() -> Self {
        Self {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: 0,
            children: 0,
            dictionary: 0,
            release: None,
            private_data: 0,
        }
    }
}

// impl From<&FFI64_ArrowArray> for FFI32_ArrowArray {
//     fn from(s64: &FFI64_ArrowArray) -> Self {
//         let mut root = Self::empty();

//         root.length = s64.length;
//         root.null_count = s64.null_count;
//         root.offset = s64.offset;
//         root.n_buffers = s64.n_buffers;
//         root.n_children = s64.n_children;

//         // root.buffers
//         // root.children
//         // root.dictionary
//         // root.release
//         // root.private_data
//         root
//     }
// }
