use crate::core::CoreInstance;
use std::{
    convert::TryInto,
    mem::{self, size_of},
    os::raw::c_void, ptr,
};

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub(crate) struct FFI64_ArrowSchema {
    format: u64,
    name: u64,
    metadata: u64,
    flags: i64,
    n_children: i64,
    pub children: u64,
    dictionary: u64,
    release: Option<unsafe extern "C" fn(arg1: *mut FFI64_ArrowSchema)>,
    private_data: u64,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub(crate) struct FFI32_ArrowSchema {
    format: u32,
    name: u32,
    metadata: u32,
    flags: i64,
    n_children: i64,
    children: u32,
    pub dictionary: u32,
    // pub release: Option<unsafe extern "C" fn(arg1: *mut FFI32_ArrowSchema)>,
    // Using u32 in order to align with the length of FFI_ArrowSchema in Wasm side
    pub release: u32,
    private_data: u32,
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct FFI32_ArrowSchema_PrivateData {
    inner: *mut FFI64_ArrowSchema,
    children_buffer_ptr: u32,
    children_buffer_size: u32,
    instance: *mut CoreInstance,
}

pub fn allocate_buffer(len: u32) -> u64 {
    let mut buf: Vec<u8> = Vec::with_capacity(len.try_into().unwrap());
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr as u64
}

pub unsafe fn deallocate_buffer(ptr: u64, size: u32) {
    let data = Vec::from_raw_parts(ptr as *mut u8, size as usize, size as usize);
    std::mem::drop(data);
}

// Global variable to use in release callback
pub(crate) struct GlobalEnv {
    pub(crate) base_mem: u64,
    pub(crate) schema32: u64,
    pub(crate) array32: u64,
    pub(crate) schema64: u64,
    pub(crate) array64: u64,
}

pub(crate) static mut GLOBAL_ENV: GlobalEnv = GlobalEnv {
    base_mem: 0,
    schema32: 0,
    array32: 0,
    schema64: 0,
    array64: 0,
};

// Release function for schema, it is imported and will be called from Wasm side
// For security reasons it is better to use the schema as we stored it in the
// global variable and not to rely on the parameter that the function got
pub fn release_exported_schema(_schema32: u32) {
    // println!("release func");
    let base;
    let global_schema;
    unsafe {
        base = GLOBAL_ENV.base_mem;
        global_schema = GLOBAL_ENV.schema32;
        // Get the schema and the private data
        let schema = global_schema as *mut FFI32_ArrowSchema;
        // println!("release func schema = {:?}", *schema);
        // note that the private_data field is a u32 pointer. Thus, we need to add the base address to it
        let private_data = to64(base, (*schema).private_data) as *mut FFI32_ArrowSchema_PrivateData;
        let mut inner = (*private_data).inner;
        let instance = (*private_data).instance;
        // Call the release function of the 64bit (that releases the children)
        match (*inner).release {
            None => (),
            Some(release) => release(inner),
        };
        (*inner).release = None;

        // Go over the children and call this function with each child in order to release the 32bit-related memory
        // The children's memory (shared for 32bit and 64bit) was released by calling the 64bit's release function
        if (*schema).n_children > 0 {
            for i in 0..(*schema).n_children as usize {
                // Get the i-th child
                // Get the start address of the array of the children
                let children = to64(base, (*schema).children) as *const u32;
                // Step to the i-th entry of the array which stores the address of the i-th child
                let child = children.add(i);
                // The address of the child is a 32bit address. The base should be added to it
                let child = to64(base, *child);
                // Now we have the needed address, we can get the child
                let child = &mut *(child as *mut FFI32_ArrowSchema);
                // println!("release func4, child = {:?}", child);
                if child.release != 0 {
                    // Use this function to release the child also, we need to change the global variable to the child
                    GLOBAL_ENV.schema32 = child as *const _ as u64;
                    // The value of the parameter is not used
                    release_exported_schema(0);
                    (*instance).deallocate_buffer(
                        to32(base, child as *const _ as u64),
                        size_of::<FFI32_ArrowSchema>() as u32,
                    );
                }
            }
            // After traversing the children, we can release the array of the children
            (*instance).deallocate_buffer(
                (*private_data).children_buffer_ptr,
                (*private_data).children_buffer_size,
            );
        }
        // Release the private_data memory
        (*instance).deallocate_buffer(
            to32(base, private_data as u64),
            size_of::<FFI32_ArrowSchema_PrivateData>() as u32,
        );

        (*schema).release = 0;
        // println!("release func8");
    }
}

/// Plugin allocates empty s32 in WASM memory (`in32`)
/// Core allocates empty s64 (`in64`)
/// Java fills in64 using WASM allocator
/// Core transmutes in64 into in32:
/// 1. Remove `base` from addresses
/// 2. Allocates any auxilary structures in WASM memory
/// 3. Overrides private data to hold `in64` and self allocations (from 2)
/// 4. Overrides release callback to a function that frees in64 and self allocations
/// 5. WITHIN WASM: Overrides release callback to imported function
impl FFI32_ArrowSchema {
    // Allocate new FFI32_ArrowSchema using Wasm allocations. Used to allocate the children
    pub fn new(instance: &mut CoreInstance) -> *mut Self {
        let allocated_offset = instance.allocate_buffer(size_of::<Self>() as u32);
        let s32 = to64(instance.allocator_base(), allocated_offset) as *mut FFI32_ArrowSchema;
        s32 as *mut Self
    }
    // Allocate new FFI32_ArrowSchema using Arc. Used to allocate the root
    pub fn new_root(instance: &mut CoreInstance) -> *mut Self {
        let s32 = instance.new_ffi_schema();
        let s32 = to64(instance.allocator_base(), s32);
        s32 as *mut Self
    }

    // pub fn delete(instance: &CoreInstance, schema_ptr: u32) {
    //     instance.deallocate_buffer(schema_ptr, size_of::<Self>() as u32);
    //     println!("delete schema");
    // }

    // Convert the given 64bit schema to 32bit schema
    pub fn from(&mut self, instance: &mut CoreInstance, s64: &mut FFI64_ArrowSchema) {
        let base = instance.allocator_base();
        unsafe {
            GLOBAL_ENV.base_mem = base;
        }
        // Allocate a memory for private_data struct
        self.private_data =
            instance.allocate_buffer(size_of::<FFI32_ArrowSchema_PrivateData>() as u32);
        let private_data = to64(base, self.private_data) as *mut FFI32_ArrowSchema_PrivateData;
        let private_data = unsafe { &mut *private_data };
        private_data.inner = s64;
        private_data.instance = instance as *const _ as *mut CoreInstance;
        // Fill the fields
        self.format = to32(base, s64.format as u64);
        self.name = to32(base, s64.name as u64);
        self.metadata = to32(base, s64.metadata as u64);
        self.flags = s64.flags;
        self.n_children = s64.n_children;

        // Recursively convert the children (the sub-schemas) from 64 to 32
        if self.n_children > 0 {
            // Allocate WASM memory for children's array
            private_data.children_buffer_size = (self.n_children * size_of::<u32>() as i64) as u32;
            private_data.children_buffer_ptr =
                instance.allocate_buffer(private_data.children_buffer_size);
            // Get the address of the start address of the 64bit schema children's array
            let s64_children_array = s64.children as *const u64;
            // Get the address of the start address of the 32bit schema children's array
            let s32_children_array = to64(base, private_data.children_buffer_ptr) as *mut u32;
            for i in 0..s64.n_children as usize {
                // Address of the i-th child of the FFI64
                let s64_child_item = unsafe { s64_children_array.add(i) };
                let s64_child_item = unsafe { *s64_child_item };
                // Address of the i-th cell in the allocated children array of FFI32
                let s32_child_item = unsafe { s32_children_array.add(i) };
                // The FFI64_ArrowSchema of the i-th child
                let s64_child = unsafe { &mut *(s64_child_item as *mut FFI64_ArrowSchema) };
                // Allocate the child's struct
                let s32_child = FFI32_ArrowSchema::new(instance);
                unsafe {
                    // Fill the new child with s64_child
                    FFI32_ArrowSchema::from(&mut *s32_child, instance, s64_child);
                    // Fill the i-th cell in children array with the address of the new child
                    *s32_child_item = to32(base, s32_child as u64);
                }
            }
            self.children = private_data.children_buffer_ptr;
        }
        // dictionary
        // self.dictionary = 0;
        // if !s64.dictionary.is_null() {}

        let fun_ptr = release_exported_schema as *const () as u64;
        // The release callback is set in the WASM module
        // Set a value different than None (but it is not used)
        self.release = fun_ptr as u32;
        // self.release = 0;
    }
}

pub(crate) fn to32(base: u64, ptr: u64) -> u32 {
    if ptr == 0 {
        return 0;
    }
    (ptr - base).try_into().unwrap()
}

pub(crate) fn to64(base: u64, ptr: u32) -> u64 {
    if ptr == 0 {
        return 0;
    }
    base + ptr as u64
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct FFI32_ArrowArray {
    pub(crate) length: i64,
    pub(crate) null_count: i64,
    pub(crate) offset: i64,
    pub(crate) n_buffers: i64,
    pub(crate) n_children: i64,
    pub(crate) buffers: u32,
    children: u32,
    dictionary: u32,
    release: u32,
    private_data: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct FFI64_ArrowArray {
    pub(crate) length: i64,
    pub(crate) null_count: i64,
    pub(crate) offset: i64,
    pub(crate) n_buffers: i64,
    pub(crate) n_children: i64,
    pub(crate) buffers: u64,
    children: *mut *mut FFI64_ArrowArray,
    dictionary: *mut FFI64_ArrowArray,
    release: Option<unsafe extern "C" fn(arg1: *mut FFI64_ArrowArray)>,
    private_data: *mut c_void,
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct FFI32_ArrowArray_PrivateData {
    inner: *mut FFI64_ArrowArray,
    children_buffer_ptr: u32,
    children_buffer_size: u32,
    buffers_ptr: u32,
    buffers_size: u32,
    instance: *mut CoreInstance,
}

// Release function for array, it is imported and will be called from Wasm side
// For security reasons it is better to use the array as we stored it in the
// global variable and not to rely on the parameter that the function got
pub fn release_exported_array(_array32: u32) {
    let base;
    let global_array;
    unsafe {
        base = GLOBAL_ENV.base_mem;
        global_array = GLOBAL_ENV.array32;
        // Get the arrow array and its private data
        let array = global_array as *mut FFI32_ArrowArray;
        let private_data = to64(base, (*array).private_data) as *mut FFI32_ArrowArray_PrivateData;
        let mut inner = (*private_data).inner;
        // release the 64 bit (this will release the 64 children also)
        match (*inner).release {
            None => (),
            Some(release) => release(inner),
        };
        (*inner).release = None;

        let instance = (*private_data).instance;

        // Go over the children and call this function with each child in order to release the 32bit-related memory
        // The children's memory (shared for 32bit and 64bit) was released by calling the 64bit's release function
        if (*array).n_children > 0 {
            for i in 0..(*array).n_children as usize {
                // Get the i-th child
                // Get the start address of the array of the children
                let children = to64(base, (*array).children) as *const u32;
                let child = children.add(i);
                let child = to64(base, *child);
                let child = &mut *(child as *mut FFI32_ArrowArray);
                if child.release != 0 {
                    // Use this function to release the child
                    // we need to change the global variable to the child
                    GLOBAL_ENV.array32 = child as *const _ as u64;
                    release_exported_array(0);
                    (*instance).deallocate_buffer(
                        to32(base, child as *const _ as u64),
                        size_of::<FFI32_ArrowArray>() as u32,
                    );
                }
            }
            // After traversing the children, we can release the array of the children
            (*instance).deallocate_buffer(
                (*private_data).children_buffer_ptr,
                (*private_data).children_buffer_size,
            );
        }

        // Release buffers array
        if (*array).n_buffers > 0 {
            (*instance)
                .deallocate_buffer((*private_data).buffers_ptr, (*private_data).buffers_size);
        }
        // Release the private_data memory
        (*instance).deallocate_buffer(
            to32(base, private_data as u64),
            size_of::<FFI32_ArrowArray_PrivateData>() as u32,
        );

        (*array).release = 0;
        // let array_ptr32 = to32(base, array as u64);
        // (*instance).deallocate_buffer(array_ptr32, 60);
        // (*instance).deallocate_buffer(array_ptr32, size_of::<FFI32_ArrowArray>() as u32);
    }
}

impl FFI32_ArrowArray {
    // Allocate new FFI32_ArrowArray using a Wasm allocations
    pub(crate) fn new(instance: &mut CoreInstance) -> *mut Self {
        let allocated_offset = instance.allocate_buffer(size_of::<Self>() as u32);
        let a32 = to64(instance.allocator_base(), allocated_offset) as *mut FFI32_ArrowArray;
        a32 as *mut Self
    }

    // Allocate new FFI32_ArrowArray using a Wasm allocations
    pub(crate) fn new_root(instance: &mut CoreInstance) -> *mut Self {
        let a32 = instance.new_ffi_array();
        let a32 = to64(instance.allocator_base(), a32);
        a32 as *mut Self
    }

    // pub fn delete(instance: &CoreInstance, array_ptr: u32) {
    //     instance.deallocate_buffer(array_ptr, 64);
    //     // instance.deallocate_buffer(array_ptr, size_of::<Self>() as u32);
    //     println!("delete array");
    // }

    // Convert the given 64bit array to 32bit array
    pub fn from(&mut self, instance: &mut CoreInstance, a64: &mut FFI64_ArrowArray) {
        let base = instance.allocator_base();

        // Allocate a memory for private_data struct
        self.private_data =
            instance.allocate_buffer(size_of::<FFI32_ArrowArray_PrivateData>() as u32);
        let private_data = to64(base, self.private_data) as *mut FFI32_ArrowArray_PrivateData;
        let private_data = unsafe { &mut *private_data };
        private_data.inner = a64;
        private_data.instance = instance as *const _ as *mut CoreInstance;
        // Fill the fields
        self.length = a64.length;
        self.null_count = a64.null_count;
        self.offset = a64.offset;
        self.n_buffers = a64.n_buffers;
        self.n_children = a64.n_children;

        // Allocate a new array to store the buffers addresses
        if self.n_buffers > 0 {
            // Allocate WASM memory for buffers array
            private_data.buffers_size = (self.n_buffers * 4) as u32;
            private_data.buffers_ptr = instance.allocate_buffer(private_data.buffers_size);

            let a64_buffers_array = a64.buffers as *const u64;
            let a32_buffers_array = to64(base, private_data.buffers_ptr) as *mut u32;
            for i in 0..a64.n_buffers as usize {
                // Get the address of the i-th buffer and set it to the new array
                let a64_buffer_item = unsafe { a64_buffers_array.add(i) };
                let a64_buffer_item = unsafe { *a64_buffer_item };
                let a32_buffer_item = unsafe { a32_buffers_array.add(i) };
                unsafe { *a32_buffer_item = to32(base, a64_buffer_item) };
            }
            self.buffers = private_data.buffers_ptr;
            // mem::forget(private_data.buffers_ptr);
        }

        // Recursively convert the children (the sub-arrays) from 64 to 32
        if self.n_children > 0 {
            // Allocate WASM memory for children array
            private_data.children_buffer_size = (self.n_children * 4) as u32;
            private_data.children_buffer_ptr =
                instance.allocate_buffer(private_data.children_buffer_size);

            let a64_children_array = a64.children as *const u64;
            let a32_children_array = to64(base, private_data.children_buffer_ptr) as *mut u32;
            for i in 0..a64.n_children as usize {
                // address of the i-th child of the FFI64
                let a64_child_item = unsafe { a64_children_array.add(i) };
                let a64_child_item = unsafe { *a64_child_item };
                // let a64_child_item = to32(base, a64_child_item as u64);

                // address of the i-th cell in the allocated children array of FFI32
                let a32_child_item = unsafe { a32_children_array.add(i) };
                // the FFI64_ArrowSchema of the i-th child
                let a64_child = unsafe { &mut *(a64_child_item as *mut FFI64_ArrowArray) };
                // let a32_child = FFI32_ArrowArray::new_root(instance);
                let a32_child = FFI32_ArrowArray::new(instance);
                unsafe {
                    // Fill the new child with s64_child
                    FFI32_ArrowArray::from(&mut *a32_child, instance, a64_child);
                    // Fill the i-th cell in children array with the address of the new child
                    *a32_child_item = to32(base, a32_child as u64);
                }
            }
            self.children = private_data.children_buffer_ptr;
        }

        let fun_ptr = release_exported_array as *const () as u64;
        self.release = fun_ptr as u32;
    }
}

impl Default for FFI64_ArrowSchema {
    fn default() -> Self {
        Self {
            format: 0,
            name: 0,
            metadata: 0,
            flags: 0,
            n_children: 0,
            children: 0,
            dictionary: 0,
            release: None,
            private_data: 0,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct FFI64_ArrowSchema_PrivateData {
    pub(crate) inner: *mut FFI32_ArrowSchema,
    pub(crate) children_ptr: *mut *mut FFI64_ArrowSchema,
    instance: *mut CoreInstance,
}

unsafe extern "C" fn release_schema64(_schema: *mut FFI64_ArrowSchema) {}

// Release function for schema 64
pub fn release_exported_schema64(_schema64: u64) {
    let base;
    let global_schema;
    unsafe {
        base = GLOBAL_ENV.base_mem;
        global_schema = GLOBAL_ENV.schema64;
        // Get the schema and the private data
        let schema = global_schema as *mut FFI64_ArrowSchema;
        let private_data = (*schema).private_data as *mut FFI64_ArrowSchema_PrivateData;
        let mut inner = (*private_data).inner;
        let instance = (*private_data).instance;
        // Call the release function of the 32bit (that releases the children)     
        (*instance).release_schema32(to32(base, inner as u64));
        (*inner).release = 0;

        // Go over the children and call this function with each child in order to release the 64bit-related memory
        // The children's memory (shared for 32bit and 64bit) was released by calling the 32bit's release function
        if (*schema).n_children > 0 {
            for i in 0..(*schema).n_children as usize {
                // Get the i-th child
                // Get the start address of the array of the children
                let children = (*schema).children as *const u64;
                // Step to the i-th entry of the array which stores the address of the i-th child
                let child = children.add(i);
                let child = *child;
                // Now we have the needed address, we can get the child
                let child = &mut *(child as *mut FFI64_ArrowSchema);
                if child.release != None {
                    // Use this function to release the child also, we need to change the global variable to the child
                    GLOBAL_ENV.schema64 = child as *const _ as u64;
                    // The value of the parameter is not used
                    release_exported_schema64(0);
                }
            }
            // After traversing the children, we can release the array of the children
            let _children_box = Box::from_raw((*private_data).children_ptr);
        }
        // Release the private_data memory
        let _private_data_box = Box::from_raw(private_data);
        (*schema).release = None;
        // println!("release func8");
    }
}

impl FFI64_ArrowSchema {
    pub(crate) fn new() -> Self {
        Default::default()
    }

    /// Core allocates empty s32 in WASM memory (`out32`)
    /// Rust fills out32
    /// Core allocates empty s64 (`out64`)
    /// Core transmutes out32 into out64:
    /// 1. Add `base` to addresses
    /// 2. Allocates any auxilary structures
    /// 3. Overrides private data to hold `out32` and self allocations (from 2)
    /// 4. Overrides release callback to a function that frees out32* and self allocations
    ///     * frees out32 using an exported function
    pub(crate) fn from(&mut self, instance: &mut CoreInstance, s32: &mut FFI32_ArrowSchema) {
        // println!("start from 64 to 32 schema");
        let base = instance.allocator_base();
        self.format = to64(base, s32.format);
        self.name = to64(base, s32.name);
        self.metadata = to64(base, s32.metadata);
        self.flags = s32.flags;
        self.n_children = s32.n_children;

        // Recursively convert the children (the sub-schemas) from 64 to 32
        if self.n_children > 0 {
            let s32_children_array_ptr = to64(base, s32.children) as *const u32;
            let s64_children_array: Vec<FFI64_ArrowSchema> = (0..s32.n_children as usize)
                .map(|i| {
                    let s32_child_item = unsafe { s32_children_array_ptr.add(i) };
                    let s32_child_item = unsafe { *s32_child_item };
                    let s32_child = to64(base, s32_child_item);
                    let s32_child = unsafe { &mut *(s32_child as *mut FFI32_ArrowSchema) };
                    let mut s64_child = FFI64_ArrowSchema::new();
                    // println!("s32 child = {:?}", s32_child);
                    s64_child.from(instance, s32_child);
                    // println!("s64 child = {:?}", s64_child);
                    s64_child
                })
                .collect();

            let mut children_ptr = s64_children_array
                .into_iter()
                .map(Box::new)
                .map(Box::into_raw)
                .collect::<Box<_>>();
            self.children = children_ptr.as_mut_ptr() as u64;
            mem::forget(children_ptr);
        }

        // Dictionary
        // if !self.dictionary.is_null() {}

        let private_data = FFI64_ArrowSchema_PrivateData {
            inner: s32,
            children_ptr: self.children as *mut *mut FFI64_ArrowSchema,
            instance: instance as *const _ as *mut CoreInstance,
        };

        self.private_data = Box::into_raw(Box::new(private_data)) as u64;

        // release
        self.release = Some(release_schema64);
        // self.release = Wasm function?
    }
}

impl Default for FFI64_ArrowArray {
    fn default() -> Self {
        Self {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: 0,
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        }
    }
}

unsafe extern "C" fn release_array64(_array: *mut FFI64_ArrowArray) {}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct FFI64_ArrowArray_PrivateData {
    pub(crate) inner: *mut FFI32_ArrowArray,
    pub(crate) buffers_ptr: u64,
    pub(crate) buffers_size: u32,
    pub(crate) children_ptr: *mut *mut FFI64_ArrowArray,
    pub(crate) children_size: u32,
    instance: *mut CoreInstance,
}

pub fn release_exported_array64(_array64: u64) {
    // println!("release func array 64");
    let base;
    let global_array;
    unsafe {
        base = GLOBAL_ENV.base_mem;
        global_array = GLOBAL_ENV.array64;
        // Get the array and the private data
        let array = global_array as *mut FFI64_ArrowArray;
        let private_data = (*array).private_data as *mut FFI64_ArrowArray_PrivateData;
        
        let mut inner = (*private_data).inner;
        let instance = (*private_data).instance;
        // Call the release function of the 32bit (that releases the children)
        (*instance).release_array32(to32(base, inner as u64));
        (*inner).release = 0;

        // Go over the children and call this function with each child in order to release the 64bit-related memory
        // The children's memory (shared for 32bit and 64bit) was released by calling the 32bit's release function
        if (*array).n_children > 0 {
            for i in 0..(*array).n_children as usize {
                // Get the i-th child
                // Get the start address of the array of the children
                let children = (*array).children as *const u64;
                // Step to the i-th entry of the array which stores the address of the i-th child
                let child = children.add(i);
                let child = *child;
                let child_to_release = child;
                // Now we have the needed address, we can get the child
                // let child = unsafe { Box::from_raw(child as *mut FFI64_ArrowArray) };
                let child = &mut *(child as *mut FFI64_ArrowArray);
                if child.release != None {
                    // Use this function to release the child also, we need to change the global variable to the child
                    GLOBAL_ENV.array64 = child as *const _ as u64;
                    // The value of the parameter is not used
                    release_exported_array64(0);
                }
                let _child = Box::from_raw(child_to_release as *mut FFI64_ArrowArray);
            }
            // After traversing the children, we can release the array of the children
            deallocate_buffer(
                (*private_data).children_ptr as u64,
                (*private_data).children_size,
            );
        }
        // Release the buffers array
        deallocate_buffer((*private_data).buffers_ptr, (*private_data).buffers_size);

        // Release the private_data memory
        let _private_data_box = Box::from_raw(private_data);
        (*array).release = None;
        // println!("release func8");
    }
}

impl FFI64_ArrowArray {
    pub(crate) fn new() -> Self {
        Default::default()
    }

    pub fn from(&mut self, instance: &mut CoreInstance, a32: &mut FFI32_ArrowArray) {
        // println!("start from 32 to 64 array");
        let base = instance.allocator_base();

        self.length = a32.length;
        self.null_count = a32.null_count;
        self.offset = a32.offset;
        self.n_buffers = a32.n_buffers;
        self.n_children = a32.n_children;
        let mut buffers_size = 0;
        let mut children_size = 0;

        // Allocate a new array to store the buffers addresses
        if a32.n_buffers > 0 {
            let a32_buffers_array = to64(base, a32.buffers) as *const u32;
            buffers_size = (self.n_buffers * 8) as u32;
            let a64_buffers_array = allocate_buffer(buffers_size) as *mut u64;
            for i in 0..a32.n_buffers as usize {
                // unsafe { println!("from 32 to 64 a64 loop {:?}", i); }
                let a32_buffer_item = unsafe { a32_buffers_array.add(i) };
                // unsafe { println!("from 32 to 64 buffer = {:?}", a32_buffer_item); }
                let a32_buffer_item = unsafe { *a32_buffer_item };
                // unsafe { println!("from 32 to 64 buffer = {:?}", a32_buffer_item); }
                let a64_buffer_item = unsafe { a64_buffers_array.add(i) };
                unsafe {
                    *a64_buffer_item = to64(base, a32_buffer_item);
                }
            }
            self.buffers = a64_buffers_array as u64;
        }

        // Recursively convert the children (the sub-array) from 32 to 64
        if self.n_children > 0 {
            let a32_children_array_ptr = to64(base, a32.children) as *const u32;
            children_size = (self.n_children * 8) as u32;
            let a64_children_array = allocate_buffer(children_size) as *mut u64;
            for i in 0..a32.n_children as usize {
                let a32_child_item = unsafe { a32_children_array_ptr.add(i) };
                let a32_child_item = unsafe { *a32_child_item };
                let a32_child = to64(base, a32_child_item);
                let a32_child = unsafe { &mut *(a32_child as *mut FFI32_ArrowArray) };
                let mut a64_child = FFI64_ArrowArray::new();
                a64_child.from(instance, a32_child);
                let a64_child_item = unsafe { a64_children_array.add(i) };
                let a64_child_ptr = Box::into_raw(Box::new(a64_child)) as *const _ as u64;
                unsafe {
                    *a64_child_item = a64_child_ptr;
                }
            }
            self.children = a64_children_array as u64 as *mut *mut FFI64_ArrowArray;
        }
        // Dictionary
        // if !self.dictionary.is_null() {}
        let private_data = FFI64_ArrowArray_PrivateData {
            inner: a32,
            buffers_ptr: self.buffers,
            buffers_size,
            children_ptr: self.children,
            children_size,
            instance: instance as *const _ as *mut CoreInstance,
        };
        self.private_data = Box::into_raw(Box::new(private_data)) as *mut c_void;

        // release
        self.release = Some(release_array64);
    }
}

// #[repr(C)]
// #[derive(Debug)]
// pub(crate) struct FFI32_ArrowSchema_PrivateData {
//     pub(crate) inner: FFI64_ArrowSchema,
//     children_buffer_ptr: u32,
//     children_buffer_size: u32,
// }

// unsafe extern "C" fn release_FFI32_ArrowSchema(schema: *mut FFI32_ArrowSchema) {
//     if schema.is_null() {
//         return;
//     }

//     let schema = &mut *schema;
//     let private_data = schema.private_data as *mut FFI32_ArrowSchema_PrivateData;
//     let private_data = &mut *private_data;
//     private_data.inner.release;
//     match private_data.inner.release {
//         None => (),
//         Some(release) => release(&mut private_data.inner),
//     };
// }

// impl FFI32_ArrowSchema {
//     fn new(instance: &CoreInstance) -> *mut Self {
//         let allocated_offset = instance.allocate_buffer(size_of::<Self>() as u32);
//         let s32 = to64(instance.allocator_base(), allocated_offset) as *mut FFI32_ArrowSchema;
//         s32 as *mut Self
//     }

//     /// Plugin allocates empty s32 in WASM memory (`in32`)
//     /// Core allocates empty s64 (`in64`)
//     /// Java fills in64 using WASM allocator
//     /// Core transmutes in64 into in32:
//     /// 1. Remove `base` from addresses
//     /// 2. Allocates any auxilary structures in WASM memory
//     /// 3. Overrides private data to hold `in64` and self allocations (from 2)
//     /// 4. Overrides release callback to a function that frees in64 and self allocations
//     /// 5. WITHIN WASM: Overrides release callback to imported function
//     pub fn from(instance: &CoreInstance, s64: &mut FFI64_ArrowSchema) -> *mut Self {
//         let root = Self::new(instance);
//         let mut root = unsafe { &mut *root };

//         let base = instance.allocator_base();

//         root.private_data =
//             instance.allocate_buffer(size_of::<FFI32_ArrowSchema_PrivateData>() as u32);
//         let private_data = root.private_data as *mut FFI32_ArrowSchema_PrivateData;
//         let private_data = unsafe { &mut *private_data };
//         private_data.inner = *s64;

//         root.format = to32(base, s64.format);
//         root.name = to32(base, s64.name);
//         root.metadata = to32(base, s64.metadata);
//         root.flags = s64.flags;
//         root.n_children = s64.n_children;

//         // Children
//         if root.n_children > 0 {
//             // Allocate WASM memory for children array
//             private_data.children_buffer_size = (root.n_children * 4) as u32;
//             private_data.children_buffer_ptr =
//                 instance.allocate_buffer(private_data.children_buffer_size);

//             // Copy children from 64bit structures
//             let s64_children_array = s64.children as *const u64;
//             let s32_children_array = to64(base, private_data.children_buffer_ptr) as *mut u32;
//             for i in 0..s64.n_children as usize {
//                 let s64_child_item = unsafe { s64_children_array.add(i) };
//                 let s32_child_item = unsafe { s32_children_array.add(i) };
//                 let s64_child = unsafe { &mut *(s64_child_item as *mut FFI64_ArrowSchema) };
//                 let s32_child = Self::from(instance, s64_child);
//                 unsafe {
//                     *s32_child_item = to32(base, s32_child as u64);
//                 }
//             }
//             root.children = private_data.children_buffer_ptr;
//         }

//         // dictionary
//         root.dictionary = 0;
//         if !s64.dictionary.is_null() {}

//         // The release callback is set in the WASM module
//         root.release = None; // TODO

//         // Move old schema
//         s64.release = None;

//         root as *mut Self
//     }
// }

// impl FFI32_ArrowArray {}

// impl FFI64_ArrowArray {
//     pub(crate) fn new() -> Self {
//         Default::default()
//     }

//     pub fn from(instance: &CoreInstance, s32: &mut FFI32_ArrowArray) -> Self {
//         let mut root = Self::new();
//         root
//     }
// }

// 32 to 64 buffers
// // let a64_buffers_array: Vec<u64> = (0..a32.n_buffers as usize)
// //     .map(|i| {
// //         let a32_buffer = unsafe { a32_buffers_array.add(i) };
// //         unsafe { println!("n_buffers = {:?}", a32.n_buffers); }
// //         let a32_buffer = unsafe { *a32_buffer };
// //         let mut a64_buffer = to64(base, a32_buffer);
// //         // mem::forget(a64_buffer);
// //         unsafe { println!("buffer = {:?}, 32 = {:?}", (a64_buffer), a32_buffer); }
// //        // a64_buffer = to64(base, 1469056);
// //         a64_buffer
// //     })
// //     .collect();
// // let a64_buffers_ptr = a64_buffers_array
// //     .into_iter()
// //     .map(Box::new)
// //     .map(Box::into_raw)
// //     .collect::<Box<_>>();
// // let mut a64_buffers_array: Vec<u64> = Vec::new(); // vec![0; a32.n_buffers as usize];
// let buffers_size = (self.n_buffers * 8) as u32;
// let mut a64_buffers_array = allocate_buffer(buffers_size) as *mut u64;
// // let mut a64_buffers_array = to64(base, instance.allocate_buffer(buffers_size)) as *mut u64;
// unsafe { println!("from 32 to 64 a64 buffers array"); }
// // let a64_buffers_ptr = &a64_buffers_array as *const _ as *mut u64;
// for i in 0..a32.n_buffers as usize {
//     unsafe { println!("from 32 to 64 a64 loop {:?}", i); }
//     let a32_buffer_item = unsafe { a32_buffers_array.add(i) };
//         unsafe { println!("from 32 to 64 buffer = {:?}", a32_buffer_item); }
//         let a32_buffer_item = unsafe { *a32_buffer_item };
//         unsafe { println!("from 32 to 64 buffer = {:?}", a32_buffer_item); }
//     let a64_buffer_item = unsafe{ a64_buffers_array.add(i) };
//     unsafe { *a64_buffer_item = to64(base, a32_buffer_item); }
//     // let a64_buffer_item = unsafe { a64_buffers_ptr.add(i) };
//     // if i > 0 {
//     //     unsafe {
//     //         let vec_buf = to64(base, a32_buffer_item) as *mut u8;
//     //         println!("from 32 to 64 a64 a64, buf byte array = {:?}", *vec_buf);
//     //     }
//     // }
//     // // unsafe { *a64_buffer_item = to64(base, a32_buffer_item) };
//     // a64_buffers_array.push(to64(base, 1469056));
//     // // a64_buffers_array.push(to64(base, a32_buffer_item));
//     // println!("buffers vec = {:?}", a64_buffers_array);
// }
