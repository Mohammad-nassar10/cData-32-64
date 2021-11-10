use std::{convert::TryInto, mem::{self, size_of}, os::raw::{c_char, c_void}, ptr::{self, NonNull}};
use std::sync::Arc;
use crate::core::CoreInstance;
use arrow::ffi::FFI_ArrowSchema;

/// ABI-compatible struct for `ArrowSchema` from C Data Interface
/// See <https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions>
/// This was created by bindgen
#[repr(C)]
#[derive(Debug)]
#[derive(Copy, Clone)]
pub struct FFI64_ArrowSchema {
    pub format: *const c_char,
    name: *const c_char,
    metadata: *const c_char,
    flags: i64,
    n_children: i64,
    pub children: *mut *mut FFI64_ArrowSchema,
    dictionary: *mut FFI64_ArrowSchema,
    pub release: Option<unsafe extern "C" fn(arg1: *mut FFI64_ArrowSchema)>,
    private_data: *mut c_void,
}

#[repr(C)]
#[derive(Debug)]
#[derive(Copy, Clone)]
pub(crate) struct FFI32_ArrowSchema {
    format: u32,
    name: u32,
    metadata: u32,
    flags: i64,
    n_children: i64,
    children: u32,
    pub dictionary: u32,
    // pub release: Option<unsafe extern "C" fn(arg1: *mut FFI32_ArrowSchema)>,
    pub release: Option<u16>,
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

// struct Base_Env {
//     base: u64
// }


// FFI definition form arrow-rs
// pub struct FFI_ArrowSchema {
//     format: *const c_char,
//     name: *const c_char,
//     metadata: *const c_char,
//     flags: i64,
//     n_children: i64,
//     children: *mut *mut FFI_ArrowSchema,
//     dictionary: *mut FFI_ArrowSchema,
//     release: Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowSchema)>,
//     private_data: *mut c_void,
// }

// FFI definition from arrow-rs
// pub struct FFI_ArrowArray {
//     pub(crate) length: i64,
//     pub(crate) null_count: i64,
//     pub(crate) offset: i64,
//     pub(crate) n_buffers: i64,
//     pub(crate) n_children: i64,
//     pub(crate) buffers: *mut *const c_void,
//     children: *mut *mut FFI_ArrowArray,
//     dictionary: *mut FFI_ArrowArray,
//     release: Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowArray)>,
//     // When exported, this MUST contain everything that is owned by this array.
//     // for example, any buffer pointed to in `buffers` must be here, as well
//     // as the `buffers` pointer itself.
//     // In other words, everything in [FFI_ArrowArray] must be owned by
//     // `private_data` and can assume that they do not outlive `private_data`.
//     private_data: *mut c_void,
// }

pub(crate) struct GlobalEnv {
    pub(crate) base_mem: u64,
    pub(crate) schema: u64,
}

pub(crate) static mut GLOBAL_ENV: GlobalEnv = GlobalEnv{base_mem : 0, schema: 0};


unsafe extern "C" fn tmp_func(arg1: *mut FFI32_ArrowSchema) {
    
}


pub fn release_exported_schema(_schema32: u32) {
    println!("release func");
    let base;
    let global_schema;
    unsafe { base = GLOBAL_ENV.base_mem; };
    unsafe { global_schema = GLOBAL_ENV.schema; };
    println!("base global = {:?}, schema = {:?}", base, global_schema);
    // let base = self.base;
    // if (schema32.release == None) {
    //     return;
    // }
    // calling the release function of the 64bit (that releases the children)
    unsafe{
        // let mut schema = &*(to64(base, schema32) as *mut FFI_ArrowSchema);
        let schema = global_schema as *mut FFI32_ArrowSchema;
        println!("release func schema = {:?}", schema);
        // let schema = Arc::into_raw(Arc::new(schema)) as *const FFI32_ArrowSchema;
        // let mut schema = *schema;
        println!("release func schema = {:?}", *schema);
        let private_data = to64(base, (*schema).private_data) as *mut FFI32_ArrowSchema_PrivateData;
        let mut inner = (*private_data).inner;
        println!("inner = {:?}\nprivate data = {:?}", *inner, *private_data);
        // if (*inner).release != None {
            match (*inner).release {
                None => (),
                Some(release) => unsafe { release(inner) },
            };
        println!("inner = {:?}\nprivate data = {:?}", *inner, *private_data);
        //     let release_func = std::mem::transmute::<*const (), fn(*mut FFI64_ArrowSchema)>((*inner).release.unwrap() as *const());
        //     println!("release func1, inner release func = {:?}", release_func);
        //     release_func(inner);
            (*inner).release = None;
        // }
        println!("release func2");
        let instance = (*private_data).instance;
        // (*instance).deallocate_buffer((*private_data).children_buffer_ptr, (*private_data).children_buffer_size);
        println!("release func3, children buffer ptr = {:?}, schema.children = {:?}", (*private_data).children_buffer_ptr, (*schema).children);

        // Should we call the release for the children? or it is called from the release of the inner?
        if (*schema).n_children > 0 {
            for i in 0..(*schema).n_children as usize {
                let children = to64(base, (*schema).children) as *const u32;
                let child = children.add(i);
                let child = unsafe { to64(base, *child) };
                let child = unsafe { &mut *(child as *mut FFI32_ArrowSchema) };
                // let child = child as *mut FFI32_ArrowSchema;
                println!("release func4, child = {:?}", child);
                if child.release != None {
                    // // call child release function
                    // // println!("store = {:?}", (*child).release);
                    // // let func_store = to64(base, (*child).release.unwrap()) as *mut u64;
                    // // let func_addr = *func_store;
                    // let func_addr = child.release.unwrap();
                    // // println!("func addr {:?}, store {:?}", func_addr, func_store);
                    // let release_func = std::mem::transmute::<*const (), fn(schema32: u32)> (func_addr as *const());
                    // println!("release func5 {:?}, func addr {:?}", release_func, func_addr);
                    // release_func(to32(base, child as *const _ as u64));
                    GLOBAL_ENV.schema = child as *const _ as u64;
                    release_exported_schema(0);
                    println!("release func6, child = {:?}", child);
                }
            }
            println!("release, children buffer ptr = {:?}, schema.children = {:?}", (*private_data).children_buffer_ptr, (*schema).children);
            (*instance).deallocate_buffer((*private_data).children_buffer_ptr, (*private_data).children_buffer_size);
        }
        
        // deallocate private_data? deallocate the schema itself?
        (*instance).deallocate_buffer(to32(base, private_data as u64), size_of::<FFI32_ArrowSchema_PrivateData>() as u32);
        println!("release func7");

        // 
        (*schema).release = None;
        println!("release func8");
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
    pub fn new(instance: &CoreInstance) -> *mut Self {
        println!("gg size of FFI32_ArrowSchema = {:?}, size of release = {:?}, i64 = {:?}", size_of::<Self>(), size_of::<Option<u16>>(), size_of::<i64>());
        let allocated_offset = instance.allocate_buffer(size_of::<Self>() as u32);
        let s32 = to64(instance.allocator_base(), allocated_offset) as *mut FFI32_ArrowSchema;
        s32 as *mut Self
    }

    pub fn from(&mut self, instance: &CoreInstance, s64: &mut FFI64_ArrowSchema) {
        println!("start of arch from function = {:?}", s64.n_children);
        let base = instance.allocator_base();
        unsafe { GLOBAL_ENV.base_mem = base; }

        self.private_data =
            instance.allocate_buffer(size_of::<FFI32_ArrowSchema_PrivateData>() as u32);
        let tmp = self.private_data; 
        let private_data = to64(base, self.private_data) as *mut FFI32_ArrowSchema_PrivateData;
        let private_data = unsafe { &mut *private_data };
        println!("from function 1");
        private_data.inner = s64;
        private_data.instance = instance as *const _ as *mut CoreInstance;
        
        self.format = to32(base, s64.format as u64);
        println!("from function 2");
        self.name = to32(base, s64.name as u64);
        self.metadata = to32(base, s64.metadata as u64);
        self.flags = s64.flags;
        self.n_children = s64.n_children;

        // Recursively convert the children (the sub-schemas) from 64 to 32
        if self.n_children > 0 {
            // Allocate WASM memory for children array
            private_data.children_buffer_size = (self.n_children * 4) as u32;
            private_data.children_buffer_ptr =
                instance.allocate_buffer(private_data.children_buffer_size);
            println!("from function 3, children ptr = {:?}", s64.children);
            // Copy children from 64bit structures
            // let s64_child = unsafe { *s64_children_array };
            // let s64_child = to32(self.base, s64_child as u64);
            // let s64_child = unsafe { &mut *(s64_child as *mut FFI64_ArrowSchema) };
            // println!("children number = {:?}", s64_child);


            let s64_children_array = s64.children as *const u64;
            let s32_children_array = to64(base, private_data.children_buffer_ptr) as *mut u32;
            for i in 0..s64.n_children as usize {
                // address of the i-th child of the FFI64
                let s64_child_item = unsafe { s64_children_array.add(i) };
                let s64_child_item = unsafe { *s64_child_item };
                // let s64_child_item = to32(base, s64_child_item as u64);

                // address of the i-th cell in the allocated children array of FFI32
                let s32_child_item = unsafe { s32_children_array.add(i) };
                println!("s43_child_item = {:?}", s32_child_item);
                // the FFI64_ArrowSchema of the i-th child 
                let s64_child = unsafe { &mut *(s64_child_item as *mut FFI64_ArrowSchema) };
                println!("children number = {:?}", s64_child);
                // let s32_child = Self::from(instance, s64_child);
                let s32_child = FFI32_ArrowSchema::new(instance);
                unsafe {
                    // Fill the new child with s64_child
                    FFI32_ArrowSchema::from(&mut *s32_child, instance, s64_child);
                    // Fill the i-th cell in children array with the address of the new child
                    *s32_child_item = to32(base, s32_child as u64);
                }
            }
            self.children = private_data.children_buffer_ptr;
            println!("self.children = {:?}", self.children);
        }
        println!("from function 4");
        // dictionary
        self.dictionary = 0;
        if !s64.dictionary.is_null() {}

        // The release callback is set in the WASM module
        // self.release = None; // TODO
        // type release_func = fn(*mut FFI32_ArrowSchema);
        // Allocate a memory in Wasm module to store the pointer to release function
        // let func_store = instance.allocate_buffer(size_of::<*mut u64>() as u32);
        // let func_store64 = to64(base, func_store) as *mut u64;
        // println!("pointer to function = {:?}", release_exported_array as *const ());
        // let base_env = Base_Env {base};
        // let release_callback = |schema: u32| base_env.release_exported_array(schema);
        // let fun_ptr = release_callback as *const () as u64;
        let fun_ptr = release_exported_schema as *const () as u64;
        println!("pointer to function = {:?}", fun_ptr);
        // unsafe { *func_store64 = fun_ptr; }
        // println!("pointer to function = {:?}, store64 = {:?}, store = {:?}", fun_ptr, func_store64, func_store);

        // self.release = Some(tmp_func);
        self.release = Some(fun_ptr as u16);
        // self.release = None;
        self.private_data = tmp;
        // unsafe { println!("from function 5, {:?}", (func_store)) };
        // Move old schema
        // Do we need the s64 release?
        // s64.release = None;

        // root as *mut Self
    }
}

pub(crate) fn to32(base: u64, ptr: u64) -> u32 {
    if ptr == 0 {
        return 0;
    }
    (ptr - base).try_into().unwrap()
}

fn to64(base: u64, ptr: u32) -> u64 {
    if ptr == 0 {
        return 0;
    }
    base + ptr as u64
}

// #[repr(C)]
// #[derive(Debug, Clone, Copy)]
// // #[derive(Debug)]
// pub(crate) struct FFI32_ArrowSchema {
//     format: u32,
//     name: u32,
//     metadata: u32,
//     flags: i64,
//     n_children: i64,
//     children: u32,
//     dictionary: u32,
//     release: Option<u32>,
//     private_data: u32,
// }

// #[repr(C)]
// #[derive(Debug, Clone, Copy)]
// pub(crate) struct FFI64_ArrowSchema {
//     format: u64,
//     name: u64,
//     metadata: u64,
//     flags: i64,
//     n_children: i64,
//     children: *mut *mut FFI64_ArrowSchema,
//     dictionary: *mut FFI64_ArrowSchema,
//     release: Option<unsafe extern "C" fn(arg1: *mut FFI64_ArrowSchema)>,
//     private_data: *mut c_void,
// }

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
    release: Option<u32>,
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
    children: u64,
    dictionary: u64,
    pub release: Option<u64>,
    private_data: u64,
}

#[repr(C)]
// #[derive(Debug)]
pub(crate) struct FFI32_ArrowArray_PrivateData {
    inner: FFI64_ArrowArray,
    children_buffer_ptr: u32,
    children_buffer_size: u32,
    buffers_ptr: u32,
    buffers_size: u32,
    instance: CoreInstance,
}

impl FFI32_ArrowArray {
    pub(crate) fn new(instance: &CoreInstance) -> *mut Self {
        let allocated_offset = instance.allocate_buffer(size_of::<Self>() as u32);
        let s32 = to64(instance.allocator_base(), allocated_offset) as *mut FFI32_ArrowArray;
        s32 as *mut Self
    }
    pub fn from(&mut self, instance: &CoreInstance, s64: &mut FFI64_ArrowArray) {
        let base = instance.allocator_base();

        self.private_data =
            instance.allocate_buffer(size_of::<FFI32_ArrowArray_PrivateData>() as u32);
        let private_data = to64(base, self.private_data) as *mut FFI32_ArrowArray_PrivateData;
        let private_data = unsafe { &mut *private_data };
        println!("array from function 1");
        private_data.inner = *s64;
        // private_data.instance = instance.clone();

        self.length = s64.length;
        self.null_count = s64.null_count;
        self.offset = s64.offset;
        self.n_buffers = s64.n_buffers;
        self.n_children = s64.n_children;

        println!("array from function 2");
        // root.buffers
        // Allocate a new array to store the buffers addresses 
        if self.n_buffers > 0 {
            // Allocate WASM memory for children array
            private_data.buffers_size = (self.n_children * 4) as u32;
            private_data.buffers_ptr =
                instance.allocate_buffer(private_data.buffers_size);

            let s64_buffers_array = s64.buffers as *const u64;
            let s32_buffers_array = to64(base, private_data.buffers_ptr) as *mut u32;
            for i in 0..s64.n_buffers as usize {
                let s64_buffer_item = unsafe { s64_buffers_array.add(i) };
                let s64_buffer_item = unsafe { *s64_buffer_item };
                let s32_buffer_item = unsafe { s32_buffers_array.add(i) };
                unsafe { *s32_buffer_item = to32(base, s64_buffer_item) };
            }
            self.buffers = private_data.buffers_ptr;
            // mem::forget(private_data.buffers_ptr);
        }
        println!("array from function 3");
        // Recursively convert the children (the sub-arrays) from 64 to 32
        if self.n_children > 0 {
            // Allocate WASM memory for children array
            private_data.children_buffer_size = (self.n_children * 4) as u32;
            private_data.children_buffer_ptr =
                instance.allocate_buffer(private_data.children_buffer_size);
            println!("array from function 4, children ptr = {:?}", s64.children);
            // Copy children from 64bit structures
            // let s64_child = unsafe { *s64_children_array };
            // let s64_child = to32(self.base, s64_child as u64);
            // let s64_child = unsafe { &mut *(s64_child as *mut FFI64_ArrowSchema) };
            // println!("children number = {:?}", s64_child);


            let s64_children_array = s64.children as *const u64;;
            let s32_children_array = to64(base, private_data.children_buffer_ptr) as *mut u32;
            for i in 0..s64.n_children as usize {
                // address of the i-th child of the FFI64
                let s64_child_item = unsafe { s64_children_array.add(i) };
                let s64_child_item = unsafe { *s64_child_item };
                // let s64_child_item = to32(base, s64_child_item as u64);

                // address of the i-th cell in the allocated children array of FFI32
                let s32_child_item = unsafe { s32_children_array.add(i) };
                // the FFI64_ArrowSchema of the i-th child 
                let s64_child = unsafe { &mut *(s64_child_item as *mut FFI64_ArrowArray) };
                println!("children number = {:?}", s64_child);
                // let s32_child = Self::from(instance, s64_child);
                let s32_child = FFI32_ArrowArray::new(instance);
                unsafe {
                    // Fill the new child with s64_child
                    FFI32_ArrowArray::from(&mut *s32_child, instance, s64_child);
                    // Fill the i-th cell in children array with the address of the new child
                    *s32_child_item = to32(base, s32_child as u64);
                }
            }
            self.children = private_data.children_buffer_ptr;
        }
        self.release = None;
    }
}

// impl FFI64_ArrowArray {
//     pub(crate) fn new() -> Self {
//         Default::default()
//     }

//     pub fn from(instance: &CoreInstance, s32: &mut FFI32_ArrowArray) -> Self {
//         let mut root = Self::new();
//         root
//     }
// }

// impl Default for FFI64_ArrowSchema {
//     fn default() -> Self {
//         Self {
//             format: 0,
//             name: 0,
//             metadata: 0,
//             flags: 0,
//             n_children: 0,
//             children: ptr::null_mut(),
//             dictionary: std::ptr::null_mut(),
//             release: None,
//             private_data: std::ptr::null_mut(),
//         }
//     }
// }

// impl Default for FFI64_ArrowArray {
//     fn default() -> Self {
//         Self {
//             length: 0,
//             null_count: 0,
//             offset: 0,
//             n_buffers: 0,
//             n_children: 0,
//             buffers: 0,
//             children: 0,
//             dictionary: 0,
//             release: None,
//             private_data: 0,
//         }
//     }
// }

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

// #[repr(C)]
// #[derive(Debug)]
// pub(crate) struct FFI64_ArrowSchema_PrivateData {
//     pub(crate) inner: FFI32_ArrowSchema,
// }

// impl FFI64_ArrowSchema {
//     pub(crate) fn new() -> Self {
//         Default::default()
//     }

//     /// Core allocates empty s32 in WASM memory (`out32`)
//     /// Rust fills out32
//     /// Core allocates empty s64 (`out64`)
//     /// Core transmutes out32 into out64:
//     /// 1. Add `base` to addresses
//     /// 2. Allocates any auxilary structures
//     /// 3. Overrides private data to hold `out32` and self allocations (from 2)
//     /// 4. Overrides release callback to a function that frees out32* and self allocations
//     ///     * frees out32 using an exported function
//     pub fn from(instance: &CoreInstance, s32: &mut FFI32_ArrowSchema) -> Self {
//         let mut root = Self::new();

//         let base = instance.allocator_base();

//         root.format = to64(base, s32.format);
//         root.name = to64(base, s32.name);
//         root.metadata = to64(base, s32.metadata);
//         root.flags = s32.flags;
//         root.n_children = s32.n_children;

//         let private_data = FFI64_ArrowSchema_PrivateData{
//             inner: *s32,
//         };

//         // Children
//         if root.n_children > 0 {
//             let children_array_ptr = to64(base, s32.children) as *const u32;
//             let children: Vec<FFI64_ArrowSchema> = (0..s32.n_children as usize)
//                 .map(|i| {
//                     let child = unsafe { children_array_ptr.add(i) };
//                     let child = unsafe { *child };
//                     let child = to64(base, child);
//                     let child = unsafe { &mut *(child as *mut FFI32_ArrowSchema) };
//                     FFI64_ArrowSchema::from(instance, child)
//                 })
//                 .collect();

//             let mut children_ptr = children
//                 .into_iter()
//                 .map(Box::new)
//                 .map(Box::into_raw)
//                 .collect::<Box<_>>();
//             root.children = children_ptr.as_mut_ptr();
//             mem::forget(children_ptr);
//         }

//         // TODO
//         if !root.dictionary.is_null() {}

//         root.private_data = Box::into_raw(Box::new(private_data)) as *mut c_void;

//         // format: u64,
//         // name: u64,
//         // metadata: u64,
//         // flags: i64,
//         // n_children: i64,
//         // children: u64,
//         // dictionary: u64,
//         // release: Option<unsafe extern "C" fn(schema: *mut FFI64_ArrowSchema)>,
//         // private_data: u64,

//         root
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
