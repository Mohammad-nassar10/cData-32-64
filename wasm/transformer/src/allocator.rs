use std::convert::TryInto;

pub(crate) static mut GLOBAL_ALLOC_SIZE: u64 = 0;
pub(crate) static mut GLOBAL_DEALLOC_SIZE: u64 = 0;


#[no_mangle]
pub extern "C" fn allocate_buffer(len: u32) -> u32 {
    let mut buf: Vec<u8> = Vec::with_capacity(len.try_into().unwrap());
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    unsafe { 
        GLOBAL_ALLOC_SIZE += len as u64;
        println!("allocate buffer ptr = {:?}, size = {:?}", ptr as u32, GLOBAL_ALLOC_SIZE);
    }
    ptr as u32
}

#[no_mangle]
pub unsafe extern "C" fn deallocate_buffer(ptr: u32, size: u32) {
    let data = Vec::from_raw_parts(ptr as *mut u8, size as usize, size as usize);
    unsafe { 
        GLOBAL_DEALLOC_SIZE += size as u64;
        println!("deallocate buffer ptr = {:?}, size = {:?}", ptr, GLOBAL_DEALLOC_SIZE);
    }
    std::mem::drop(data);
}
