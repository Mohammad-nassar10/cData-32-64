use std::convert::TryInto;

#[no_mangle]
pub extern "C" fn allocate_buffer(len: u32) -> u32 {
    let mut buf: Vec<u8> = Vec::with_capacity(len.try_into().unwrap());
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    println!("allocate buffer ptr = {:?}", ptr as u32);
    ptr as u32
}

#[no_mangle]
pub unsafe extern "C" fn deallocate_buffer(ptr: u32, size: u32) {
    let data = Vec::from_raw_parts(ptr as *mut u8, size as usize, size as usize);
    println!("deallocate buffer ptr = {:?}", ptr);
    std::mem::drop(data);
}
