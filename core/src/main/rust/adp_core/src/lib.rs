use dhat::{Dhat, DhatAlloc};

#[global_allocator]
static ALLOCATOR: DhatAlloc = DhatAlloc;


mod core;
mod core_jni;
mod types;
mod arch;
