// use dhat::{Dhat, DhatAlloc};

// #[global_allocator]
// static ALLOCATOR: DhatAlloc = DhatAlloc;
// extern crate wee_alloc;
// #[global_allocator]
// static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

// use std::alloc::System;
// use wasm_tracing_allocator::WasmTracingAllocator;

// #[global_allocator]
// static GLOBAL_ALLOCATOR: WasmTracingAllocator<System> = WasmTracingAllocator(System);

mod core;
mod core_jni;
mod types;
mod arch;
