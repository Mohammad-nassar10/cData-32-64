extern crate wee_alloc;
// #[cfg(not(target_env = "msvc"))]
// use jemallocator::Jemalloc;
// use dhat::{Dhat, DhatAlloc};
// #[global_allocator]
// static ALLOCATOR: DhatAlloc = DhatAlloc;
// Use `wee_alloc` as the global allocator.
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

// #[cfg(not(target_env = "msvc"))]
// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;

pub mod allocator;
pub mod schema;
pub mod array;
pub mod common;
pub mod transform;