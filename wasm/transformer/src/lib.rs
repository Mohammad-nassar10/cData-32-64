extern crate wee_alloc;

// Use `wee_alloc` as the global allocator.
// #[global_allocator]
// static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

pub mod allocator;
pub mod schema;
pub mod array;
pub mod common;
pub mod transform;