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
