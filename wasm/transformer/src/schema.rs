#[repr(C)]
#[derive(Debug)]
pub(crate) struct FFI64_ArrowSchema {
    format: u64,
    name: u64,
    metadata: u64,
    flags: i64,
    n_children: i64,
    pub children: u64,
    dictionary: u64,
    release: Option<u64>,
    private_data: u64,
}

impl FFI64_ArrowSchema {
    pub(crate) fn empty() -> Self {
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
