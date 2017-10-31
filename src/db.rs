use types::pgid_t;
use types::txid_t;
use bucket::_Bucket;
use freelist::FreeList;
use std::rc::Rc;
use std::cell::RefCell;

pub struct Meta {
    pub magic: u32,
    pub version: u32,
    pub page_size: u32,
    pub flags: u32,
    pub root: _Bucket,
    pub freelist: pgid_t,
    pub pgid: pgid_t,
    pub txid: txid_t,
    pub checksum: u64,
}

impl Meta {
    pub fn new() -> Meta {
        Meta {
            magic: 0,
            version: 0,
            page_size: 0,
            flags: 0,
            root: _Bucket::new(),
            freelist: 0,
            pgid: 0,
            txid: 0,
            checksum: 0,
        }
    }
}

pub struct DB {
    pub page_size: usize,

    // TODO: need to use mutex
    pub freelist: Rc<RefCell<FreeList>>,
}


impl DB {
    pub fn new() -> DB {
        DB {
            page_size: 4 * 1024,
            freelist: Rc::new(RefCell::new(FreeList::new())),
        }
    }
}
