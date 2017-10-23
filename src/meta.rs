use bucket::_Bucket;
use types::{pgid_t, txid_t};

#[repr(C, packed)]
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