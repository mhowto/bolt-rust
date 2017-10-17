use types::pgid_t;
use types::txid_t;
use bucket::_Bucket;

pub struct Meta {
   	pub magic:    u32,
	pub version:  u32,
	pub pageSize: u32,
	pub flags:    u32,
	pub root:     _Bucket,
	pub freelist: pgid_t,
	pub pgid:     pgid_t,
	pub txid:     txid_t,
	pub checksum: u64,
}

impl Meta {
	pub fn new() -> Meta {
		Meta{
   			magic: 0,
			version: 0,
			pageSize: 0,
			flags: 0,
			root: _Bucket::new(),
			freelist: 0,
			pgid: 0,
			txid: 0,
			checksum: 0,
		}
	}
}
