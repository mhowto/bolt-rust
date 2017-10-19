use db::Meta;
use page::Page;
use types::pgid_t;

pub struct Tx {
    pub meta: Meta,
}

impl Tx {
    pub fn page(&self, pgid: pgid_t) -> Option<Page> {
        None
    }
}
