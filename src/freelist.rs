use types::txid_t;
use page::Page;
use std::rc::Rc;
use std::cell::RefCell;

pub struct FreeList {

}

impl FreeList {
    pub fn new() -> FreeList {
        FreeList{
        }
    }
    pub fn free(&self, txid: txid_t, p: Rc<RefCell<Page>>) {
    }
}