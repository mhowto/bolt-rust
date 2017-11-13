use bucket::Bucket;
use page::{Page, LEAF_PAGE_FLAG};
use node::Node;
use types::pgid_t;

use std::rc::Rc;
use std::cell::RefCell;

// Cursor represents an iterator that can traverse over all key/value pairs in a bucket in sorted order.
// Cursors see nested buckets with value == nil.
// Cursors can be obtained from a transaction and are valid as long as transaction is open.
//
// Keys and values returned from the cursor are only valid for the life of the transaction.
//
// Changing data while traversing with a cursor may cause it to be invalidated
// and return unexpected keys and/or values. You must reposition your cursor
// after mutating data.
pub struct Cursor<'a> {
    bucket: Rc<RefCell<Bucket<'a>>>,
    stack: Vec<elem_ref<'a>>,
}

impl<'a> Cursor<'a> {
    pub fn new(bucket: &Rc<RefCell<Bucket<'a>>>) -> Cursor<'a> {
        Cursor {
            bucket: Rc::clone(bucket),
            stack: vec![],
        }
    }

    // returns the bucket that this cursor was created from.
    pub fn get_bucket(&self) -> &Rc<RefCell<Bucket<'a>>> {
        &self.bucket
    }

    // First moves the cursor to the first item in the bucket and returns its key and value.
    // If the bucket is empty then a nil key and value are returned.
    // The returned key and value are only valid for the life of the transaction.
    pub fn first(&self) -> (Option<&'a str>, Option<&'a str>) {
        unimplemented!()
    }

    // moves the cursor to the first leaf element under the last page in the bucket.
    fn _first(&self) {
        unimplemented!()
    }

    // Last moves the cursor to the last item in the bucket and returns its key and value.
    // If the bucket is empty then a nil key and value are returned.
    // The returned key and value are only valid for the life of the transaction.
    pub fn last(&self) -> (Option<&'a str>, Option<&'a str>) {
        unimplemented!()
    }

    // moves the cursor to the last leaf element under the last page in the bucket.
    fn _last(&self) {
        unimplemented!()
    }

    // Next moves the cursor to the next item in the bucket and returns sits key and value.
    // If the cursor is at the end of the bucket then a nil key and value are returned.
    // The returned key and value are only valid for the life of the transaction.
    pub fn next(&self) -> (Option<&'a str>, Option<&'a str>) {
        unimplemented!()
    }

    // moves to the next leaf element and returns the key and value.
    // If the cursor is at the last leaf element then it stays there and returns nil.
    fn _next(&self) -> (Option<&'a str>, Option<&'a str>, u32) {
        unimplemented!()
    }

    // Prev moves the cursor to the previous item in the bucket and returns sits key and value.
    // If the cursor is at the beginning of the bucket then a nil key and value are returned.
    // The returned key and value are only valid for the life of the transaction.
    pub fn prev(&self) -> (Option<&'a str>, Option<&'a str>) {
        unimplemented!()
    }

    // Seek moves the cursor to a given key and returns it.
    // If the key does not exist then the next key is used. If no keys
    // follow, a nil key is returned.
    // The returned key and value are only valid for the life of the transaction.
    pub fn seek(&self, seek: &'a str) -> (Option<&'a str>, Option<&'a str>) {
        unimplemented!()
    }

    // seek moves the cursor to a given key and returns it.
    // If the key does not exist then the next key is used.
    pub fn seek1(&self, seek: &'a str) -> (Option<&'a str>, Option<&'a str>, u32) {
        unimplemented!()
    }

    // Delete removes the current key/value under the cursor from the bucket.
    // Delete fails if current key/value is a bucket or if the transaction is not writable.
    pub fn delete(&mut self) -> Result<(), &'static str>{
        unimplemented!()
    }

    // search recursively performs a binary search against a given page/node until it finds a given key.
    fn search(&self, key: &'a str, pgid: pgid_t) {
        unimplemented!()
    }

    fn search_node(&self, key: &'a str, n: Rc<RefCell<Node<'a>>>) {
        unimplemented!()
    }

    fn search_page(&self, key: &'a str, p: Rc<RefCell<Page>>) {
        unimplemented!()
    }

    // nsearch searches the leaf node on the top of the stack for a key.
    fn nsearch(&self, key: &'a str) {
        unimplemented!()
    }

    // returns the key and value of the current leaf element.
    fn key_value(&self) -> (&'a str, Option<&'a str>, u32) {
        unimplemented!()
    }
}

// elem_ref represents a reference to an element on a given page/node.
pub struct elem_ref<'a> {
    pub page: Rc<RefCell<Page>>,
    pub node: Option<Rc<RefCell<Node<'a>>>>,
    pub index: i64,
}

impl<'a> elem_ref<'a> {
    fn is_leaf(&self) -> bool {
        match self.node {
            Some(ref n) => n.borrow().is_leaf,
            None => (self.page.borrow().flags & LEAF_PAGE_FLAG) != 0,
        }
    }

    fn count(&self) -> usize {
        match self.node {
            Some(ref n) => n.borrow().inodes.len(),
            None => self.page.borrow().count as usize,
        }
    }
}