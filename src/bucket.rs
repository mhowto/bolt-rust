use types::pgid_t;
use tx::Tx;
use node::Node;
use cursor::Cursor;
use page::{Page, BUCKET_LEAF_FLAG};

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Weak;

// MAX_KEY_SIZE is the maximum length of a key, in bytes
pub const MAX_KEY_SIZE: u32 = 32768;
// MAX_VALUE_SIZE is the maximum length of a value, in bytes
pub const MAX_VALUE_SIZE: u32 = (1 << 31) - 2;

pub const MIN_FILL_PERCENT: f32 = 0.1;
pub const MAX_FILL_PERCENT: f32 = 1.0;

// DefaultFillPercent is the percentage that split pages are filled.
// This value can be changed by setting Bucket.FillPercent.
pub const DEFAULT_FILL_PERCENT: f32 = 0.5;

// Bucket represents a collection of key/value pairs inside the datasbase.
pub struct Bucket<'a> {
    pub bucket: Box<_Bucket>,
    pub tx: Rc<RefCell<Tx>>,                           // the associated transcation
    buckets: HashMap<&'static str, Rc<RefCell<Bucket<'a>>>>,        // subbucket cache
    page: Option<Rc<RefCell<Page>>>,                   // inline page reference
    pub root_node: Option<Rc<RefCell<Node<'a>>>>,      // materialized node for the root page.
    pub nodes: HashMap<pgid_t, Rc<RefCell<Node<'a>>>>, // node cache

    // Sets the threshold for filling nodes when they split. By default,
    // the bucket will fill to 50% but it can be useful to increase this
    // amount if you know that your write workloads are mostly append-only.
    //
    // This is non-persisted across transactions so it must be set in every Tx.
    pub fill_percent: f32,
    pub weak_self: Weak<RefCell<Bucket<'a>>>, // weak pointer to self
}

impl<'a> Bucket<'a> {
    pub fn new(b: Box<_Bucket>, tx: &Rc<RefCell<Tx>>) -> Bucket<'a> {
        Bucket {
            bucket: b,
            tx: Rc::clone(tx),
            buckets: HashMap::new(),
            page: None,
            root_node: None,
            nodes: HashMap::new(),
            fill_percent: 0.0,
            weak_self: Weak::new(),
        }
    }

    pub fn new_rc_refcell(b: Box<_Bucket>, tx: &Rc<RefCell<Tx>>) -> Rc<RefCell<Bucket<'a>>> {
        let b = Rc::new(RefCell::new(Bucket::new(b, tx)));
        b.borrow_mut().weak_self = Rc::downgrade(&b);
        b
    }

    // node creates a node from a page and associates it with a given parent.
    pub fn node(
        &mut self,
        pgid: pgid_t,
        parent: Option<Rc<RefCell<Node<'a>>>>,
    ) -> Rc<RefCell<Node<'a>>> {
        // Retrieve node if it's already been created.
        if let Some(ref n) = self.nodes.get(&pgid) {
            return Rc::clone(n);
        }

        // Otherwise create a node and cache it.
        let n = Rc::new(RefCell::new(
            Node::new(Rc::clone(&self.weak_self.upgrade().unwrap()))
        ));

        if let Some(ref p) = parent {
            let mut parent_node = p.borrow_mut();
            parent_node.append_child(&n);
            n.borrow_mut().set_parent(Rc::downgrade(p));
        } else {
            self.root_node = Some(Rc::clone(&n));
        }

        // use the inline page if this is an inline bucket.
        let p = &mut self.page;
        if p.is_none() {
            *p = Some(Rc::clone(&self.tx.borrow().page(pgid)));
        }

        // Read the page into the node and cache it.
        // n.read(p);
        self.nodes.insert(pgid, Rc::clone(&n));

        // Update statistics
        self.tx.borrow_mut().stats.node_count += 1;

        n
    }

    pub fn tx(&self) -> &Rc<RefCell<Tx>> {
        &self.tx
    }

    // returns the root of the bucket
    pub fn root(&self) -> pgid_t {
        self.bucket.root
    }

    pub fn writable(&self) -> bool {
        self.tx.borrow().writable
    }

    // creates a cursor associated with the bucket.
    // The cursor is only valid as long as the transaction is open.
    // Do not use a cursor after the transaction is closed.
    pub fn cursor(&mut self) -> Rc<RefCell<Cursor<'a>>> {
        // update transaction statistics.
        self.tx.borrow_mut().stats.cursor_count += 1;

        // Allocate and return a cursor.
        Rc::new(RefCell::new(Cursor::new(&self.weak_self.upgrade().unwrap())))
    }

    // Bucket retrieves a nested bucket by name.
    // Returns nil if the bucket does not exist.
    // The bucket instance is only valid for the lifetime of the transaction.
    pub fn bucket(&mut self, name: &'static str) -> Option<Rc<RefCell<Bucket<'a>>>> {
        match self.buckets.get(name) {
            Some(ref b) => return Some(Rc::clone(b)),
            None => (),
        }

        // Move cursor to key
        let c = self.cursor();
        let (k, v, flags) = c.borrow().seek1(name);

        // Return nil if the key doesn't exist or it is not a bucket.
        match k {
            Some(key) => {
                if key != name {
                    return None
                }
            },
            None => return None,
        }
        if (flags & BUCKET_LEAF_FLAG as u32) == 0 {
            return None
        }

        match v {
            Some(value) => {
                match self.open_bucket(value) {
                    Some(ref child) => {
                        self.buckets.insert(name, Rc::clone(child));
                        return Some(Rc::clone(child))
                    },
                    None => panic!("failed to create bucket"),
                }
            },
            None => None,
        }
    }

    // Helper method that re-interprets a sub-subcket value
    // from a parent into a bucket.
    fn open_bucket(&mut self, value: &'a str) -> Option<Rc<RefCell<Bucket<'a>>>> {
        unimplemented!();
    }

    // creates a new bucket at the given key and returns the new bucket.
    // Returns an error if the key already exists, if the bucket name is blank, or if
    // the bucket name is too long.
    // The bucket instances is only valid for the lifetime of the transaction.
    pub fn create_bucket(&mut self, key: &'static str) -> Result<Rc<RefCell<Bucket>>, &'static str> {
        unimplemented!();
    }

    // creates a new bucket if it doesn't already exists and returns a reference to it.
    // Returns an error if the bucket name is blank, or if the bucket name is too long.
    // The bucket instance is only valid for the lifetime of the transaction.
    pub fn create_bucket_if_not_exists(&mut self, key: &'static str) -> Result<Rc<RefCell<Bucket>>, &'static str> {
        unimplemented!();
    }

    // deletes a bucket at the given kehy.
    // Returns an error if the bucket does not exists, or if the key represents a non-bucket value.
    pub fn delete_bucket(&mut self, key: &'static str) -> Result<(), &'static str> {
        unimplemented!();
    }

    // returns the value for a key in the bucket.
    // Returns a nil value if the key does not exist or if the key is a nested bucket.
    // The returned value is only valid for the life of the transaction.
    pub fn get(&self, key: &'static str) -> Option<&'static str> {
        unimplemented!();
    }

    // Put sets the value for a key in the bucket.
    // If the key exist then its previous value will be overwritten.
    // Supplied value must remain valid for the life of the transaction.
    // Returns an error if the bucket was created from a read-only transaction, if the key is blank,
    // if the key is too large, or if the value is too large.
    pub fn put(&mut self, key: &'static str, value: Option<&'static str>) -> Result<(), &'static str> {
        unimplemented!();
    }

    // Delete removes a key from the bucket.
    // If the key dose not exist then nothing is done and a nil error is returned.
    // Returns an error if the bucket was created from a read-only transaction.
    pub fn delete(&mut self, key: &'static str) -> Result<(), &'static str> {
        unimplemented!();
    }

    // sequence returns the current integer for the bucket without incrementing it.
    pub fn sequence(&self) -> u64 {
        unimplemented!();
    }

    // updates the sequence number for the bucket.
    pub fn set_sequence(&mut self, v: u64) -> Result<(), &'static str> {
        unimplemented!();
    }

    // returns an autoincrementing integer for the bucket
    pub fn next_sequence(&mut self) -> Result<u64, &'static str> {
        unimplemented!();
    }

    // executes a function for each key/value pair in a bucket.
    // If the provided function returns an error then the iteration is stopped and
    // the error is returned to the caller. The provided function must not modify
    // the bucket; this will result in undefined behaviour.
    pub fn for_each<F>(&self, f: F) -> Result<(), &'static str>
    where F: Fn(&'static str, Option<&'static str>) -> Result<(), &'static str> {
        unimplemented!();
    }

    pub fn stats(&self) ->  BucketStats {
        unimplemented!();
    }

    // forEachPage iterates over every page in a bucket, including inline pages.
    pub fn forEachPage<F>(&self, f: F) -> (Option<Rc<RefCell<Page>>>, i64)
    where F: Fn(Option<Rc<RefCell<Page>>>, Option<Rc<RefCell<Node>>>, i64) {
        unimplemented!();
    }

    // spill writes all the nodes for this bucket to dirty pages.
    fn spill(&mut self) -> Result<(), &'static str> {
        unimplemented!();
    }

    // returns true if a bucket is small enough to be written inline and if it contains no subbuckets.
    // Otherwise returns false.
    fn inlineable(&self) -> bool {
        unimplemented!();
    }

    // returns the maximum total size of a bucket to make it a candidate for inlining.
    fn max_inline_bucket_size(&self) -> i64 {
        unimplemented!();
    }

    // write allocates and writes a bucket to a byte slice.
    fn write(&self) -> &'static str  {
        unimplemented!();
    }

    // attempts to balance all nodes.
    fn rebalance(&mut self) {
        unimplemented!();
    }

    // free recursively frees all pages in the bucket.
    fn free(&mut self) {
        unimplemented!();
    }

    // dereference removes all references to the old mmap.
    fn dereference(&mut self) {
        unimplemented!();
    }

    // page_node returns the in-memory node, if it exists.
    // Otherwise returns the underlying page.
    fn page_node(&mut self, id: pgid_t) -> (Rc<RefCell<Page>>, Rc<RefCell<Node>>) {
        unimplemented!();
    }
}

// _Bucket represents the on-file representation of a bucket.
// This is stored as the "value" of a bucket key. If the _Bucket is small enough,
// then its root page can be stored inline in the "value", after the _Bucket
// header. In the case of inline buckets, the "root" will be 0.
pub struct _Bucket {
    pub root: pgid_t,  // page id of the _Bucket's root-level page
    pub sequence: u64, // monotonically incrementing, used by NextSequence()
}

impl _Bucket {
    pub fn new() -> _Bucket {
        _Bucket {
            root: 0,
            sequence: 0,
        }
    }
}

pub struct BucketStats {
    // Page count statistics.
    pub branch_page_n: i64,       // number of logical branch pages
    pub branch_overflow_n: i64,   // number of physical branch overflow pages
    pub leaf_page_n: i64,         // number of logical leaf pages
    pub leaf_overflow_n: i64,     // number of physical leaf overflow pages

    // Tree statistics.
    pub key_n: i64,               // number of keys/value pairs
    pub depth: i64,               // number of levels in B+tree

    // Page size utilization.
    pub branch_alloc: i64,        // bytes allocated for physical branch pages
    pub branch_inuse: i64,        // bytes actually used for branch data
    pub leaf_alloc:   i64,        // bytes allocated for physical leaf pages
    pub leaf_inuse:   i64,        // bytes actually used for leaf data

    // Bucket statistics
    pub bucket_n:     i64,        // total number of buckets including the top bucket
    pub inline_bucket_n: i64,     // total number on inlined buckets
    pub inline_bucket_inuse: i64, // bytes used for inlined buckets (also accounted for in LeafInuse)
}
