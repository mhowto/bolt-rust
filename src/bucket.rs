use types::pgid_t;
use tx::Tx;
use node::Node;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use page::Page;

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
    pub tx: Rc<RefCell<Tx>>,                                   // the associated transcation
    buckets: HashMap<&'static str, Bucket<'a>>,        // subbucket cache
    page: Option<Rc<RefCell<Page>>>,                            // inline page reference
    pub root_node: Option<Rc<RefCell<Node<'a>>>>,      // materialized node for the root page.
    pub nodes: HashMap<pgid_t, Rc<RefCell<Node<'a>>>>, // node cache

    // Sets the threshold for filling nodes when they split. By default,
    // the bucket will fill to 50% but it can be useful to increase this
    // amount if you know that your write workloads are mostly append-only.
    //
    // This is non-persisted across transactions so it must be set in every Tx.
    pub fill_percent: f32,
}

impl<'a> Bucket<'a> {
    pub fn new(b: Box<_Bucket>, tx: Rc<RefCell<Tx>>) -> Bucket<'a> {
        Bucket {
            bucket: b,
            tx: tx,
            buckets: HashMap::new(),
            page: None,
            root_node: None,
            nodes: HashMap::new(),
            fill_percent: 0.0,
        }
    }

    // node creates a node from a page and associates it with a given parent.
    pub fn node(
        &mut self,
        pgid: pgid_t,
        parent: Option<Rc<RefCell<Node<'a>>>>,
        bucket: &Rc<RefCell<Bucket<'a>>>,
    ) -> Rc<RefCell<Node<'a>>> {
        // Retrieve node if it's already been created.
        if let Some(n) = self.nodes.get(&pgid) {
            return Rc::clone(&n);
        }

        // Otherwise create a node and cache it.
        let n = Rc::new(RefCell::new(Node::new(Rc::clone(bucket))));

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
        unimplemented!();

        n
    }

    pub fn tx(&self) -> Rc<RefCell<Tx>> {
        Rc::clone(&self.tx)
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
