use types::pgid_t;
use tx::Tx;
use node::Node;
use std::sync::Arc;
use std::collections::HashMap;

// Bucket represents a collection of key/value pairs inside the datasbase.
pub struct Bucket<'a> {
    pub bucket: Box<_Bucket>,
    pub tx: Arc<Tx>, // the associated transcation
    buckets: HashMap<&'static str, Bucket<'a>>, // subbucket cache
    // page: &'b Page, // inline page reference
    root_node: Option<Arc<Node<'a>>>, // materialized node for the root page.
    nodes: HashMap<pgid_t, Node<'a>>, // node cache

    // Sets the threshold for filling nodes when they split. By default,
    // the bucket will fill to 50% but it can be useful to increase this
    // amount if you know that your write workloads are mostly append-only.
    //
    // This is non-persisted across transactions so it must be set in every Tx.
    fill_percent: f64,
}

impl<'a> Bucket<'a> {
    pub fn new(b: Box<_Bucket>, tx: Arc<Tx>, root: Option<Arc<Node<'a>>>) -> Bucket<'a> {
        Bucket {
            bucket: b,
            tx: tx,
            buckets: HashMap::new(),
            root_node: root,
            nodes: HashMap::new(),
            fill_percent: 0.0,
        }
    }
}

// _Bucket represents the on-file representation of a bucket.
// This is stored as the "value" of a bucket key. If the _Bucket is small enough,
// then its root page can be stored inline in the "value", after the _Bucket
// header. In the case of inline buckets, the "root" will be 0.
pub struct _Bucket {
    pub root: pgid_t, // page id of the _Bucket's root-level page
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
