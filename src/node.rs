use bucket::Bucket;
use types::pgid_t;
use std::sync::Arc;

// Node represents an in-memory, deserialized page.
pub struct Node<'a> {
    pub bucket: Arc<Bucket<'a>>,
    pub is_leaf: bool,
    pub unbalanced: bool,
    pub spilled: bool,
    pub key: &'a [u8],
    pub pgid: pgid_t,
    parent: Option<Arc<Node<'a>>>,
    children: Vec<Node<'a>>,
    pub inodes: Vec<INode<'a>>,
}

impl<'a> Node<'a> {
    pub fn new(b: Arc<Bucket<'a>>) -> Node<'a> {
        Node {
            bucket: b,
            is_leaf: false,
            unbalanced: false,
            spilled: false,
            key: "".as_bytes(),
            pgid: 0,
            parent: None,
            children: Vec::new(),
            inodes: Vec::new(),
        }
    }

    pub fn root(&self) -> &Node {
        match self.parent {
            Some(ref p) => p.root(),
            None => self,
        }
    }

    pub fn min_keys(self) -> i32 {
        if self.is_leaf {
            1
        } else {
            2
        }
    }

    // size returns the size of the node after serialization
    /*
    pub fn size() ->isize {

    }
    */

    pub fn put(&mut self,
               old_key: &[u8],
               new_key: &'a [u8],
               value: &'a [u8],
               pgid: pgid_t,
               flags: u32) {
        if pgid > self.bucket.tx.meta.pgid {
            panic!("pgid {} above high water mark {}",
                   pgid,
                   self.bucket.tx.meta.pgid)
        } else if old_key.len() <= 0 {
            panic!("put: zero-length old key")
        } else if new_key.len() <= 0 {
            panic!("put: zero-length new key")
        }

        // Find insertion index
        let r = self.inodes.binary_search_by(|ref inode| inode.key.cmp(old_key));

        // Add capacity and shift nodes if we don't have an exact match and need to insert.
        let index = match r {
            Err(idx) => {
                self.inodes.insert(idx, INode::new());
                idx
            }
            Ok(idx) => idx,
        };

        let inode = &mut self.inodes[index];
        inode.flags = flags;
        inode.key = new_key;
        inode.value = value;
        inode.pgid = pgid;
        assert!(inode.key.len() > 0, "put: zero-length inode key");
    }
}

// INode represents an internal node inside of a node.
// It can be used to point to elements in a page or point
// to an element which hasn't been added to a page yet.
pub struct INode<'a> {
    pub flags: u32,
    pub pgid: pgid_t,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

impl<'a> INode<'a> {
    pub fn new() -> INode<'a> {
        INode {
            flags: 0,
            pgid: 0,
            key: "".as_bytes(),
            value: "".as_bytes(),
        }
    }
}
