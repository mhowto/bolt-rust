use bucket::Bucket;
use types::pgid_t;
use std::sync::Arc;

// Node represents an in-memory, deserialized page.
pub struct Node<'a> {
    pub bucket: Arc<Bucket<'a>>,
    pub is_leaf: bool,
    pub unbalanced: bool,
    pub spilled: bool,
    pub key: &'a[u8],
    pub pgid: pgid_t,
    parent: Option<Arc<Node<'a>>>,
    children: Vec<Arc<Node<'a>>>,
    inodes: Vec<Arc<INode<'a>>>,
}

impl <'a> Node<'a> {
    pub fn new(b: Arc<Bucket<'a>>) -> Node<'a> {
        Node{
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

    pub fn put(self, old_key: &[u8], new_key: &[u8], value: &[u8], pgid: pgid_t, flags: u32) {
        if pgid > self.bucket.tx.meta.pgid {
            panic!("pgid {} above high water mark {}", pgid, self.bucket.tx.meta.pgid)
        } else if old_key.len() <= 0 {
            panic!("put: zero-length old key")
        } else if new_key.len() <= 0 {
            panic!("put: zero-length new key")
        }

        // Find insertion index
        let r = self.inodes.binary_search_by(|&inode_rc| {
            let inode_r = Arc::try_unwrap(inode_rc);
            match inode_r {
                Ok(inode) => inode.key.cmp(old_key),
                Err(err) => panic!("inode rc empty")
            }
        });

        match r {
            Ok(index) => { self.inodes.insert(index, Arc::new(INode::new())); },
            Err(index) =>  {
                // Add capacity and shift nodes if we don't have an exact match and need to insert.
                let inode = Arc::get_mut(&mut self.inodes[index]).unwrap();
                inode.flags = flags;
                inode.key = new_key;
                inode.value = value;
                inode.pgid = pgid;
                assert!(inode.key.len() > 0, "put: zero-length inode key");
            }
        }
    }
}

// INode represents an internal node inside of a node.
// It can be used to point to elements in a page or point
// to an element which hasn't been added to a page yet.
struct INode<'a> {
    flags: u32,
    pgid: pgid_t,
    key: &'a[u8],
    value: &'a[u8],
}

impl <'a> INode<'a> {
    pub fn new() -> INode<'a> {
        INode {
            flags: 0,
            pgid: 0,
            key: "".as_bytes(),
            value: "".as_bytes(),
        }
    }
}