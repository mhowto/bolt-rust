use bucket::Bucket;
use types::pgid_t;
use std::rc::{Weak, Rc};
use std::cell::RefCell;
use page;

// Node represents an in-memory, deserialized page.
pub struct Node<'a> {
    pub bucket: Rc<Bucket<'a>>,
    pub is_leaf: bool,
    pub unbalanced: bool,
    pub spilled: bool,
    pub key: &'a [u8],
    pub pgid: pgid_t,
    pub parent: Option<Rc<Node<'a>>>,
    pub inodes: Vec<INode<'a>>,
    children: RefCell<Vec<Weak<Node<'a>>>>,
}

impl<'a> Node<'a> {
    pub fn new(b: Rc<Bucket<'a>>) -> Node<'a> {
        Node {
            bucket: b,
            is_leaf: false,
            unbalanced: false,
            spilled: false,
            key: "".as_bytes(),
            pgid: 0,
            parent: None,
            children: RefCell::new(vec![]),
            inodes: Vec::new(),
        }
    }

    pub fn root(&self) -> Rc<Node<'a>> {
        if let Some(ref p) = self.parent {
            p.root()
        } else {
            unsafe {Rc::from_raw(self as *const Node)}
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
    pub fn size(&self) -> usize {
        let mut sz: usize = 0;
        unsafe {
            sz = page::PAGE_HEADER_SIZE;
        }
        let elsz = self.page_element_size();
        for inode in &self.inodes {
            sz += elsz + inode.key.len() + inode.value.len();
        }
        sz
    }

    // returns true if the node is less than a given size.
    // This is an optimization to avoid calculating a large node when we only need
    // to know if it fits inside a certain page size.
    fn size_less_than(&self, v: usize) -> bool {
        let mut sz: usize = 0;
        unsafe {
            sz = page::PAGE_HEADER_SIZE;
        }
        let elsz = self.page_element_size();
        for inode in &self.inodes {
            sz += elsz + inode.key.len() + inode.value.len();
            if sz >= v {
                return false
            }
        }
        true
    }

    fn page_element_size(&self) -> usize {
        if self.is_leaf {
            page::LEAF_PAGE_ELEMENT_SIZE
        } else {
            page::BRANCH_PAGE_ELEMENT_SIZE
        }
    }

    pub fn append_child(&mut self, child: &Rc<Node<'a>>) {
        let mut children = self.children.borrow_mut();
        children.push(Rc::downgrade(child));
    }

    pub fn set_parent(&mut self, p: &Rc<Node<'a>>) {
        self.parent = Some(Rc::clone(p));
    }

/*
    pub fn child_at(&self, index: isize) -> &Node {
        if self.is_leaf {
            panic!("invalid child_at{} on a leaf node", index);
        }
        self.bucket.bucket.
    }
    */

    pub fn put(&mut self,
               old_key: &[u8],
               new_key: &'a [u8],
               value: &'a [u8],
               pgid: pgid_t,
               flags: u32) {
        let meta_pgid = self.bucket.as_ref().tx.meta.pgid;
        if pgid > meta_pgid {
            panic!("pgid {} above high water mark {}",
                   pgid,
                   meta_pgid)
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
#[repr(C,packed)]
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
