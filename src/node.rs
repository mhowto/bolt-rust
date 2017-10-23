use bucket::Bucket;
use types::pgid_t;
use std::rc::{Rc, Weak};
use std::cell::RefCell;
use page;
use std::slice;
use std::ptr;

// Node represents an in-memory, deserialized page.
pub struct Node<'a> {
    pub bucket: Rc<RefCell<Bucket<'a>>>,
    pub is_leaf: bool,
    pub unbalanced: bool,
    pub spilled: bool,
    pub key: &'a [u8],
    pub pgid: pgid_t,
    pub parent: Option<Rc<RefCell<Node<'a>>>>,
    pub inodes: Vec<INode<'a>>,
    children: RefCell<Vec<Weak<RefCell<Node<'a>>>>>,
}

impl<'a> Node<'a> {
    pub fn new(b: Rc<RefCell<Bucket<'a>>>) -> Node<'a> {
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

    fn to_rc_refcell_node(&self) -> Rc<RefCell<Node<'a>>> {
        Rc::clone(self.bucket.borrow().nodes.get(&(self.pgid)).unwrap())
    }

    pub fn root(&self) -> Rc<RefCell<Node<'a>>> {
        if let Some(ref p) = self.parent {
            p.borrow().root()
        } else {
            self.to_rc_refcell_node()
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
                return false;
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

    pub fn append_child(&mut self, child: &Rc<RefCell<Node<'a>>>) {
        let mut children = self.children.borrow_mut();
        children.push(Rc::downgrade(child));
    }

    pub fn set_parent(&mut self, p: &Rc<RefCell<Node<'a>>>) {
        self.parent = Some(Rc::clone(p));
    }

    pub fn child_at(&self, index: usize) -> Rc<RefCell<Node<'a>>> {
        if self.is_leaf {
            panic!("invalid child_at{} on a leaf node", index);
        }
        self.bucket.borrow_mut().node(
            self.inodes[index].pgid,
            Some(self.to_rc_refcell_node()),
            &self.bucket,
        )
    }

    fn find_key_index(&self, key: &'a [u8]) -> usize {
        let r = self.inodes.binary_search_by(|ref inode| inode.key.cmp(key));
        match r {
            Ok(idx) => idx,
            Err(idx) => idx,
        }
    }

    // returns the index of a given child node.
    pub fn child_index(&self, child: &Node<'a>) -> usize {
        self.find_key_index(child.key)
    }

    pub fn num_children(&self) -> usize {
        self.inodes.len()
    }

    pub fn next_sibling(&self) -> Option<Rc<RefCell<Node<'a>>>> {
        if self.parent.is_none() {
            return None
        }
        let parent_refcell = self.parent.as_ref().unwrap();
        let parent = parent_refcell.borrow();
        let index = parent.child_index(self);
        if index >= parent.num_children() -1 {
            return None
        }
        Some(parent.child_at(index + 1))
    }

    pub fn prev_sibling(&self) -> Option<Rc<RefCell<Node<'a>>>> {
        if self.parent.is_none() {
            return None
        }
        let parent_refcell = self.parent.as_ref().unwrap();
        let parent = parent_refcell.borrow();
        let index = parent.child_index(self);
        if index == 0 {
            return None
        }
        Some(parent.child_at(index - 1))
    }

    pub fn put(
        &mut self,
        old_key: &[u8],
        new_key: &'a [u8],
        value: &'a [u8],
        pgid: pgid_t,
        flags: u32,
    ) {
        let meta_pgid = self.bucket.borrow().tx.meta.pgid;
        if pgid > meta_pgid {
            panic!("pgid {} above high water mark {}", pgid, meta_pgid)
        } else if old_key.len() <= 0 {
            panic!("put: zero-length old key")
        } else if new_key.len() <= 0 {
            panic!("put: zero-length new key")
        }

        // Find insertion index
        let r = self.inodes
            .binary_search_by(|ref inode| inode.key.cmp(old_key));

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

    pub fn del(&mut self, key: &'a [u8]) {
        let r = self.inodes.binary_search_by(|ref inode| inode.key.cmp(key));
        // Exit if the key isn't found.
        if r.is_err() {
            return
        }

        // Delete inode from the node
        self.inodes.remove(r.unwrap());

        // Mark the node as needing rebalancing.
        self.unbalanced = true;
    }

    pub fn read(&mut self, p: &page::Page) {
        self.pgid = p.id;
        self.is_leaf = (p.flags & page::LEAF_PAGE_FLAG) != 0;
        self.inodes = Vec::with_capacity(p.count as usize);

        for i in 0..p.count {
            if self.is_leaf {
                let elem = p.leaf_page_element(i);
                unsafe {
                    self.inodes.push(INode {
                        flags: (*elem).flags,
                        pgid: 0,
                        key: (*elem).key(),
                        value: (*elem).value(),
                    });
                }
            } else {
                let elem = p.branch_page_element(i);
                unsafe {
                    self.inodes.push(INode {
                        flags: 0,
                        pgid: (*elem).pgid,
                        key: (*elem).key(),
                        value: "".as_bytes(),
                    })
                }
            }
            assert!(self.inodes[i as usize].key.len() > 0, "read: zero-length inode key");
        }

        // Save first key so we can find the node in the parent when we spill.
        if self.inodes.len() > 0 {
            self.key = self.inodes[0].key;
            assert!(self.key.len() > 0, "read: zero-length node key")
        } else {
            self.key = "".as_bytes();
        }
    }

    pub fn write(&mut self, p: &mut page::Page) {
        // Initialize page
        if self.is_leaf {
            p.flags |= page::LEAF_PAGE_FLAG;
        } else {
            p.flags |= page::BRANCH_PAGE_FLAG;
        }

        if self.inodes.len() >= 0xFFFF {
            panic!("inode overflow: {} (pgid={})", self.inodes.len(), p.id);
        }

        // Stop here if there are no items to write
        if p.count == 0 {
            return
        }

        // Loop over each item and write it to the page.
        let mut b: *mut u8 = 0 as *mut u8;
        unsafe {
            let ptr = p as *mut page::Page as *mut u8;
            b = ptr.offset(p.ptr as isize +
                self.inodes.len() as isize * self.page_element_size() as isize);
        }

        for (i, item) in self.inodes.iter().enumerate() {
            assert!(item.key.len() > 0, "write: zero-length inode key");

            // Write the page element
            if self.is_leaf {
                let elem = p.leaf_page_element(i as u16)
                    as *mut page::LeafPageElement;
                unsafe {
                    (*elem).pos = b as u32 - elem as u32;
                    (*elem).flags = item.flags;
                    (*elem).ksize = item.key.len() as u32;
                    (*elem).vsize = item.value.len() as u32;
                }
            } else {
                let elem = p.branch_page_element(i as u16)
                    as *mut page::BranchPageElement;
                unsafe {
                    (*elem).pos = b as u32 - elem as u32;
                    (*elem).ksize = item.key.len() as u32;
                    (*elem).pgid = item.pgid;
                    assert!((*elem).pgid != p.id, "write: circular dependency occurred")
                }
            }

            // If the length of key+value is larger than the max allocation size
            // then we need to reallocate the byte array pointer.
            //
            // See: https://github.com/boltdb/bolt/pull/335
            let klen = item.key.len();
            let vlen = item.value.len();

            // Write data for the element to the end of the page.
            unsafe {
                ptr::copy(item.key.as_ptr(), b, klen);
                b = b.offset(klen as isize);
                ptr::copy(item.value.as_ptr(), b, vlen);
                b = b.offset(vlen as isize);
            }


        }
        // DEBUG ONLY: n.dump()
    }
}

// INode represents an internal node inside of a node.
// It can be used to point to elements in a page or point
// to an element which hasn't been added to a page yet.
#[repr(C, packed)]
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
