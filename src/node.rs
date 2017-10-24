use bucket::Bucket;
use types::pgid_t;
use std::rc::{Rc, Weak};
use std::cell::RefCell;
use page;
use std::slice;
use std::ptr;
use std::str;

// Node represents an in-memory, deserialized page.
pub struct Node<'a> {
    pub bucket: Rc<RefCell<Bucket<'a>>>,
    pub is_leaf: bool,
    pub unbalanced: bool,
    pub spilled: bool,
    pub key: &'a str,
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
            key: "",
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

    fn find_key_index(&self, key: &'a str) -> usize {
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
        old_key: &'a str,
        new_key: &'a str,
        value: &'a str,
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

    pub fn del(&mut self, key: &'a str) {
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
                        key:  str::from_utf8((*elem).key()).unwrap(),
                        value: str::from_utf8((*elem).value()).unwrap(),
                    });
                }
            } else {
                let elem = p.branch_page_element(i);
                unsafe {
                    self.inodes.push(INode {
                        flags: 0,
                        pgid: (*elem).pgid,
                        key: str::from_utf8((*elem).key()).unwrap(),
                        value: "",
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
            self.key = "";
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
        p.count = self.inodes.len() as u16;

        // Stop here if there are no items to write
        if p.count == 0 {
            return
        }

        // Loop over each item and write it to the page.
        let mut b: *mut u8 = unsafe{ &mut p.ptr as *mut usize as *mut u8};
        unsafe {
            b = b.offset(self.inodes.len() as isize * self.page_element_size() as isize);
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
                    assert_ne!((*elem).pgid, p.id, "write: circular dependency occurred");
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
    pub key: &'a str,
    pub value: &'a str,
}

impl<'a> INode<'a> {
    pub fn new() -> INode<'a> {
        INode {
            flags: 0,
            pgid: 0,
            key: "",
            value: "",
        }
    }
}

#[cfg(test)]
mod tests {
    use node::Node;
    use std::rc::Rc;
    use std::cell::RefCell;
    use bucket::{Bucket, _Bucket};
    use tx::Tx;
    use db::Meta;
    use std::str;
    use page;
    use std::ptr;

    #[test]
    fn node_put() {
        let bucket: Rc<RefCell<Bucket>> = Rc::new(RefCell::new(Bucket::new(
            Box::new(_Bucket {
                root: 0,
                sequence: 0,
            }),
            Box::new(Tx { meta: Meta::new() }),
        )));
        let mut node = Node::new(Rc::clone(&bucket));
        node.put("baz", "baz", "2", 0, 0);
        node.put("foo", "foo", "0", 0, 0);
        node.put("bar", "bar", "1", 0, 0);
        node.put("foo", "foo", "3", 0, 0x02);

        assert_eq!(node.inodes.len(), 3);
        page::initialize();
        assert_eq!(node.size(), 16 + 3 * (16 + 4));

        {
            let inode = &node.inodes[0];
            assert_eq!(inode.key, "bar");
            assert_eq!(inode.value, "1");
        }

        {
            let inode = &node.inodes[1];
            assert_eq!(inode.key, "baz");
            assert_eq!(inode.value, "2");
        }

        {
            let inode = &node.inodes[2];
            assert_eq!(inode.key, "foo");
            assert_eq!(inode.value, "3");
        }

        {
            assert_eq!(node.inodes[2].flags, 0x02);
        }
    }

    #[test]
    fn node_read_leaf_page() {
        page::initialize();
        // Create a page
        let mut buf: [u8; 4096] = [0; 4096];
        let mut page: *mut page::Page = buf.as_mut_ptr() as *mut page::Page;
        unsafe {
            (*page).flags = page::LEAF_PAGE_FLAG;
            (*page).count = 2;
        }

        // Insert 2 elements at the beginning.
        let nodes_start_ptr: *mut page::LeafPageElement = unsafe { &mut (*page).ptr as *mut usize as *mut page::LeafPageElement };
        unsafe {
            *nodes_start_ptr = page::LeafPageElement {
                flags: 0,
                //                pos: 32,
                pos: page::LEAF_PAGE_ELEMENT_SIZE as u32 * 2,
                ksize: 3,
                vsize: 4,
            };
            *nodes_start_ptr.offset(1) = page::LeafPageElement {
                flags: 0,
                //pos: 23,
                pos: page::LEAF_PAGE_ELEMENT_SIZE as u32 + 3 + 4,
                ksize: 10,
                vsize: 3,
            };
        }

        // Write data for the nodes at the end.
        let data_ptr: *mut u8 = unsafe { nodes_start_ptr.offset(2) as *mut u8 };
        unsafe {
            let key = "barfooz";
            ptr::copy(key.as_ptr(), data_ptr, key.len());
            let value = "helloworldbye";
            ptr::copy(value.as_ptr(), data_ptr.offset(key.len() as isize), value.len());
        }

        // Deserialize page into a leaf.
        let mut n = Node::new(Rc::new(RefCell::new(Bucket::new(
            Box::new(_Bucket {
                root: 0,
                sequence: 0,
            }),
            Box::new(Tx { meta: Meta::new() }),
        ))));
        unsafe { n.read(page.as_mut().unwrap()); }

        // Check that there are two inodes with correct data.
        assert!(n.is_leaf, "expected leaf");
        assert_eq!(n.inodes.len(), 2);
        assert_eq!(n.inodes[0].key, "bar");
        assert_eq!(n.inodes[0].value, "fooz");
        assert_eq!(n.inodes[1].key, "helloworld");
        assert_eq!(n.inodes[1].value, "bye");
    }

    #[test]
    fn node_write_leaf_page() {
        let mut tx = Tx{meta: Meta::new()};
        tx.meta.pgid = 1;

        let bucket = Bucket::new(
            Box::new(_Bucket{
                root: 0,
                sequence: 0,
            }),
            Box::new(tx),
        );

        let mut n = Node::new(Rc::new(RefCell::new(bucket)));
        n.is_leaf = true;
        n.put("susy", "susy", "que", 0, 0);
        n.put("ricki", "ricki", "lake", 0, 0);
        n.put("john", "john", "johnson", 0, 0);

        // write it to a page
        let mut buf: [u8; 4096] = [0; 4096];
        let mut page: *mut page::Page = buf.as_mut_ptr() as *mut page::Page;

        unsafe { n.write(page.as_mut().unwrap()); };

        // Read the page back in
        let mut n2 = Node::new(Rc::new(RefCell::new(Bucket::new(
            Box::new(_Bucket {
                root: 0,
                sequence: 0,
            }),
            Box::new(Tx { meta: Meta::new() }),
        ))));
        unsafe { n2.read(page.as_mut().unwrap()); }

        // Check that the two pages are the same.
        assert_eq!(n2.inodes.len(), 3);

        assert_eq!(n2.inodes[0].key, "john");
        assert_eq!(n2.inodes[0].value, "johnson");

        assert_eq!(n2.inodes[1].key, "ricki");
        assert_eq!(n2.inodes[1].value, "lake");

        assert_eq!(n2.inodes[2].key, "susy");
        assert_eq!(n2.inodes[2].value, "que");
    }
}
