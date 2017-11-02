use bucket;
use bucket::Bucket;
use types::pgid_t;
use std::rc::{Rc, Weak};
use std::cell::RefCell;
use page;
use std::ptr;
use std::str;
use tx::Tx;

// Node represents an in-memory, deserialized page.
pub struct Node<'a> {
    pub bucket: Rc<RefCell<Bucket<'a>>>,
    pub is_leaf: bool,
    pub unbalanced: bool,
    pub spilled: bool,
    pub key: &'a str,
    pub pgid: pgid_t,
    pub parent: Option<Weak<RefCell<Node<'a>>>>,
    pub inodes: Vec<INode<'a>>,
    children: RefCell<Vec<Rc<RefCell<Node<'a>>>>>,
    pub weak_self: Weak<RefCell<Node<'a>>>, // pointer to self
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
            weak_self: Weak::new(),
        }
    }

    fn to_rc_refcell_node(&self) -> Rc<RefCell<Node<'a>>> {
//        Rc::clone(self.bucket.borrow().nodes.get(&(self.pgid)).unwrap())
        Rc::clone(&self.weak_self.upgrade().unwrap())
    }

    pub fn root(&self) -> Rc<RefCell<Node<'a>>> {
        if let Some(ref p) = self.parent {
            match p.upgrade() {
                None => self.to_rc_refcell_node(),
                Some(ref pp) => pp.borrow().root()
            }
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
        let mut sz: usize = page::get_page_header_size();
        let elsz = self.page_element_size();
        for inode in &self.inodes {
            sz += elsz + inode.key.len() + match inode.value {
                None => 0,
                Some(ref v) => v.len(),
            };
        }
        sz
    }

    // returns true if the node is less than a given size.
    // This is an optimization to avoid calculating a large node when we only need
    // to know if it fits inside a certain page size.
    fn size_less_than(&self, v: usize) -> bool {
        let mut sz: usize = page::get_page_header_size();
        let elsz = self.page_element_size();
        for inode in &self.inodes {
            sz += elsz + inode.key.len() + match inode.value {
                None => 0,
                Some(ref v) => v.len(),
            };
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
        children.push(Rc::clone(child));
    }

    pub fn set_parent(&mut self, p: Weak<RefCell<Node<'a>>>) {
        self.parent = Some(p);
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
        let parent_weak_refcell = self.parent.as_ref().unwrap();
        let parent_refcell = parent_weak_refcell.upgrade().unwrap();
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
        let parent_weak_refcell = self.parent.as_ref().unwrap();
        let parent_refcell = parent_weak_refcell.upgrade().unwrap();
        let parent = parent_refcell.borrow();
        let index = parent.child_index(self);
        if index == 0 {
            return None
        }
        Some(parent.child_at(index - 1))
    }

    /*
    pub fn put(
        &mut self,
        old_key: &'a str,
        new_key: &'a str,
        value: &'a str,
        pgid: pgid_t,
        flags: u32,
    ) {
        let bucket_borrow = self.bucket.borrow();
        let tx_borrow = bucket_borrow.tx.borrow();
        let meta_pgid = tx_borrow.meta.pgid;
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
    */

    pub fn put(
        &mut self,
        old_key: &'a str,
        new_key: &'a str,
        value: Option<&'a str>,
        pgid: pgid_t,
        flags: u32,
    ) {
        let bucket_borrow = self.bucket.borrow();
        let tx_borrow = bucket_borrow.tx.borrow();
        let meta_pgid = tx_borrow.meta.pgid;
        if pgid > meta_pgid {
            panic!("pgid {} above high water mark {}", pgid, meta_pgid)
        } else if old_key.len() == 0 {
            panic!("put: zero-length old key")
        } else if new_key.len() == 0 {
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
                        value: Some(str::from_utf8((*elem).value()).unwrap()),
                    });
                }
            } else {
                let elem = p.branch_page_element(i);
                unsafe {
                    self.inodes.push(INode {
                        flags: 0,
                        pgid: (*elem).pgid,
                        key: str::from_utf8((*elem).key()).unwrap(),
                        value: None,
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
        let mut b: *mut u8 = &mut p.ptr as *mut usize as *mut u8;
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
                    (*elem).vsize = match item.value {
                        None => 0,
                        Some(ref v) => v.len(),
                    } as u32;
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
            // Write data for the element to the end of the page.
            let klen = item.key.len();
            unsafe {
                ptr::copy(item.key.as_ptr(), b, klen);
                b = b.offset(klen as isize);
            }

            match item.value {
                None => (),
                Some(ref v) => {
                    let vlen = v.len();
                    unsafe {
                        ptr::copy(v.as_ptr(), b, vlen);
                        b = b.offset(vlen as isize);
                    }
                },
            };

        }
        // DEBUG ONLY: n.dump()
    }

    pub fn write_refcell(&mut self, p: Rc<RefCell<page::Page>>) {
        // Initialize page
        if self.is_leaf {
            p.borrow_mut().flags |= page::LEAF_PAGE_FLAG;
        } else {
            p.borrow_mut().flags |= page::BRANCH_PAGE_FLAG;
        }

        if self.inodes.len() >= 0xFFFF {
            panic!("inode overflow: {} (pgid={})", self.inodes.len(), p.borrow().id);
        }
        p.borrow_mut().count = self.inodes.len() as u16;

        // Stop here if there are no items to write
        if p.borrow().count == 0 {
            return
        }

        // Loop over each item and write it to the page.
        let mut b: *mut u8 = &mut p.borrow_mut().ptr as *mut usize as *mut u8;
        unsafe {
            b = b.offset(self.inodes.len() as isize * self.page_element_size() as isize);
        }

        for (i, item) in self.inodes.iter().enumerate() {
            assert!(item.key.len() > 0, "write: zero-length inode key");

            // Write the page element
            if self.is_leaf {
                let elem = p.borrow().leaf_page_element(i as u16)
                    as *mut page::LeafPageElement;
                unsafe {
                    (*elem).pos = b as u32 - elem as u32;
                    (*elem).flags = item.flags;
                    (*elem).ksize = item.key.len() as u32;
                    (*elem).vsize = match item.value {
                        None => 0,
                        Some(ref v) => v.len(),
                    } as u32;
                }
            } else {
                let elem = p.borrow().branch_page_element(i as u16)
                    as *mut page::BranchPageElement;
                unsafe {
                    (*elem).pos = b as u32 - elem as u32;
                    (*elem).ksize = item.key.len() as u32;
                    (*elem).pgid = item.pgid;
                    assert_ne!((*elem).pgid, p.borrow().id, "write: circular dependency occurred");
                }
            }

            // If the length of key+value is larger than the max allocation size
            // then we need to reallocate the byte array pointer.
            //
            // See: https://github.com/boltdb/bolt/pull/335
            // Write data for the element to the end of the page.
            let klen = item.key.len();
            unsafe {
                ptr::copy(item.key.as_ptr(), b, klen);
                b = b.offset(klen as isize);
            }
            match item.value {
                None => (),
                Some(ref v) => {
                    let vlen = v.len();
                    unsafe {
                        ptr::copy(v.as_ptr(), b, vlen);
                        b = b.offset(vlen as isize);
                    }
                },
            };
        }
        // DEBUG ONLY: n.dump()
    }

    // split breaks up a node into multiple smaller nodes, if appropriate.
    // This should only be called from the spill() function.
    fn split(&mut self, page_size: usize, new_parents: &mut Vec<Rc<RefCell<Node<'a>>>>) -> Vec<Rc<RefCell<Node<'a>>>> {
        let mut nodes: Vec<Rc<RefCell<Node<'a>>>> = vec![];

        let node_option = self.split_two(page_size, new_parents);
        if node_option.is_none() {
            return nodes;
        }
        let mut node  = node_option.unwrap();
        nodes.push(Rc::clone(&node));

        loop {
            // Split node into two.
            let second = node.borrow_mut().split_two(page_size, new_parents);

            // If we can't split then exit the loop.
            if second.is_none() {
                break;
            }

            // Set node to b so it gets split on the next iteration.
            node = second.unwrap();
            nodes.push(Rc::clone(&node));
        }

        nodes
    }

    // split_two breaks up a node into two smaller nodes, if appropriate.
    // This should only be called from the split() function.
    fn split_two(&mut self, page_size: usize, new_parents: &mut Vec<Rc<RefCell<Node<'a>>>>) -> Option<Rc<RefCell<Node<'a>>>> {
        // Ignore the split if the page doesn't have at least enough nodes for
        // two pages or if the nodes can fit in a single pages.
        if self.inodes.len() < page::MIN_KEYS_PER_PAGE as usize * 2 || self.size_less_than(page_size) {
            return None
        }

        // Determine the threshold before starting a new node
        let mut fill_percent = self.bucket.borrow().fill_percent;
        if fill_percent < bucket::MIN_FILL_PERCENT {
            fill_percent = bucket::MIN_FILL_PERCENT;
        } else if fill_percent > bucket::MAX_FILL_PERCENT {
            fill_percent = bucket::MAX_FILL_PERCENT;
        }
        let threshold = (fill_percent * page_size as f32) as usize;

        // Determine split position and sizes of the two pages.
        let (split_index, _) = self.split_index(threshold);

        // Split node into two separate nodes.
        // If there's no parent then we'll need to create one.
        if self.parent.is_none() {
            let node =  Rc::new(RefCell::new(Node::new(Rc::clone(&self.bucket))));
            let node_refmut = node.borrow_mut();
            let mut children = node_refmut.children.borrow_mut();
            children.push(Rc::clone(&self.to_rc_refcell_node()));
            self.parent = Some(Rc::downgrade(&node));
            new_parents.push(Rc::clone(&node));
        }

        let next = Rc::new(RefCell::new(Node {
            bucket: Rc::clone(&self.bucket),
            is_leaf: self.is_leaf,
            unbalanced: false,
            spilled: false,
            key: "",
            pgid: 0,
            parent: None,
            inodes: self.inodes.split_off(split_index), // Split inodes across two nodes.
            children: RefCell::new(Vec::new()),
            weak_self: Weak::new(),
        }));
        next.borrow_mut().weak_self = Rc::downgrade(&next);
        // Create a new node and add it to the parent.
        match self.parent {
            None => panic!("node should have parent"),
            Some(ref p) => {
                next.borrow_mut().parent = Some(Weak::clone(p));
                let p_refmut_strong = p.upgrade().unwrap();
                let p_refmut = p_refmut_strong.borrow_mut();
                let mut children_refmut = p_refmut.children.borrow_mut();
                children_refmut.push(Rc::clone(&next));
            },
        }

        // Update the statistics.
        let bucket_borrow = self.bucket.borrow_mut();
        let mut tx_borrow = bucket_borrow.tx.borrow_mut();
        tx_borrow.stats.split += 1;

        return Some(Rc::clone(&next))
    }

    // split_index finds the position where a page will fill a given threshold.
    // It returns the index as well as the size of the first page.
    // This is only be called from split().
    fn split_index(&self, threshold: usize) -> (usize, usize) {
        let mut sz = page::get_page_header_size();

        let mut index: usize = 0;
        // Loop until we only have the minimum number of keys required for the second page.
        for i in 0 .. self.inodes.len() - page::MIN_KEYS_PER_PAGE as usize{
            index = i;
            let inode = &self.inodes[i];
            let elsize = self.page_element_size() + inode.key.len() + match inode.value {
                None => 0,
                Some(ref v) => v.len(),
            };

            // If we have at least the minimum number of keys and adding another
            // node would put us over the threshold then exit and return.
            if i > page::MIN_KEYS_PER_PAGE as usize && sz + elsize > threshold {
                break;
            }

            // Add the element size to the total size.else
            sz += elsize;
        }
        (index, sz)
    }

    // spill writes the nodes to dirty pages and splits nodes as it goes.
    // Returns an error if dirty pages cannot be allocated.
    fn spill(&mut self) -> Result<Vec<Rc<RefCell<Node<'a>>>>, &'static str> {
        let mut new_parents = Vec::new();
        if self.spilled {
            return Ok(new_parents);
        }

        // Spill child nodes first. Child nodes can materialize sibling nodes in
        // the case of split-merge so we cannot use a range loop. We have to check
        // the children size on every loop iteration.
        {
            let mut children = self.children.borrow_mut();
            children.sort_by_key(|node_strong| {
                let node = node_strong.borrow();
                let key = node.inodes[0].key;
                key
            });
            for child in children.as_slice() {
                let mut cnode = child.borrow_mut();
                let result = cnode.spill();
                if result.is_err() {
                    return result;
                }
            }

            // We no longer need the child list because it's only used for spill tracking.
            children.clear();
        }

        let page_size = self.get_page_size();

        // Split nodes into appropriate sizes. The first node will always be self.
        let tx = self.get_tx();
        let nodes = self.split(page_size, &mut new_parents);
        for node in &nodes {
            // Add node's page to the freelist if it's not new.
            if node.borrow().pgid > 0 {
                tx.borrow_mut().free(node.borrow().pgid);
                node.borrow_mut().pgid = 0;
            }

            // Allocate contiguous space for the node.
            let result = tx.borrow_mut().allocate(self.size() / tx.borrow().get_page_size() + 1);
            match result {
                Err(s) => return Err(s),
                Ok(p) => {
                    // Write the code.
                    if p.borrow().id >= tx.borrow().meta.pgid {
                        panic!("pgid {} above high water mark {}", p.borrow().id, tx.borrow().meta.pgid);
                    }
                    self.pgid = p.borrow().id;
                    self.write_refcell(Rc::clone(&p));
                    self.spilled = true;

                    // Insert into parent inodes.
                    match self.parent {
                        None => (),
                        Some(ref p) => {
                            let mut key = self.key;
                            if key.len() == 0 {
                                key = self.inodes[0].key;
                            }

                            // 只有在spill的时候才会更新父节点的key
                            let p_strong = p.upgrade().unwrap();
                            p_strong.borrow_mut().put(key, self.inodes[0].key, None, self.pgid, 0);
                            self.key = self.inodes[0].key;
                            assert!(self.key.len() > 0, "spill: zero-length node key");
                        }
                    };
                },
            }

            // Update the statistics
            tx.borrow_mut().stats.spill += 1;
        }

        // If the root node split and created a new root then we need to spill that
        // as well. We'll clear out the children to make sure it doesn't try to respill.
        if let Some(ref p) = self.parent {
            let p_strong = p.upgrade().unwrap();
            if p_strong.borrow().pgid == 0 {
                self.children.borrow_mut().clear();
                return p_strong.borrow_mut().spill();
            }
        }

        Ok(new_parents)
    }

    fn get_page_size(&self) -> usize {
        let bucket_borrow = self.bucket.borrow();
        let tx_borrow = bucket_borrow.tx.borrow();
        let db_borrow = tx_borrow.db.borrow();
        db_borrow.page_size
    }

    fn get_tx(&self) -> Rc<RefCell<Tx>> {
        let bucket_borrow = self.bucket.borrow();
        Rc::clone(&bucket_borrow.tx)
    }

    // attempts to combine the node with sibling nodes if the node fill
    // size is below a threshold or if there are not enough keys.
    fn reblance(&mut self) {
        unimplemented!();
    }

    // remove a node from the list of in-memory children.
    // This does not affect the inodes.
    fn remove_child(&mut self, target: Rc<RefCell<Node<'a>>>) {
        unimplemented!();
    }

    // dereference causes the node to copy all its inode key/value references to heap memory.
    // This is required when the mmap is reallocated so inodes are not pointing to stale data.
    fn dereference(&mut self) {
        unimplemented!();
    }

    // free adds the node's underlying page to the freelist.
    fn free(&mut self) {
        unimplemented!();
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
    pub value: Option<&'a str>,
}

impl<'a> INode<'a> {
    pub fn new() -> INode<'a> {
        INode {
            flags: 0,
            pgid: 0,
            key: "",
            value: None,
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
    use std::str;
    use page;
    use std::ptr;
    use db::DB;

    #[test]
    fn node_put() {
        let db = Rc::new(RefCell::new(DB::new()));
        let bucket: Rc<RefCell<Bucket>> = Rc::new(RefCell::new(Bucket::new(
            Box::new(_Bucket {
                root: 0,
                sequence: 0,
            }),
            Rc::new(RefCell::new(Tx::new(&db))),
        )));
        let mut node = Node::new(Rc::clone(&bucket));
        node.put("baz", "baz", Some("2"), 0, 0);
        node.put("foo", "foo", Some("0"), 0, 0);
        node.put("bar", "bar", Some("1"), 0, 0);
        node.put("foo", "foo", Some("3"), 0, 0x02);

        assert_eq!(node.inodes.len(), 3);
        assert_eq!(node.size(), 16 + 3 * (16 + 4));

        {
            let inode = &node.inodes[0];
            assert_eq!(inode.key, "bar");
            assert_eq!(inode.value, Some("1"));
        }

        {
            let inode = &node.inodes[1];
            assert_eq!(inode.key, "baz");
            assert_eq!(inode.value, Some("2"));
        }

        {
            let inode = &node.inodes[2];
            assert_eq!(inode.key, "foo");
            assert_eq!(inode.value, Some("3"));
        }

        {
            assert_eq!(node.inodes[2].flags, 0x02);
        }
    }

    #[test]
    fn node_read_leaf_page() {
        // Create a page
        let mut buf: [u8; 4096] = [0; 4096];
        let page: *mut page::Page = buf.as_mut_ptr() as *mut page::Page;
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
        let db = Rc::new(RefCell::new(DB::new()));
        let mut n = Node::new(Rc::new(RefCell::new(Bucket::new(
            Box::new(_Bucket {
                root: 0,
                sequence: 0,
            }),
            Rc::new(RefCell::new(Tx::new(&db))),
        ))));
        unsafe { n.read(page.as_mut().unwrap()); }

        // Check that there are two inodes with correct data.
        assert!(n.is_leaf, "expected leaf");
        assert_eq!(n.inodes.len(), 2);
        assert_eq!(n.inodes[0].key, "bar");
        assert_eq!(n.inodes[0].value, Some("fooz"));
        assert_eq!(n.inodes[1].key, "helloworld");
        assert_eq!(n.inodes[1].value, Some("bye"));
    }

    #[test]
    fn node_write_leaf_page() {
        let db = Rc::new(RefCell::new(DB::new()));
        let mut tx = Tx::new(&db);
        tx.meta.pgid = 1;

        let bucket = Bucket::new(
            Box::new(_Bucket{
                root: 0,
                sequence: 0,
            }),
            Rc::new(RefCell::new(tx)),
        );

        let mut n = Node::new(Rc::new(RefCell::new(bucket)));
        n.is_leaf = true;
        n.put("susy", "susy", Some("que"), 0, 0);
        n.put("ricki", "ricki", Some("lake"), 0, 0);
        n.put("john", "john", Some("johnson"), 0, 0);

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
            Rc::new(RefCell::new(Tx::new(&db))),
        ))));
        unsafe { n2.read(page.as_mut().unwrap()); }

        // Check that the two pages are the same.
        assert_eq!(n2.inodes.len(), 3);

        assert_eq!(n2.inodes[0].key, "john");
        assert_eq!(n2.inodes[0].value, Some("johnson"));

        assert_eq!(n2.inodes[1].key, "ricki");
        assert_eq!(n2.inodes[1].value, Some("lake"));

        assert_eq!(n2.inodes[2].key, "susy");
        assert_eq!(n2.inodes[2].value, Some("que"));
    }

    // Ensure that a node can split into approriate subgroups.
    #[test]
    fn node_split() {
        // Create a node
        let db = Rc::new(RefCell::new(DB::new()));
        let mut tx = Tx::new(&db);
        tx.meta.pgid = 1;

        let bucket = Bucket::new(
            Box::new(_Bucket{
                root: 0,
                sequence: 0,
            }),
            Rc::new(RefCell::new(tx)),
        );

        let n = Rc::new(RefCell::new(Node::new(Rc::new(RefCell::new(bucket)))));
        n.borrow_mut().weak_self = Rc::downgrade(&n);
        n.borrow_mut().put("00000001", "00000001", Some("0123456701234567"), 0, 0);
        n.borrow_mut().put("00000002", "00000002", Some("0123456701234567"), 0, 0);
        n.borrow_mut().put("00000003", "00000003", Some("0123456701234567"), 0, 0);
        n.borrow_mut().put("00000004", "00000004", Some("0123456701234567"), 0, 0);
        n.borrow_mut().put("00000005", "00000005", Some("0123456701234567"), 0, 0);

        // Split between 2 & 3
        let mut new_parents = vec![];
        n.borrow_mut().split(100, &mut new_parents);

        let n_borrow = n.borrow();
        match n_borrow.parent {
            None => assert!(false),
            Some(ref p) => {
                let p_strong = p.upgrade().unwrap();
                let p_borrow = p_strong.borrow();
                let children_borrow = p_borrow.children.borrow();
                assert_eq!(children_borrow.len(), 2);


                {
                    let child1 = children_borrow[0].borrow();
                    assert_eq!(child1.inodes.len(), 2);
                    let child2 = children_borrow[1].borrow();
                    assert_eq!(child2.inodes.len(), 3);
                }
            }
        }
    }

    // Ensure that a page with the minimum number of inodes just returns a single node.
    #[test]
    fn node_split_min_keys() {
        // Create a node
        let db = Rc::new(RefCell::new(DB::new()));
        let mut tx = Tx::new(&db);
        tx.meta.pgid = 1;

        let bucket = Bucket::new(
            Box::new(_Bucket{
                root: 0,
                sequence: 0,
            }),
            Rc::new(RefCell::new(tx)),
        );

        let n = Rc::new(RefCell::new(Node::new(Rc::new(RefCell::new(bucket)))));
        n.borrow_mut().weak_self = Rc::downgrade(&n);
        n.borrow_mut().put("00000001", "00000001", Some("0123456701234567"), 0, 0);
        n.borrow_mut().put("00000002", "00000002", Some("0123456701234567"), 0, 0);

        // Split
        let mut new_parents = vec![];
        n.borrow_mut().split(20, &mut new_parents);

        let n_borrow = n.borrow();
        assert!(n_borrow.parent.is_none(), "expected nil parent");
    }

    #[test]
    fn node_split_single_page() {
        // Create a node
        let db = Rc::new(RefCell::new(DB::new()));
        let mut tx = Tx::new(&db);
        tx.meta.pgid = 1;

        let bucket = Bucket::new(
            Box::new(_Bucket{
                root: 0,
                sequence: 0,
            }),
            Rc::new(RefCell::new(tx)),
        );

        let n = Rc::new(RefCell::new(Node::new(Rc::new(RefCell::new(bucket)))));
        n.borrow_mut().weak_self = Rc::downgrade(&n);
        n.borrow_mut().put("00000001", "00000001", Some("0123456701234567"), 0, 0);
        n.borrow_mut().put("00000002", "00000002", Some("0123456701234567"), 0, 0);
        n.borrow_mut().put("00000003", "00000003", Some("0123456701234567"), 0, 0);
        n.borrow_mut().put("00000004", "00000004", Some("0123456701234567"), 0, 0);
        n.borrow_mut().put("00000005", "00000005", Some("0123456701234567"), 0, 0);

        // Split between 2 & 3
        let mut new_parents = vec![];
        n.borrow_mut().split(4096, &mut new_parents);

        let n_borrow = n.borrow();
        assert!(n_borrow.parent.is_none(), "expected nil parent");
    }
}
