use types::{txid_t, pgid_t};
use page::{Page, get_page_header_size, merge_pgids};
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::mem;

// FreeList represents a list of all pages that are available for allocation.
// It also tracks pages that have been freed but are still in use by open transactions.
pub struct FreeList {
    pub ids: Vec<pgid_t>, // all free and available free page ids
    pub pending: HashMap<txid_t, Vec<pgid_t>>, // mapping of soon-to-be free page ids by tx
    pub cache: HashSet<pgid_t>, // fast lookup of all free and pending page ids
}

impl FreeList {
    pub fn new() -> FreeList {
        FreeList{
            ids: vec![],
            pending: HashMap::new(),
            cache: HashSet::new(),
        }
    }

    // returns the size of the page after serialization.
    pub fn size(&self) -> usize {
        let mut n = self.count();
        if n >= 0xFFFF {
            // The first element will be used to store the count. See freelist.write.
            n += 1;
        }
        get_page_header_size() + mem::size_of::<pgid_t>() * n
    }

    pub fn count(&self) -> usize {
        self.free_count() + self.pending_count()
    }

    pub fn free_count(&self) -> usize {
        self.ids.len()
    }

    pub fn pending_count(&self) -> usize {
        let mut count: usize = 0;
        for (_, val) in self.pending.iter() {
            count += val.len()
        }
        count
    }

    // copyall copies into dst a list of all free ids and all pending ids in one sorted list.
    // f.count returns the minimum length required for dst.
    pub fn copyall(&self, dst: &mut Vec<pgid_t>) {
        let mut m = Vec::with_capacity(self.pending_count());

        for (_, list) in self.pending.iter() {
            let mut copy_list = list.to_vec();
            m.append(&mut copy_list);
        }
        m.sort();
        merge_pgids(dst, &self.ids, &m);
    }

    // allocate returns the starting page id of a contiguous list of pages of a given size.
    // If a contiguous block cannot be found then 0 is returned.
    pub fn allocate(&mut self, n: usize) -> pgid_t {
        if self.ids.len() == 0 {
            return 0;
        }

        let mut initial: pgid_t = 0;
        let mut previd: pgid_t = 0;
        let mut found_index: Option<usize> = None;
        for i in 0..self.ids.len() {
            let id = self.ids[i];
            if id <= 1 {
                panic!("invalid page allocation: {}", id);
            }

            // Reset initial page if this is not contiguous.
            if previd == 0 || id - previd != 1 {
                initial = id;
            }

            // If we found a contiguous block then remove it and return it.
            if (id - initial) + 1 == n as pgid_t {
                found_index = Some(i);
                break;
            }

            previd = id
        }

        match found_index {
            None => 0,
            Some(idx) => {
                // If we're allocating off the beginning then take the fast path
                // and just adjust the existing slice. This will use extra memory
                // temporarily but the append() in free() will realloc the slice
                // as is necessary.
                if idx + 1 == n {
                    self.ids.drain(..idx+1);
                } else {
                    self.ids.drain(idx-n+1..idx+1);
                }

                // Remove from the free cache
                for i in 0 as pgid_t .. n as pgid_t {
                    self.cache.remove(&i);
                }

                initial
            }
        }
    }

    // free releases a page and its overflow for a given transaction id.
    // If the page is already free then a panic will occur.
    pub fn free(&mut self, txid: txid_t, p: Rc<RefCell<Page>>) {
        let pgid = p.borrow().id;
        if pgid <= 1 {
            panic!("cannot free page 0 or 1: {}", pgid);
        }

        // Free page and all its overflow pages.
        if !self.pending.contains_key(&txid) {
            self.pending.insert(txid, Vec::new());
        }
        let ids_option = self.pending.get_mut(&txid);
        let ids = ids_option.unwrap();

        for id in pgid..pgid+1+p.borrow().overflow as pgid_t {
            // Verify that page is not already free.
            if self.cache.contains(&id) {
                panic!("page {} already freed")
            }

            // Add to the freelist and cache.
            ids.push(id);
            self.cache.insert(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use freelist::FreeList;
    use std::rc::Rc;
    use std::cell::RefCell;
    use page::Page;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn freelist_free() {
        let mut f = FreeList::new();
        let page = Rc::new(RefCell::new(Page{
            id: 12,
            flags: 0,
            count: 0,
            overflow: 0,
            ptr: 0,
        }));
        f.free(100, Rc::clone(&page));
        assert_eq!(f.pending[&100], vec![12]);
    }

    #[test]
    fn freelist_free_overflow() {
        let mut f = FreeList::new();
        let page = Rc::new(RefCell::new(Page{
            id: 12,
            flags: 0,
            count: 0,
            overflow: 3,
            ptr: 0,
        }));
        f.free(100, Rc::clone(&page));
        assert_eq!(f.pending[&100], vec![12,13,14,15]);
    }

    #[test]
    fn freelist_allocate() {
//        let mut f = FreeList::new();
        let mut f = FreeList {
            ids: vec![3,4,5,6,7,9,12,13,18],
            pending: HashMap::new(),
            cache: HashSet::new(),
        };

        assert_eq!(f.allocate(3), 3);
        assert_eq!(f.allocate(1), 6);
        assert_eq!(f.allocate(3), 0);
        assert_eq!(f.allocate(2), 12);
        assert_eq!(f.allocate(1), 7);
        assert_eq!(f.allocate(0), 0);
        assert_eq!(f.allocate(0), 0);
        assert_eq!(f.ids, vec![9,18]);

        assert_eq!(f.allocate(1), 9);
        assert_eq!(f.allocate(1), 18);
        assert_eq!(f.allocate(1), 0);
        assert_eq!(f.ids, vec![]);
    }
}
