use types::{txid_t, pgid_t};
use page::{Page, get_page_header_size, merge_pgids, merge_pgids_raw, FREELIST_PAGE_FLAG};
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::mem;
use std::slice;

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
    pub fn copyall(&self, dst: *mut pgid_t) {
        let mut m = Vec::with_capacity(self.pending_count());

        for (_, list) in self.pending.iter() {
            let mut copy_list = list.to_vec();
            m.append(&mut copy_list);
        }
        m.sort();
        merge_pgids_raw(dst, &self.ids, &m);
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
        match ids_option {
            None => panic!("pending should not be None"),
            Some(ids) => {
                for id in pgid..pgid + 1 + p.borrow().overflow as pgid_t {
                    // Verify that page is not already free.
                    if self.cache.contains(&id) {
                        panic!("page {} already freed")
                    }

                    // Add to the freelist and cache.
                    ids.push(id);
                    self.cache.insert(id);
                }
            },
        }
    }

    // release moves all page ids for a transaction id (or older) to the freelist.
    pub fn release(&mut self, txid: txid_t) {
        let mut m: Vec<pgid_t> = Vec::new();
        self.pending.retain(|tid, ids| {
            if *tid <= txid {
                m.append(&mut ids.to_vec());
                return false;
            }
            true
        });

        m.sort();
        let mut new_ids: Vec<pgid_t> = Vec::with_capacity(self.ids.len() + m.len());
        merge_pgids(&mut new_ids, &self.ids, &m);
        self.ids = new_ids;
    }

    // rollback removes the pages from a given pending tx.
    pub fn rollback(&mut self, txid: txid_t) {
        // Remove page ids from cache.
        for id in &self.pending[&txid] {
            self.cache.remove(id);
        }

        // Remove pages from pending list
        self.pending.remove(&txid);
    }

    // freed returns whether a given page is in the free list
    pub fn freed(&self, pgid: pgid_t) -> bool {
        self.cache.contains(&pgid)
    }

    // read initializes the freelist from a freelist page.
    pub fn read(&mut self, p: &Page) {
        // If the page.count is at the max uint16 value (64k) then it's considered
        // an overflow and the size of the freelist is stored as the first element.
        let mut idx: usize = 0;
        let mut count: usize = p.count as usize;
        if count == 0xFFFF {
            idx = 1;
            let pgid_ptr = &p.ptr as *const usize as *const pgid_t;
            count = unsafe { (*pgid_ptr) as usize };
        }

        // Copy the list of page ids from the freelist
        if count == 0 {
            self.ids.clear();
        } else {
            let pgid_ptr = &p.ptr as *const usize as *const pgid_t;
            self.ids.reserve(count - idx);
            let mut pgids_slice = unsafe {
                slice::from_raw_parts(pgid_ptr.offset(idx as isize), count)
            };
            self.ids.append(&mut pgids_slice.to_vec());

            // Make sure they're sorted.
            self.ids.sort();
        }

        // Rebuild the page cache.
        self.reindex();
    }

    // writes the page ids onto a freelist page. All free and pending ids are
    // saved to disk since in the event of a program crash, all pending ids will
    // become free.
    pub fn write(&self, p: &mut Page) {
        // Combine the old free pgids and pgids waiting on an open transaction.

        // Update the header flag.
        p.flags |= FREELIST_PAGE_FLAG;

        // The page.count can only hold up to 64k elementes so if we overflow that
        // number then we handle it by putting the size in the first element.
        let lenids = self.count();
        if lenids == 0 {
            p.count = lenids as u16;
        } else if lenids < 0xFFFF {
            p.count = lenids as u16;
            let mut pgid_ptr = &mut p.ptr as *mut usize as *mut pgid_t;
            /*
            let mut dst = unsafe {
                Vec::from_raw_parts(pgid_ptr, 0, lenids)
            };
            */
            self.copyall(pgid_ptr);
        } else {
            p.count = 0xFFFF;
            let mut pgid_ptr = &mut p.ptr as *mut usize as *mut pgid_t;
            unsafe {*pgid_ptr = lenids as u64;}
            /*
            let mut dst = unsafe {
                Vec::from_raw_parts(pgid_ptr.offset(1), 0, lenids)
            };
            */
            self.copyall(unsafe {pgid_ptr.offset(1)});
        }
    }

    // reload reads the freelist from a page and filters out pending items.
    pub fn reload(&mut self, p: &Page) {
        self.read(p);

        // Build a cache of only pending pages.
        let mut pcache: HashSet<pgid_t> = HashSet::new();

        for pending_ids in self.pending.values() {
            for pending_id in pending_ids {
                pcache.insert(*pending_id);
            }
        }

        // Check each page in the freelist and build a new available freelist
        // with any pages not in the pending lists.
        let mut a: Vec<pgid_t> = Vec::new();
        for id in &self.ids {
            if !pcache.contains(id) {
                a.push(*id);
            }
        }
        self.ids = a;

        // Once the available list is rebuilt then rebuild the free cache so that
        // it includes the available and pending free pages.
        self.reindex();
    }

    // reindex rebuilds the free cache based on available and pending free lists.
    pub fn reindex(&mut self) {
        self.cache.clear();
        self.cache.reserve(self.ids.len());
        for id in &self.ids {
            self.cache.insert(*id);
        }

        for pending_ids in self.pending.values() {
            for pending_id in pending_ids {
                self.cache.insert(*pending_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use freelist::FreeList;
    use std::rc::Rc;
    use std::cell::RefCell;
    use page::{Page, FREELIST_PAGE_FLAG};
    use std::collections::{HashMap, HashSet};
    use types::pgid_t;

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
    fn freelist_release() {
        let mut f = FreeList::new();
        let page1 = Rc::new(RefCell::new(Page {
            id: 12,
            flags: 0,
            count: 0,
            overflow: 1,
            ptr: 0,
        }));
        f.free(100, Rc::clone(&page1));

        let page2 = Rc::new(RefCell::new(Page {
            id: 9,
            flags: 0,
            count: 0,
            overflow: 0,
            ptr: 0,
        }));
        f.free(100, Rc::clone(&page2));

        let page3 = Rc::new(RefCell::new(Page {
            id: 39,
            flags: 0,
            count: 0,
            overflow: 0,
            ptr: 0,
        }));
        f.free(102, Rc::clone(&page3));

        f.release(100);
        f.release(101);
        assert_eq!(f.ids, vec![9,12,13]);

        f.release(102);
        assert_eq!(f.ids, vec![9,12,13, 39]);

    }

    #[test]
    fn freelist_allocate() {
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

    #[test]
    fn freelist_read() {
        // Create a page.
        let mut buf: [u8; 4096] = [0; 4096];
        let page: *mut Page = buf.as_mut_ptr() as *mut Page;
        unsafe {
            (*page).flags = FREELIST_PAGE_FLAG;
            (*page).count = 2;
        }

        // Insert 2 page ids
        let ids_ptr: *mut pgid_t = unsafe {
            &mut (*page).ptr as *mut usize as *mut pgid_t
        };
        unsafe {
            *ids_ptr = 23;
            *ids_ptr.offset(1) = 50;
        }

        // Deserialize page into a freelist.
        let mut f = FreeList::new();
        unsafe {
            f.read(&(*page));
        }

        // Ensure that there are two page ids in the freelist.
        assert_eq!(f.ids, vec![23, 50]);
    }

    #[test]
    fn freelist_write() {
        // Create a freelist and write it to a page.
        let mut buf: [u8; 4096] = [0; 4096];
        let page: *mut Page = buf.as_mut_ptr() as *mut Page;
        let mut f = FreeList {
            ids: vec![12, 39],
            pending: HashMap::new(),
            cache: HashSet::new(),
        };
        f.pending.insert(100, vec![28, 11]);
        f.pending.insert(101, vec![3]);

        unsafe { f.write(page.as_mut().unwrap()); };

        // Read the page back out
        let mut f2 = FreeList::new();
        let p_const = page as *const Page;
        unsafe {
            f2.read(&(*p_const));
        }

        // Ensure that the freelist is correct.
        // All pages should be present and in reverse order.
        assert_eq!(f2.ids, vec![3, 11, 12, 28, 39]);
    }
}
