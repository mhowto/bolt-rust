#![plugin(quickcheck_macros)]


use types::pgid_t;
use std::mem;
use std::slice;
use std::fmt;
use meta::Meta;
use std::sync::{Once, ONCE_INIT};

pub const MAX_PAGE_SIZE: usize = 0x7FFFFFF;
pub const MAX_ALLOC_SIZE: usize = 0x7FFFFFFF;

pub const BRANCH_PAGE_FLAG: u16 = 0x01;
pub const LEAF_PAGE_FLAG: u16 = 0x02;
pub const META_PAGE_FLAG: u16 = 0x04;
pub const FREELIST_PAGE_FLAG: u16 = 0x10;

// pub const PageHeaderSize: isize = intrusive_collections::offset_of!(page, ptr);
//pub const PageHeaderSize: isize = offset_of!(page, ptr);
pub const MIN_KEYS_PER_PAGE: i32 = 2;
pub const BRANCH_PAGE_ELEMENT_SIZE: usize = mem::size_of::<BranchPageElement>();
pub const LEAF_PAGE_ELEMENT_SIZE: usize = mem::size_of::<LeafPageElement>();

static mut PAGE_HEADER_SIZE: usize = 0;
static INIT: Once = ONCE_INIT;

// Accessing a `static mut` is unsafe much of the time, but if we do so
// in a synchronized fashion (e.g. write once or read all) then we're
// good to go!
//
// This function will only call `expensive_computation` once, and will
// otherwise always return the value returned from the first invocation.
pub fn get_page_header_size() -> usize {
    unsafe {
        INIT.call_once(|| {
            PAGE_HEADER_SIZE = offset_of!(Page, ptr) as usize;
        });
        PAGE_HEADER_SIZE
    }
}

#[repr(C, packed)]
pub struct Page {
    pub id: pgid_t,
    pub flags: u16,
    pub count: u16,
    pub overflow: u32,
    pub ptr: usize,
}

impl <'a> Page {
    pub fn typ(&self) -> String {
        if (self.flags & BRANCH_PAGE_FLAG) != 0 {
            return "branch".to_string()
        } else if (self.flags & LEAF_PAGE_FLAG) != 0 {
            return "leaf".to_string()
        } else if (self.flags & META_PAGE_FLAG) != 0 {
            return "meta".to_string()
        } else if (self.flags & FREELIST_PAGE_FLAG) != 0 {
            return "freelist".to_string()
        }
        fmt::format(format_args!("unknown{}", self.flags))
    }

    pub fn meta(&self) -> *const Meta{
        &self.ptr as *const usize as *const Meta
    }

    pub fn leaf_page_element(&self, index: u16) -> *const LeafPageElement {
        unsafe {
            let leaf_ptr = &self.ptr as *const usize as *const LeafPageElement;
            leaf_ptr.offset(index as isize)
        }
    }

    pub fn leaf_page_elements(&self) -> &'a [LeafPageElement] {
//        let ptr: *const u8 = self as *const Page as *const u8;
        unsafe {
            let leaf_ptr = &self.ptr as *const usize as *const LeafPageElement;
            return slice::from_raw_parts(leaf_ptr, MAX_PAGE_SIZE)
        }
    }

    pub fn branch_page_element(&self, index: u16) -> *const BranchPageElement {
        unsafe {
            let leaf_ptr = &self.ptr as *const usize as *const BranchPageElement;
            leaf_ptr.offset(index as isize)
        }
    }

    pub fn branch_page_elements(&self) -> &'a [BranchPageElement] {
        unsafe {
            let leaf_ptr = &self.ptr as *const usize as *const BranchPageElement;
            return slice::from_raw_parts(leaf_ptr, MAX_PAGE_SIZE)
        }
    }

    // writes n bytes of the page to STDERR as hex output.
    pub fn hexdump(&self, n: i32) {
        let ptr: *const u8 = self as *const Page as *const u8;
        unsafe {
            let buf = slice::from_raw_parts(ptr, MAX_ALLOC_SIZE);
            // TODO: fmt string to [u8] as binary
            eprintln!("{:?}", buf);
        }
    }
}

// represents a node on a branch page.
#[repr(C, packed)]
pub struct BranchPageElement {
    pub pos: u32,
    pub ksize: u32,
    pub pgid: pgid_t,
}

impl<'a> BranchPageElement {
    pub fn key(&self) -> &'a [u8] {
        let ptr: *const u8 = self as *const BranchPageElement as *const u8;
        unsafe {
            let start: *const u8 = ptr.offset(self.pos as isize);
            return slice::from_raw_parts(start, self.ksize as usize);
        }
    }

    pub fn get_body_pointer(&self) -> *const u8 {
        let ptr: *const u8 = self as *const BranchPageElement as *const u8;
        unsafe { ptr.offset(self.pos as isize) }
    }
}

// represents a node on a leaf page.
#[repr(C, packed)]
pub struct LeafPageElement {
    pub flags: u32,
    pub pos: u32,
    pub ksize: u32,
    pub vsize: u32,
}

impl<'a> LeafPageElement {
    pub fn key(&self) -> &'a [u8] {
        let ptr: *const u8 = self as *const LeafPageElement as *const u8;
        unsafe {
            let start: *const u8 = ptr.offset(self.pos as isize);
            slice::from_raw_parts(start, self.ksize as usize)
        }
    }

    pub fn value(&self) -> &'a [u8] {
        let ptr: *const u8 = self as *const LeafPageElement as *const u8;
        unsafe {
            let start: *const u8 = ptr.offset(self.pos as isize + self.ksize as isize);
            slice::from_raw_parts(start, self.vsize as usize)
        }
    }

    pub fn get_body_pointer(&self) -> *const u8 {
        let ptr: *const u8 = self as *const LeafPageElement as *const u8;
        unsafe { ptr.offset(self.pos as isize) }
    }
}

// merge_pgids copies the sorted union of a and b into dst.
// If dst is too small, it panics
pub fn merge_pgids(dst: &mut Vec<pgid_t>, a: &Vec<pgid_t>, b: &Vec<pgid_t>) {
    // TODO: capacity -> len()
    if dst.capacity() < a.len() + b.len() {
        panic!("merge_pgids bad len {} < {} + {}", dst.len(), a.len(), b.len());
    }

    // Copy in the opposite slice if one is nil.
    if a.len() == 0 {
        let mut copy_a = a.to_vec();
        dst.append(&mut copy_a);
    }

    if b.len() == 0 {
        let mut copy_b = b.to_vec();
        dst.append(&mut copy_b);
    }

    // Merged will hold all elements from both lists.
    let mut lead = a.as_slice();
    let mut follow = b.as_slice();
    if b[0] < a[0] {
        lead = b.as_slice();
        follow = a.as_slice();
    }

    // Continue while there are elements in the lead.
    while lead.len() >0 {
        // Merge largest prefix of lead that is ahead of follow[0].
        let result = lead.binary_search(&follow[0]);
        let n = match result {
            Ok(nn) => nn,
            Err(nn) => nn,
        };
        let mut merged_copy = lead[..n].to_vec();
        dst.append(&mut merged_copy);
        if n >= lead.len() {
            break;
        }

        // Swap lead and follow.
        let mut temp = &lead[n..];
        lead = follow;
        follow = temp;
    }

    // Append what's left in follow.
    let mut merged_copy = follow.to_vec();
    dst.append(&mut merged_copy);
}

#[cfg(test)]
extern crate quickcheck;

#[cfg(test)]
mod tests {
    use page;
    use std::mem;
    use std::ptr;
    use types::pgid_t;

    #[test]
    fn offset_of_works() {
        unsafe {
            assert_eq!(page::get_page_header_size(), 16);
        }
        assert_eq!(page::BRANCH_PAGE_ELEMENT_SIZE, 16);
        assert_eq!(page::LEAF_PAGE_ELEMENT_SIZE, 16);
    }

    #[test]
    fn branch_page_element_dump() {
        #[repr(C, packed)]
        struct _BranchPageElement {
            pub pos: u32,
            pub ksize: u32,
            pub pgid: pgid_t,
            pub key: [u8; 4],
        }
        let mut ele = _BranchPageElement {
            pos: 16,
            ksize: 4,
            pgid: 0,
            key: [0; 4],
        };
        let key = "weep";
        let mut key_pointer = &mut ele as *mut _BranchPageElement as *mut u8;
        unsafe {
            key_pointer = key_pointer.offset(ele.pos as isize);
            ptr::copy(key.as_ptr(), key_pointer, key.len());

            let branch_page_element: *const page::BranchPageElement =
                &ele as *const _BranchPageElement as *const page::BranchPageElement;
            assert_eq!((*branch_page_element).pos, ele.pos);
            assert_eq!((*branch_page_element).ksize, ele.ksize);
            assert_eq!((*branch_page_element).key(), key.as_bytes());
        }
    }

    #[test]
    fn leaf_page_element_dump() {
        #[repr(C, packed)]
        struct _LeafPageElement {
            pub flags: u32,
            pub pos: u32,
            pub ksize: u32,
            pub vsize: u32,
            pub key: [u8; 4],
            pub value: [u8; 17],
        }
        assert_eq!(mem::size_of::<_LeafPageElement>(), 37);
        let mut ele = _LeafPageElement {
            flags: 0x04,
            pos: 16,
            ksize: 4,
            vsize: 17,
            key: [0; 4],
            value: [0; 17],
        };
        let key = "weep";
        let mut key_pointer = &mut ele as *mut _LeafPageElement as *mut u8;
        let value = "weep teers of joy";
        unsafe {
            key_pointer = key_pointer.offset(ele.pos as isize);
            ptr::copy(key.as_ptr(), key_pointer, key.len());
            let value_pointer = key_pointer.offset(ele.ksize as isize);
            ptr::copy(value.as_ptr(), value_pointer, value.len());

            let leaf_page_element: *const page::LeafPageElement =
                &ele as *const _LeafPageElement as *const page::LeafPageElement;
            assert_eq!((*leaf_page_element).flags, ele.flags);
            assert_eq!((*leaf_page_element).pos, ele.pos);
            assert_eq!((*leaf_page_element).ksize, ele.ksize);
            assert_eq!((*leaf_page_element).vsize, ele.vsize);
            assert_eq!((*leaf_page_element).key(), key.as_bytes());
            assert_eq!((*leaf_page_element).value(), value.as_bytes());
            // if let Some(leaf) = leaf_page_element.as_ref() {
            // assert_eq!(leaf.key(), key.as_bytes());
            // }
        }
    }

    #[test]
    fn pgids_merge() {
        {
            let a: Vec<pgid_t> = vec![4, 5, 6, 10, 11, 12, 13, 27];
            let b: Vec<pgid_t> = vec![1, 3, 8, 9, 25, 30];
            let mut c: Vec<pgid_t> = Vec::with_capacity(a.len() + b.len());
            page::merge_pgids(&mut c, &a, &b);
            assert_eq!(c, vec![1, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 25, 27, 30]);
        }

        {
            let a: Vec<pgid_t> = vec![4, 5, 6, 10, 11, 12, 13, 27, 35, 36];
            let b: Vec<pgid_t> = vec![8, 9, 25, 30];
            let mut c: Vec<pgid_t> = Vec::with_capacity(a.len() + b.len());
            page::merge_pgids(&mut c, &a, &b);
            assert_eq!(c, vec![4, 5, 6, 8, 9, 10, 11, 12, 13, 25, 27, 30, 35, 36]);
        }
    }

    fn reverse<T: Clone>(xs: &[T]) -> Vec<T> {
        let mut rev = vec!();
        for x in xs {
            rev.insert(0, x.clone())
        }
        rev
    }

    #[quickcheck]
    fn double_reversal_is_identity(xs: Vec<isize>) -> bool {
        xs != reverse(&reverse(&xs))
    }

    /*
    #[quickcheck]
    fn pgids_merge_quick(a: Vec<pgid_t>, b: Vec<pgid_t>) -> bool {

        let mut a_mut = a.to_vec();
        let mut b_mut = b.to_vec();
        a_mut.sort();
        b_mut.sort();

        println!("a: {:?}", a_mut.to_vec());
        println!("b: {:?}", b_mut.to_vec());

        let mut c = Vec::with_capacity(a.len() + b.len());
        page::merge_pgids(&mut c, &a_mut, &b_mut);
        println!("c: {:?}", c.to_vec());

        a_mut.append(&mut b_mut);
        a_mut.sort();
        c == a_mut
    }
    */
}
