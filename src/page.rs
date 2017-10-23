use types::pgid_t;
use std::mem;
use std::slice;
use std::fmt;
use meta::Meta;

pub const MAX_PAGE_SIZE: usize = 0x7FFFFFF;
pub const MAX_ALLOC_SIZE: usize = 0x7FFFFFFF;

pub const BRANCH_PAGE_FLAG: u16 = 0x01;
pub const LEAF_PAGE_FLAG: u16 = 0x02;
pub const META_PAGE_FLAG: u16 = 0x04;
pub const FREELIST_PAGE_FLAG: u16 = 0x10;

// pub const PageHeaderSize: isize = intrusive_collections::offset_of!(page, ptr);
//pub const PageHeaderSize: isize = offset_of!(page, ptr);
pub static mut PAGE_HEADER_SIZE: usize = 0;
pub const MIN_KEY_PER_PAGE: i32 = 2;
pub const BRANCH_PAGE_ELEMENT_SIZE: usize = mem::size_of::<BranchPageElement>();
pub const LEAF_PAGE_ELEMENT_SIZE: usize = mem::size_of::<LeafPageElement>();

pub fn initialize() {
    unsafe {
        PAGE_HEADER_SIZE = offset_of!(Page, ptr) as usize;
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
        let ptr: *const u8 = self as *const Page as *const u8;
        unsafe {
            ptr.offset(self.ptr as isize) as *const Meta
        }
    }

    pub fn leaf_page_element(&self, index: u16) -> *const LeafPageElement {
        let ptr: *const u8 = self as *const Page as *const u8;
        unsafe {
            let leaf_ptr = ptr.offset(self.ptr as isize) as *const LeafPageElement;
            leaf_ptr.offset(index as isize)
        }
    }

    pub fn leaf_page_elements(&self) -> &'a [LeafPageElement] {
        let ptr: *const u8 = self as *const Page as *const u8;
        unsafe {
            let leaf_ptr = ptr.offset(self.ptr as isize) as *const LeafPageElement;
            return slice::from_raw_parts(leaf_ptr, MAX_PAGE_SIZE)
        }
    }

    pub fn branch_page_element(&self, index: u16) -> *const BranchPageElement {
        let ptr: *const u8 = self as *const Page as *const u8;
        unsafe {
            let leaf_ptr = ptr.offset(self.ptr as isize) as *const BranchPageElement;
            leaf_ptr.offset(index as isize)
        }
    }

    pub fn branch_page_elements(&self) -> &'a [BranchPageElement] {
        let ptr: *const u8 = self as *const Page as *const u8;
        unsafe {
            let leaf_ptr = ptr.offset(self.ptr as isize) as *const BranchPageElement;
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

#[cfg(test)]
mod tests {
    use page;
    use std::mem;
    use std::ptr;
    use types::pgid_t;

    #[test]
    fn offset_of_works() {
        page::initialize();
        unsafe {
            assert_eq!(page::PAGE_HEADER_SIZE, 16);
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
}
