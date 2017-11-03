use db::{Meta, DB};
use page::Page;
use types::pgid_t;
use std::time::Duration;
use std::ops::{Add, Sub, AddAssign, SubAssign};
use std::rc::Rc;
use std::cell::RefCell;

pub struct Tx {
    pub meta: Meta,
    pub stats: TxStats,
    pub db: Rc<RefCell<DB>>
}

impl Tx {
    pub fn new(db: &Rc<RefCell<DB>>) -> Tx {
        Tx {
            meta: Meta::new(),
            stats: TxStats::new(),
            db: Rc::clone(db),
        }
    }

    // page returns a reference to the page with a given id.
    // If page has been written to then a temporary buffered page is returned.
    pub fn page(&self, pgid: pgid_t) -> Rc<RefCell<Page>> {
        unimplemented!();
    }

    // delegate to freelist.
    // releases a page and its overflow for a given transaction id.
    // If the page is already free then a panic will occur.
    pub fn free(&self, pgid: pgid_t) {
        let db_borrow = self.db.borrow_mut();
        let mut freelist_borrow = db_borrow.freelist.borrow_mut();

        freelist_borrow.free(self.meta.pgid, Rc::clone(&self.page(pgid)));
    }

    // allocate returns a continuous block of memory starting at a given page
    pub fn allocate(&self, count: usize) -> Result<Rc<RefCell<Page>>, &'static str> {
//        return Err("沟里个就算所以")
        unimplemented!();
    }

    pub fn get_page_size(&self) -> usize {
        self.db.borrow().page_size
    }
}

// TxStats represents statistics about the actions performed by the transaction.
pub struct TxStats {
    // Page statistics.
    pub page_count: i32, // number of page allocations
    pub page_alloc: i32, // total bytes allocated

    // Cursor statistics.
    pub cursor_count: i32, // number of cursors created

    // Node statistics
    pub node_count: i32, // number of node allocations
    pub node_deref: i32, // number of node dereferences

    // Rebalance statistics.
    pub rebalance: i32,           // number of node rebalances
    pub rebalance_time: Duration, // total time spent rebalancing

    // Split/Spill statistics.
    pub split: i32,           // number of nodes split
    pub spill: i32,           // number of nodes spilled
    pub spill_time: Duration, // total time spent spilling

    // Write statistics.
    pub write: i32,           // number of writes performed
    pub write_time: Duration // total time spent writing to disk
}

impl TxStats {
    pub fn new() -> TxStats {
        TxStats{
            page_count: 0,
            page_alloc: 0,
            cursor_count: 0,
            node_count: 0,
            node_deref: 0,
            rebalance: 0,
            rebalance_time: Duration::new(0, 0),
            split: 0,
            spill: 0,
            spill_time: Duration::new(0, 0),
            write: 0,
            write_time: Duration::new(0,0),
        }
    }
}

impl AddAssign<TxStats> for TxStats {
    fn add_assign(&mut self, rhs: TxStats) {
        *self = TxStats{
            page_count: self.page_count + rhs.page_count,
            page_alloc: self.page_alloc + rhs.page_alloc,
            cursor_count: self.cursor_count + rhs.cursor_count,
            node_count: self.node_count + rhs.node_count,
            node_deref: self.node_deref + rhs.node_deref,
            rebalance: self.rebalance + rhs.rebalance,
            rebalance_time: self.rebalance_time + rhs.rebalance_time,
            split: self.split + rhs.split,
            spill: self.spill + rhs.spill,
            spill_time: self.spill_time + rhs.spill_time,
            write: self.write + rhs.write,
            write_time: self.write_time + rhs.write_time,
        };
    }
}

impl SubAssign<TxStats> for TxStats {
    fn sub_assign(&mut self, rhs: TxStats) {
        *self = TxStats{
            page_count: self.page_count - rhs.page_count,
            page_alloc: self.page_alloc - rhs.page_alloc,
            cursor_count: self.cursor_count - rhs.cursor_count,
            node_count: self.node_count - rhs.node_count,
            node_deref: self.node_deref - rhs.node_deref,
            rebalance: self.rebalance - rhs.rebalance,
            rebalance_time: self.rebalance_time - rhs.rebalance_time,
            split: self.split - rhs.split,
            spill: self.spill - rhs.spill,
            spill_time: self.spill_time - rhs.spill_time,
            write: self.write - rhs.write,
            write_time: self.write_time - rhs.write_time,
        };
    }
}

impl Add<TxStats> for TxStats {
    type Output = TxStats;

    fn add(self, rhs: TxStats) -> Self::Output {
        TxStats{
            page_count: self.page_count + rhs.page_count,
            page_alloc: self.page_alloc + rhs.page_alloc,
            cursor_count: self.cursor_count + rhs.cursor_count,
            node_count: self.node_count + rhs.node_count,
            node_deref: self.node_deref + rhs.node_deref,
            rebalance: self.rebalance + rhs.rebalance,
            rebalance_time: self.rebalance_time + rhs.rebalance_time,
            split: self.split + rhs.split,
            spill: self.spill + rhs.spill,
            spill_time: self.spill_time + rhs.spill_time,
            write: self.write + rhs.write,
            write_time: self.write_time + rhs.write_time,
        }
    }
}

impl Sub<TxStats> for TxStats {
    type Output = TxStats;

    fn sub(self, rhs: TxStats) -> Self::Output {
        TxStats{
            page_count: self.page_count - rhs.page_count,
            page_alloc: self.page_alloc - rhs.page_alloc,
            cursor_count: self.cursor_count - rhs.cursor_count,
            node_count: self.node_count - rhs.node_count,
            node_deref: self.node_deref - rhs.node_deref,
            rebalance: self.rebalance - rhs.rebalance,
            rebalance_time: self.rebalance_time - rhs.rebalance_time,
            split: self.split - rhs.split,
            spill: self.spill - rhs.spill,
            spill_time: self.spill_time - rhs.spill_time,
            write: self.write - rhs.write,
            write_time: self.write_time - rhs.write_time,
        }
    }
}

