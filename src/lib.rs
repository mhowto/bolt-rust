#![feature(const_fn, const_size_of)]
#![feature(custom_attribute)] #![feature(plugin)]
#![feature(test)]

extern crate test;

// Re-export core for use by macros
#[doc(hidden)]
pub extern crate core as __core;

#[macro_use]
mod macros;
// #[macro_use]
// extern crate intrusive_collections;
// extern crate memoffset;

mod bucket;
mod cursor;
mod node;
mod types;
mod tx;
mod db;
mod page;
mod meta;
mod freelist;
