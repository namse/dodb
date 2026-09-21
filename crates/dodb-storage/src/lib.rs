//! Storage-facing foundations for dodb.
//!
//! No B+Tree, allocator, WAL, recovery, or checkpoint implementation belongs
//! in this Phase 0 crate yet.

pub mod durable_file;
pub mod page;
pub mod superblock;

pub use durable_file::{DurableFile, ProductionFile};
pub use page::{
    DecodedPage, PAGE_HEADER_SIZE, PAGE_SIZE, PageHeader, PageType, decode_page, decode_page_at,
    encode_page,
};
pub use superblock::{
    SUPERBLOCK_FORMAT_VERSION, SelectedSuperblock, Superblock, SuperblockSlot, choose_superblock,
    decode_superblock, encode_superblock,
};
