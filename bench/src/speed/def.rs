pub const PAGE_SIZE: usize = 8;
pub const L: usize = 16;

// dev
// pub const HOVER_INTERVAL: usize = 10;
// pub const HOVER_RECREATE_BLOCK: u64 = 2;
// pub const HOVER_WRITE_BLOCK: u64 = 2;

// test
// pub const HOVER_INTERVAL: usize = 1000;
// pub const HOVER_RECREATE_BLOCK: u64 = 100;
// pub const HOVER_WRITE_BLOCK: u64 = 100;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
