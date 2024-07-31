pub const BYTES_CACHE_SHARD_COUNT: usize = 512;
pub const BIG_BUF_SIZE: usize = 64 * 1024; // 64KB

pub const ENTRY_FIXED_LENGTH: usize = 1 + 3 + 1 + 32 + 8 + 8 + 8;
pub const NULL_ENTRY_VERSION: i64 = -2;
pub const SHARD_COUNT: usize = 16; //excludes code shard
pub const CODE_SHARD_ID: usize = 16;
pub const DEFAULT_ENTRY_SIZE: usize = 300;

pub const SENTRY_COUNT: usize = (1 << 16) / SHARD_COUNT; // 4096

pub const PRUNE_EVERY_NBLOCKS: i64 = 32;

pub const PRE_READ_BUF_SIZE: usize = 256 * 1024;

pub const SHARD_DIV: usize = (1 << 16) / SHARD_COUNT; // 4096
pub const OP_READ: u8 = 1;
pub const OP_WRITE: u8 = 2;
pub const OP_CREATE: u8 = 3;
pub const OP_DELETE: u8 = 4;

pub const DEFAULT_FILE_SIZE: i64 = 1024 * 1024;
pub const SMALL_BUFFER_SIZE: i64 = 32 * 1024;

pub const FIRST_LEVEL_ABOVE_TWIG: i64 = 13;
pub const TWIG_ROOT_LEVEL: i64 = FIRST_LEVEL_ABOVE_TWIG - 1; // 12
pub const MIN_PRUNE_COUNT: u64 = 2;
pub const CODE_PATH: &str = "code";
pub const ENTRIES_PATH: &str = "entries";
pub const TWIG_PATH: &str = "twig";
pub const TWIG_SHARD_COUNT: usize = 4;
pub const NODE_SHARD_COUNT: usize = 4;
pub const MAX_TREE_LEVEL: usize = 64;
pub const MAX_UPPER_LEVEL: usize = MAX_TREE_LEVEL - FIRST_LEVEL_ABOVE_TWIG as usize; // 51

pub const TWIG_SHIFT: u32 = 11; // a twig has 2**11 leaves
pub const LEAF_COUNT_IN_TWIG: u32 = 1 << TWIG_SHIFT; // 2**11==2048
pub const TWIG_MASK: u32 = LEAF_COUNT_IN_TWIG - 1;

pub const COMPACT_THRES: i64 = 200000;
pub const COMPACT_TRIGGER: usize = COMPACT_THRES as usize / 10;
pub const UTILIZATION_RATIO: i64 = 128;
pub const UTILIZATION_DIV: i64 = 256;

pub const TASK_CHAN_SIZE: usize = 10000;
pub const PREFETCHER_THREAD_COUNT: usize = 64;

pub const IN_BLOCK_IDX_BITS: usize = 24;
pub const IN_BLOCK_IDX_MASK: i64 = (1 << IN_BLOCK_IDX_BITS) - 1;

pub const COMPACT_RING_SIZE: usize = 1024;
