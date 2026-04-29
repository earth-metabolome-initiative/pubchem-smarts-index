use std::path::PathBuf;

use smarts_rs::screening::persisted::PersistedShardCompression;

#[derive(Clone, Debug)]
pub(crate) struct PubChemIndexConfig {
    pub(crate) shard_dir: PathBuf,
    pub(crate) shard_size: usize,
    pub(crate) compression: PersistedShardCompression,
}

impl PubChemIndexConfig {
    pub(crate) fn new() -> Self {
        Self {
            shard_dir: PathBuf::from("data/shards"),
            shard_size: 15_000_000,
            compression: PersistedShardCompression::Zstd {
                level: 19,
                worker_threads: default_zstd_threads(),
            },
        }
    }
}

fn default_zstd_threads() -> u32 {
    let available = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    u32::try_from(available).unwrap_or(u32::MAX)
}
