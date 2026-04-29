use std::{
    fs::File,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

use crate::{config::PubChemIndexConfig, errors::DynError};

#[derive(Debug)]
pub(crate) struct BuildReport {
    pub(crate) shards: Vec<ShardRecord>,
}

impl BuildReport {
    pub(crate) fn target_count(&self) -> usize {
        self.shards.iter().map(|shard| shard.target_count).sum()
    }

    pub(crate) fn upload_paths(&self) -> impl Iterator<Item = &Path> {
        self.shards
            .iter()
            .flat_map(|shard| [shard.path.as_path(), shard.pubchem_id_map_path.as_path()])
    }
}

pub(crate) fn write_manifest(
    config: &PubChemIndexConfig,
    report: &BuildReport,
) -> Result<PathBuf, DynError> {
    let manifest_path = config.shard_dir.join("pubchem-smarts-index-manifest.json");
    let manifest = IndexManifest::new(config, report);
    serde_json::to_writer_pretty(File::create(&manifest_path)?, &manifest)?;
    Ok(manifest_path)
}

#[derive(Debug, Serialize)]
struct IndexManifest {
    dataset: &'static str,
    generated_unix_seconds: u64,
    shard_size: usize,
    target_count: usize,
    shard_count: usize,
    compression: String,
    shards: Vec<ShardManifestEntry>,
}

impl IndexManifest {
    fn new(config: &PubChemIndexConfig, report: &BuildReport) -> Self {
        Self {
            dataset: "PubChem CID-SMILES",
            generated_unix_seconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |duration| duration.as_secs()),
            shard_size: config.shard_size,
            target_count: report.target_count(),
            shard_count: report.shards.len(),
            compression: config.compression.to_string(),
            shards: report.shards.iter().map(ShardManifestEntry::from).collect(),
        }
    }
}

#[derive(Debug, Serialize)]
struct ShardManifestEntry {
    file_name: String,
    pubchem_id_map_file_name: String,
    base_target_id: usize,
    target_count: usize,
    disk_bytes: u64,
    raw_epserde_bytes: usize,
    pubchem_id_map_disk_bytes: u64,
    pubchem_id_map_raw_bytes: usize,
    compression: String,
}

impl From<&ShardRecord> for ShardManifestEntry {
    fn from(shard: &ShardRecord) -> Self {
        Self {
            file_name: shard
                .path
                .file_name()
                .map_or_else(String::new, |name| name.to_string_lossy().into_owned()),
            pubchem_id_map_file_name: shard
                .pubchem_id_map_path
                .file_name()
                .map_or_else(String::new, |name| name.to_string_lossy().into_owned()),
            base_target_id: shard.base_target_id,
            target_count: shard.target_count,
            disk_bytes: shard.disk_bytes,
            raw_epserde_bytes: shard.raw_epserde_bytes,
            pubchem_id_map_disk_bytes: shard.pubchem_id_map_disk_bytes,
            pubchem_id_map_raw_bytes: shard.pubchem_id_map_raw_bytes,
            compression: shard.compression.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ShardRecord {
    pub(crate) path: PathBuf,
    pub(crate) pubchem_id_map_path: PathBuf,
    pub(crate) base_target_id: usize,
    pub(crate) target_count: usize,
    pub(crate) disk_bytes: u64,
    pub(crate) raw_epserde_bytes: usize,
    pub(crate) pubchem_id_map_disk_bytes: u64,
    pub(crate) pubchem_id_map_raw_bytes: usize,
    pub(crate) compression: String,
}
