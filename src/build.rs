use std::{fs, time::Instant};

use smarts_rs::{screening::persisted::PersistedTargetCorpusIndexShardBuilder, PreparedTarget};
use smiles_parser::{DatasetFetchOptions, DatasetSource, GzipMode, PUBCHEM_SMILES};

use crate::{
    cid_map::{pubchem_id_map_path_for_shard_path, store_pubchem_id_map},
    cli::BuildOptions,
    config::PubChemIndexConfig,
    errors::{invalid_data, DynError},
    manifest::{BuildReport, ShardRecord},
    pubchem::{prepare_targets, PreparedPubChemTarget, PubChemCidSmilesIter},
};

pub(crate) fn build_shards(
    config: &PubChemIndexConfig,
    options: BuildOptions,
) -> Result<BuildReport, DynError> {
    fs::create_dir_all(&config.shard_dir)?;
    let fetch_options = DatasetFetchOptions {
        cache_dir: None,
        gzip_mode: GzipMode::KeepCompressed,
        ..DatasetFetchOptions::default()
    };
    let artifact = PUBCHEM_SMILES.fetch_with_options(&fetch_options)?;
    let records = PubChemCidSmilesIter::open(artifact.path())?;
    let mut batch = Vec::with_capacity(8192);
    let mut writer = PubChemShardWriter::new(config, options);

    for (record_idx, record) in records.enumerate() {
        batch.push((record_idx, record?));
        if batch.len() == 8192 {
            writer.push(prepare_targets(&mut batch)?)?;
        }
    }
    writer.push(prepare_targets(&mut batch)?)?;
    writer.finish()
}

struct PubChemShardWriter<'a> {
    config: &'a PubChemIndexConfig,
    options: BuildOptions,
    targets: Vec<PreparedTarget>,
    pubchem_ids: Vec<u32>,
    base_target_id: usize,
    shard_index: usize,
    shards: Vec<ShardRecord>,
}

impl<'a> PubChemShardWriter<'a> {
    fn new(config: &'a PubChemIndexConfig, options: BuildOptions) -> Self {
        Self {
            config,
            options,
            targets: Vec::with_capacity(config.shard_size.min(1_000_000)),
            pubchem_ids: Vec::with_capacity(config.shard_size.min(1_000_000)),
            base_target_id: 0,
            shard_index: 0,
            shards: Vec::new(),
        }
    }

    fn push(&mut self, targets: Vec<PreparedPubChemTarget>) -> Result<(), DynError> {
        for target in targets {
            self.pubchem_ids.push(target.pubchem_id);
            self.targets.push(target.target);
        }
        self.flush_full_shards()
    }

    fn finish(mut self) -> Result<BuildReport, DynError> {
        if !self.targets.is_empty() {
            let targets = std::mem::take(&mut self.targets);
            let pubchem_ids = std::mem::take(&mut self.pubchem_ids);
            self.store_shard(targets, pubchem_ids)?;
        }
        Ok(BuildReport {
            shards: self.shards,
        })
    }

    fn flush_full_shards(&mut self) -> Result<(), DynError> {
        while self.targets.len() >= self.config.shard_size {
            let target_remainder = self.targets.split_off(self.config.shard_size);
            let id_remainder = self.pubchem_ids.split_off(self.config.shard_size);
            let targets = std::mem::replace(&mut self.targets, target_remainder);
            let pubchem_ids = std::mem::replace(&mut self.pubchem_ids, id_remainder);
            self.store_shard(targets, pubchem_ids)?;
        }
        Ok(())
    }

    fn store_shard(
        &mut self,
        targets: Vec<PreparedTarget>,
        pubchem_ids: Vec<u32>,
    ) -> Result<(), DynError> {
        if targets.len() != pubchem_ids.len() {
            return Err(
                invalid_data("prepared target count does not match PubChem CID count").into(),
            );
        }
        let file_name = format!(
            "target-index-shard-{shard:06}-base-{base}-len-{len}.{extension}",
            shard = self.shard_index,
            base = self.base_target_id,
            len = targets.len(),
            extension = self.config.compression.file_extension()
        );
        let path = self.config.shard_dir.join(file_name);
        let pubchem_id_map_path = pubchem_id_map_path_for_shard_path(&path);
        let started = Instant::now();
        let builder = PersistedTargetCorpusIndexShardBuilder::new(&targets)
            .base_target_id(self.base_target_id)
            .compression(self.config.compression);
        // SAFETY: this writes a trusted persisted-index payload that will be
        // consumed by this tool and compatible `smarts-rs` versions.
        let stats = unsafe {
            if self.options.verbose {
                builder.store_unchecked_with_indicatif_progress(
                    &path,
                    format!("{:06}", self.shard_index),
                )?
            } else {
                builder.store_unchecked(&path)?
            }
        };
        let pubchem_id_map_stats =
            store_pubchem_id_map(&pubchem_id_map_path, &pubchem_ids, self.config.compression)?;
        eprintln!(
            "stored shard {} at {} in {:?}: {} targets, {} index bytes and {} CID-map bytes on disk",
            self.shard_index,
            path.display(),
            started.elapsed(),
            stats.target_count,
            stats.store_stats.disk_bytes,
            pubchem_id_map_stats.disk_bytes
        );
        drop(targets);
        drop(pubchem_ids);

        self.shards.push(ShardRecord {
            path,
            pubchem_id_map_path,
            base_target_id: self.base_target_id,
            target_count: stats.target_count,
            disk_bytes: stats.store_stats.disk_bytes,
            raw_epserde_bytes: stats.store_stats.serialized_bytes,
            pubchem_id_map_disk_bytes: pubchem_id_map_stats.disk_bytes,
            pubchem_id_map_raw_bytes: pubchem_id_map_stats.raw_bytes,
            compression: self.config.compression.to_string(),
        });
        self.base_target_id += stats.target_count;
        self.shard_index += 1;
        Ok(())
    }
}
