use std::{str::FromStr, time::Instant};

use smarts_rs::{
    screening::persisted::PersistedTargetCorpusIndexShardPaths, QueryMol, QueryScreen,
};

use crate::{
    cid_map::{extract_pubchem_id_maps_for_compressed_shards, PubChemIdLookup},
    config::PubChemIndexConfig,
    errors::{invalid_input, DynError},
};

pub(crate) fn query_smarts(config: &PubChemIndexConfig, smarts: &str) -> Result<(), DynError> {
    let query = QueryMol::from_str(smarts)?;
    let screen = QueryScreen::new(&query);
    let inputs = extracted_raw_shards(config)?;
    let started = Instant::now();
    // SAFETY: query input files are local shards produced by the matching
    // `smarts-rs` persisted-index writer. Epserde mmap loading requires
    // trusted payloads from the same format/version family.
    let candidates = unsafe { inputs.shards.par_candidate_ids_unchecked(&screen)? };
    let lookup = PubChemIdLookup::from_shards(&inputs.shards)?;

    lookup.write_candidate_rows(&candidates)?;
    eprintln!(
        "screened SMARTS {smarts:?} across {} shards in {:?}: {} candidate target ids",
        inputs.shards.len(),
        started.elapsed(),
        candidates.len()
    );
    Ok(())
}

struct PreparedShardInputs {
    shards: PersistedTargetCorpusIndexShardPaths,
}

fn extracted_raw_shards(config: &PubChemIndexConfig) -> Result<PreparedShardInputs, DynError> {
    if !config.shard_dir.exists() {
        return Err(invalid_input(format!(
            "shard directory does not exist: {}",
            config.shard_dir.display()
        ))
        .into());
    }

    let compressed = PersistedTargetCorpusIndexShardPaths::zstd_in_dir(&config.shard_dir)?;
    if !compressed.is_empty() {
        let extracted = compressed.extract_zstd_if_missing_in_parallel()?;
        let created = extracted.iter().filter(|report| report.extracted).count();
        eprintln!(
            "prepared {} raw shards from {} compressed shards; extracted {} new files",
            compressed.len(),
            compressed.len(),
            created
        );
        let raw_shards = compressed.extracted_raw_paths();
        extract_pubchem_id_maps_for_compressed_shards(&compressed)?;
        return Ok(PreparedShardInputs { shards: raw_shards });
    }

    let raw = PersistedTargetCorpusIndexShardPaths::raw_in_dir(&config.shard_dir)?;
    if raw.is_empty() {
        return Err(invalid_input(format!(
            "no .eps or .eps.zst shards found in {}",
            config.shard_dir.display()
        ))
        .into());
    }
    Ok(PreparedShardInputs { shards: raw })
}
