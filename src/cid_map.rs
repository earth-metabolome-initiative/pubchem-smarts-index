use std::{
    fs::{self, File},
    io::{BufWriter, Error as IoError, Write},
    path::{Path, PathBuf},
};

use rayon::prelude::*;
use smarts_rs::screening::persisted::{
    PersistedShardCompression, PersistedTargetCorpusIndexShardPaths,
};

use crate::errors::{invalid_data, DynError};

const PUBCHEM_ID_MAP_SUFFIX: &str = ".pubchem-cids.u32le";
const ZSTD_EXTENSION: &str = ".zst";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PubChemIdMapStoreStats {
    pub(crate) raw_bytes: usize,
    pub(crate) disk_bytes: u64,
}

pub(crate) fn store_pubchem_id_map(
    path: &Path,
    pubchem_ids: &[u32],
    compression: PersistedShardCompression,
) -> Result<PubChemIdMapStoreStats, DynError> {
    let raw_bytes = pubchem_ids
        .len()
        .checked_mul(size_of::<u32>())
        .ok_or_else(|| invalid_data("PubChem CID map is too large"))?;
    match compression {
        PersistedShardCompression::None => {
            let mut writer = BufWriter::new(File::create(path)?);
            write_u32_le_values(&mut writer, pubchem_ids)?;
            writer.flush()?;
        }
        PersistedShardCompression::Zstd {
            level,
            worker_threads,
        } => {
            let writer = BufWriter::new(File::create(path)?);
            let mut encoder = zstd::stream::write::Encoder::new(writer, level)?;
            if worker_threads > 1 {
                encoder.multithread(worker_threads)?;
            }
            write_u32_le_values(&mut encoder, pubchem_ids)?;
            let mut writer = encoder.finish()?;
            writer.flush()?;
        }
    }
    Ok(PubChemIdMapStoreStats {
        raw_bytes,
        disk_bytes: fs::metadata(path)?.len(),
    })
}

fn write_u32_le_values(writer: &mut impl Write, values: &[u32]) -> Result<(), IoError> {
    let mut buffer = Vec::with_capacity(8192 * size_of::<u32>());
    for chunk in values.chunks(8192) {
        buffer.clear();
        for &value in chunk {
            buffer.extend_from_slice(&value.to_le_bytes());
        }
        writer.write_all(&buffer)?;
    }
    Ok(())
}

fn read_pubchem_id_map(path: &Path) -> Result<Vec<u32>, IoError> {
    let bytes = fs::read(path)?;
    let mut chunks = bytes.chunks_exact(size_of::<u32>());
    let ids = chunks
        .by_ref()
        .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect::<Vec<_>>();
    if !chunks.remainder().is_empty() {
        return Err(invalid_data(format!(
            "PubChem CID map has a trailing partial u32: {}",
            path.display()
        )));
    }
    Ok(ids)
}

pub(crate) struct PubChemIdLookup {
    shards: Vec<PubChemIdShard>,
}

impl PubChemIdLookup {
    pub(crate) fn from_shards(
        shards: &PersistedTargetCorpusIndexShardPaths,
    ) -> Result<Self, DynError> {
        let mut records = shards
            .paths()
            .iter()
            .map(|path| PubChemIdShard::from_shard_path(path))
            .collect::<Result<Vec<_>, _>>()?;
        records.sort_by_key(|record| record.base_target_id);
        validate_pubchem_id_shards(&records)?;
        Ok(Self { shards: records })
    }

    pub(crate) fn write_candidate_rows(&self, candidate_ids: &[usize]) -> Result<(), DynError> {
        let mut active_shard_index = None;
        let mut active_pubchem_ids = Vec::new();

        for &target_id in candidate_ids {
            let shard_index = self.shard_index_for_target(target_id).ok_or_else(|| {
                invalid_data(format!(
                    "candidate target id {target_id} is outside the available PubChem CID maps"
                ))
            })?;
            if active_shard_index != Some(shard_index) {
                active_pubchem_ids = read_pubchem_id_map(&self.shards[shard_index].id_map_path)?;
                self.shards[shard_index].validate_ids(&active_pubchem_ids)?;
                active_shard_index = Some(shard_index);
            }
            let local_target_id = target_id - self.shards[shard_index].base_target_id;
            let pubchem_id = active_pubchem_ids[local_target_id];
            println!("{target_id}\t{pubchem_id}");
        }
        Ok(())
    }

    fn shard_index_for_target(&self, target_id: usize) -> Option<usize> {
        self.shards
            .binary_search_by(|shard| {
                if target_id < shard.base_target_id {
                    std::cmp::Ordering::Greater
                } else if target_id >= shard.end_target_id {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .ok()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PubChemIdShard {
    base_target_id: usize,
    end_target_id: usize,
    target_count: usize,
    id_map_path: PathBuf,
}

impl PubChemIdShard {
    fn from_shard_path(path: &Path) -> Result<Self, IoError> {
        let (base_target_id, target_count) = parse_shard_base_and_len(path)?;
        let end_target_id = base_target_id
            .checked_add(target_count)
            .ok_or_else(|| invalid_data(format!("target id overflow in {}", path.display())))?;
        let id_map_path = pubchem_id_map_raw_path_for_shard_path(path);
        if !id_map_path.is_file() {
            return Err(invalid_data(format!(
                "missing PubChem CID map for shard {}: expected {}",
                path.display(),
                id_map_path.display()
            )));
        }
        Ok(Self {
            base_target_id,
            end_target_id,
            target_count,
            id_map_path,
        })
    }

    fn validate_ids(&self, pubchem_ids: &[u32]) -> Result<(), IoError> {
        if pubchem_ids.len() == self.target_count {
            return Ok(());
        }
        Err(invalid_data(format!(
            "PubChem CID map {} has {} ids, expected {}",
            self.id_map_path.display(),
            pubchem_ids.len(),
            self.target_count
        )))
    }
}

fn validate_pubchem_id_shards(shards: &[PubChemIdShard]) -> Result<(), IoError> {
    let mut expected_base_target_id = 0usize;
    for shard in shards {
        if shard.base_target_id < expected_base_target_id {
            return Err(invalid_data(format!(
                "overlapping PubChem CID maps around target id {}",
                shard.base_target_id
            )));
        }
        if shard.base_target_id > expected_base_target_id {
            return Err(invalid_data(format!(
                "missing PubChem CID map range starting at target id {expected_base_target_id}; next shard starts at {}",
                shard.base_target_id
            )));
        }
        expected_base_target_id = shard.end_target_id;
    }
    Ok(())
}

fn parse_shard_base_and_len(path: &Path) -> Result<(usize, usize), IoError> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid_data(format!("invalid shard path: {}", path.display())))?;
    let stem = file_name
        .strip_suffix(".eps")
        .or_else(|| file_name.strip_suffix(".eps.zst"))
        .ok_or_else(|| invalid_data(format!("invalid shard extension: {file_name}")))?;
    let fields = stem.split('-').collect::<Vec<_>>();
    let base_position = fields
        .iter()
        .position(|field| *field == "base")
        .ok_or_else(|| invalid_data(format!("missing shard base in {file_name}")))?;
    let len_position = fields
        .iter()
        .position(|field| *field == "len")
        .ok_or_else(|| invalid_data(format!("missing shard length in {file_name}")))?;
    let base = parse_shard_usize_field(&fields, base_position + 1, file_name, "base")?;
    let len = parse_shard_usize_field(&fields, len_position + 1, file_name, "len")?;
    Ok((base, len))
}

fn parse_shard_usize_field(
    fields: &[&str],
    index: usize,
    file_name: &str,
    field_name: &str,
) -> Result<usize, IoError> {
    let value = fields
        .get(index)
        .ok_or_else(|| invalid_data(format!("missing shard {field_name} value in {file_name}")))?;
    value.parse::<usize>().map_err(|error| {
        invalid_data(format!(
            "invalid shard {field_name} value in {file_name}: {error}"
        ))
    })
}

pub(crate) fn pubchem_id_map_path_for_shard_path(path: &Path) -> PathBuf {
    let raw = pubchem_id_map_raw_path_for_shard_path(path);
    if path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            Path::new(name)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("zst"))
        })
    {
        raw.with_file_name(format!(
            "{}{}",
            raw.file_name()
                .map_or_else(String::new, |name| name.to_string_lossy().into_owned()),
            ZSTD_EXTENSION
        ))
    } else {
        raw
    }
}

fn pubchem_id_map_raw_path_for_shard_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let stem = file_name
        .strip_suffix(".eps")
        .or_else(|| file_name.strip_suffix(".eps.zst"))
        .unwrap_or(file_name);
    path.with_file_name(format!("{stem}{PUBCHEM_ID_MAP_SUFFIX}"))
}

pub(crate) fn extract_pubchem_id_maps_for_compressed_shards(
    compressed_shards: &PersistedTargetCorpusIndexShardPaths,
) -> Result<(), DynError> {
    compressed_shards
        .paths()
        .par_iter()
        .map(|path| extract_pubchem_id_map_if_missing(&pubchem_id_map_path_for_shard_path(path)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(())
}

fn extract_pubchem_id_map_if_missing(path: &Path) -> Result<(), IoError> {
    let destination = raw_path_for_zstd_path(path)?;
    if destination.exists() {
        return Ok(());
    }
    if !path.is_file() {
        return Err(invalid_data(format!(
            "missing compressed PubChem CID map: {}",
            path.display()
        )));
    }
    let mut decoder = zstd::stream::read::Decoder::new(File::open(path)?)?;
    let mut output = BufWriter::new(File::create(destination)?);
    std::io::copy(&mut decoder, &mut output)?;
    output.flush()
}

fn raw_path_for_zstd_path(path: &Path) -> Result<PathBuf, IoError> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid_data(format!("invalid zstd path: {}", path.display())))?;
    let raw_name = file_name.strip_suffix(ZSTD_EXTENSION).ok_or_else(|| {
        invalid_data(format!(
            "expected zstd-compressed path ending in {ZSTD_EXTENSION}: {file_name}"
        ))
    })?;
    Ok(path.with_file_name(raw_name))
}

#[cfg(test)]
mod tests {
    use super::{
        parse_shard_base_and_len, pubchem_id_map_path_for_shard_path,
        pubchem_id_map_raw_path_for_shard_path, read_pubchem_id_map, store_pubchem_id_map,
        validate_pubchem_id_shards, PubChemIdShard,
    };
    use crate::errors::DynError;
    use smarts_rs::screening::persisted::PersistedShardCompression;
    use std::{
        fs,
        path::{Path, PathBuf},
    };

    fn pubchem_id_shard(base_target_id: usize, target_count: usize) -> PubChemIdShard {
        PubChemIdShard {
            base_target_id,
            end_target_id: base_target_id + target_count,
            target_count,
            id_map_path: PathBuf::from(format!("shard-{base_target_id}.pubchem-cids.u32le")),
        }
    }

    #[test]
    fn derives_pubchem_id_map_paths_from_index_shards() {
        let compressed = Path::new("target-index-shard-000007-base-105-len-11.eps.zst");
        let raw = Path::new("target-index-shard-000007-base-105-len-11.eps");

        assert_eq!(
            pubchem_id_map_path_for_shard_path(compressed),
            PathBuf::from("target-index-shard-000007-base-105-len-11.pubchem-cids.u32le.zst")
        );
        assert_eq!(
            pubchem_id_map_raw_path_for_shard_path(compressed),
            PathBuf::from("target-index-shard-000007-base-105-len-11.pubchem-cids.u32le")
        );
        assert_eq!(
            pubchem_id_map_path_for_shard_path(raw),
            PathBuf::from("target-index-shard-000007-base-105-len-11.pubchem-cids.u32le")
        );
    }

    #[test]
    fn parses_generated_shard_ranges() -> Result<(), DynError> {
        let path = Path::new("target-index-shard-000007-base-105-len-11.eps");
        let range = parse_shard_base_and_len(path)?;

        assert_eq!(range, (105, 11));
        Ok(())
    }

    #[test]
    fn validates_pubchem_id_shard_ranges_are_contiguous() {
        assert!(validate_pubchem_id_shards(&[
            pubchem_id_shard(0, 2),
            pubchem_id_shard(2, 3),
            pubchem_id_shard(5, 1),
        ])
        .is_ok());

        assert!(validate_pubchem_id_shards(&[pubchem_id_shard(2, 3)]).is_err());
        assert!(
            validate_pubchem_id_shards(&[pubchem_id_shard(0, 2), pubchem_id_shard(3, 1),]).is_err()
        );
        assert!(
            validate_pubchem_id_shards(&[pubchem_id_shard(0, 3), pubchem_id_shard(2, 1),]).is_err()
        );
    }

    #[test]
    fn pubchem_id_map_round_trips_as_u32le() -> Result<(), DynError> {
        let path = std::env::temp_dir().join(format!(
            "pubchem-smarts-index-cids-{}.u32le",
            std::process::id()
        ));
        let ids = [1, 2244, 123_456_789];

        let stats = store_pubchem_id_map(&path, &ids, PersistedShardCompression::None)?;
        let loaded = read_pubchem_id_map(&path)?;

        assert_eq!(stats.raw_bytes, ids.len() * size_of::<u32>());
        assert_eq!(loaded, ids);
        fs::remove_file(path)?;
        Ok(())
    }
}
