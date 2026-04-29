use std::{
    fs::File,
    io::{BufRead, BufReader, Error as IoError},
    path::{Path, PathBuf},
    str::FromStr,
};

use flate2::read::GzDecoder;
use rayon::prelude::*;
use smarts_rs::PreparedTarget;
use smiles_parser::Smiles;

use crate::errors::{invalid_data, DynError};

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PubChemRecord {
    pub(crate) pubchem_id: u32,
    smiles: String,
}

pub(crate) struct PubChemCidSmilesIter {
    path: PathBuf,
    reader: Box<dyn BufRead + Send>,
    line_number: usize,
    line_buffer: String,
}

impl PubChemCidSmilesIter {
    pub(crate) fn open(path: &Path) -> Result<Self, IoError> {
        let file = File::open(path)?;
        let reader: Box<dyn BufRead + Send> =
            if path.extension().is_some_and(|extension| extension == "gz") {
                Box::new(BufReader::new(GzDecoder::new(file)))
            } else {
                Box::new(BufReader::new(file))
            };
        Ok(Self {
            path: path.to_path_buf(),
            reader,
            line_number: 0,
            line_buffer: String::new(),
        })
    }
}

impl Iterator for PubChemCidSmilesIter {
    type Item = Result<PubChemRecord, IoError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.line_buffer.clear();
            match self.reader.read_line(&mut self.line_buffer) {
                Ok(0) => return None,
                Ok(_) => {
                    self.line_number += 1;
                }
                Err(error) => return Some(Err(error)),
            }

            let line = self.line_buffer.trim_end_matches(['\r', '\n']);
            if line.is_empty() {
                continue;
            }
            return Some(
                parse_pubchem_record(self.line_number, line).map_err(|error| {
                    invalid_data(format!(
                        "{}:{}: {error}",
                        self.path.display(),
                        self.line_number
                    ))
                }),
            );
        }
    }
}

pub(crate) struct PreparedPubChemTarget {
    pub(crate) pubchem_id: u32,
    pub(crate) target: PreparedTarget,
}

pub(crate) fn prepare_targets(
    batch: &mut Vec<(usize, PubChemRecord)>,
) -> Result<Vec<PreparedPubChemTarget>, DynError> {
    if batch.is_empty() {
        return Ok(Vec::new());
    }
    let batch = std::mem::replace(batch, Vec::with_capacity(8192));
    batch
        .into_par_iter()
        .map(|(record_idx, record)| {
            let molecule = Smiles::from_str(&record.smiles).map_err(|error| {
                invalid_data(format!(
                    "PubChem SMILES record {} with CID {} failed to parse: {error}",
                    record_idx + 1,
                    record.pubchem_id
                ))
            })?;
            Ok(PreparedPubChemTarget {
                pubchem_id: record.pubchem_id,
                target: PreparedTarget::new(molecule),
            })
        })
        .collect()
}

fn parse_pubchem_record(line_number: usize, line: &str) -> Result<PubChemRecord, IoError> {
    let (pubchem_id, smiles) = line.split_once('\t').ok_or_else(|| {
        invalid_data(format!(
            "expected CID<TAB>SMILES record at line {line_number}"
        ))
    })?;
    if smiles.is_empty() {
        return Err(invalid_data(format!(
            "empty SMILES field at line {line_number}"
        )));
    }
    let pubchem_id = pubchem_id.parse::<u32>().map_err(|error| {
        invalid_data(format!(
            "invalid PubChem CID at line {line_number}: {error}"
        ))
    })?;
    Ok(PubChemRecord {
        pubchem_id,
        smiles: smiles.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::parse_pubchem_record;
    use crate::errors::DynError;

    #[test]
    fn parses_pubchem_cid_smiles_lines() -> Result<(), DynError> {
        let record = parse_pubchem_record(12, "2244\tCC(=O)OC1=CC=CC=C1C(=O)O")?;

        assert_eq!(record.pubchem_id, 2244);
        assert_eq!(record.smiles, "CC(=O)OC1=CC=CC=C1C(=O)O");
        assert!(parse_pubchem_record(13, "not-a-record").is_err());
        Ok(())
    }
}
