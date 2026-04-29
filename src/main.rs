#![doc = include_str!("../README.md")]

/// `PubChem` shard build workflow.
pub mod build;
/// `PubChem` CID sidecar storage and lookup.
pub mod cid_map;
/// Command-line dispatch.
pub mod cli;
/// Fixed production configuration.
pub mod config;
/// Shared error helpers.
pub mod errors;
/// Build reports and publication manifest writing.
pub mod manifest;
/// `PubChem` CID-SMILES parsing and target preparation.
pub mod pubchem;
/// Zenodo publication.
pub mod publisher;
/// SMARTS query workflow.
pub mod query;
/// Top-level build/query orchestration.
pub mod workflow;

use crate::errors::DynError;

fn main() -> Result<(), DynError> {
    let _ = dotenvy::dotenv();

    cli::run(std::env::args())
}
