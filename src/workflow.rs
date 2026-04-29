use std::time::Instant;

use crate::{
    build::build_shards, config::PubChemIndexConfig, errors::DynError, manifest::write_manifest,
    publisher::ZenodoPublisher, query::query_smarts,
};

#[derive(Clone, Debug)]
pub(crate) struct PubChemIndex {
    config: PubChemIndexConfig,
}

impl PubChemIndex {
    pub(crate) fn new() -> Self {
        Self {
            config: PubChemIndexConfig::new(),
        }
    }

    pub(crate) fn build_and_publish(&self) -> Result<(), DynError> {
        let publisher = ZenodoPublisher::from_env()?;
        let started = Instant::now();
        let report = build_shards(&self.config)?;
        let manifest_path = write_manifest(&self.config, &report)?;
        eprintln!(
            "built {} PubChem SMARTS index shards for {} targets in {:?}",
            report.shards.len(),
            report.target_count(),
            started.elapsed()
        );
        publisher.publish(&report, &manifest_path)
    }

    pub(crate) fn query_smarts(&self, smarts: &str) -> Result<(), DynError> {
        query_smarts(&self.config, smarts)
    }
}
