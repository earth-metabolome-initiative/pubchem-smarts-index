use std::path::Path;

use zenodo_rs::{AccessRight, Auth, DepositMetadataUpdate, UploadSpec, UploadType, ZenodoClient};

use crate::{
    errors::{invalid_input, DynError},
    manifest::BuildReport,
};

#[derive(Clone, Debug)]
pub(crate) struct ZenodoPublisher {
    auth: Auth,
}

impl ZenodoPublisher {
    pub(crate) fn from_env() -> Result<Self, DynError> {
        Ok(Self {
            auth: Auth::from_env().map_err(|_| {
                invalid_input("ZENODO_TOKEN is required; create .env from .env.example")
            })?,
        })
    }

    pub(crate) fn publish(
        &self,
        report: &BuildReport,
        manifest_path: &Path,
    ) -> Result<(), DynError> {
        let metadata = DepositMetadataUpdate::builder()
            .title("PubChem SMARTS target-index shards")
            .upload_type(UploadType::Dataset)
            .description_html(Self::description(report))
            .creator_named("Earth Metabolome Initiative")
            .access_right(AccessRight::Open)
            .license("cc-by-4.0")
            .keyword("PubChem")
            .keyword("SMARTS")
            .keyword("SMILES")
            .keyword("smarts-rs")
            .build()?;
        let mut uploads = report
            .upload_paths()
            .map(UploadSpec::from_path)
            .collect::<Result<Vec<_>, _>>()?;
        uploads.push(UploadSpec::from_path(manifest_path)?);

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let published = runtime.block_on(async {
            let client = ZenodoClient::new(self.auth.clone())?;
            client.create_and_publish_dataset(&metadata, uploads).await
        })?;
        eprintln!(
            "published PubChem SMARTS index shards to Zenodo record {}",
            published.record.id.0
        );
        Ok(())
    }

    fn description(report: &BuildReport) -> String {
        format!(
            "<p>Persisted smarts-rs target-index shards built from the PubChem CID-SMILES corpus.</p>\
             <p>The archive contains {shards} zstd-compressed epserde shard files covering {targets} target records, \
             one zstd-compressed PubChem CID map per shard, and a JSON manifest. \
             Decompress the .eps.zst and .pubchem-cids.u32le.zst files before mmap-backed querying.</p>",
            shards = report.shards.len(),
            targets = report.target_count()
        )
    }
}
