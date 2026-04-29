use std::path::Path;

use chrono::Utc;
use zenodo_rs::{
    AccessRight, Auth, Creator, DepositMetadataUpdate, UploadSpec, UploadType, ZenodoClient,
};

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
        let metadata = Self::metadata(report)?;
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

    fn metadata(report: &BuildReport) -> Result<DepositMetadataUpdate, DynError> {
        Self::metadata_with_version(report, publication_version())
    }

    fn metadata_with_version(
        report: &BuildReport,
        version: String,
    ) -> Result<DepositMetadataUpdate, DynError> {
        Ok(DepositMetadataUpdate::builder()
            .title("PubChem SMARTS target-index shards")
            .upload_type(UploadType::Dataset)
            .description_html(Self::description(report))
            .creator(
                Creator::builder()
                    .name("Luca Cappelletti")
                    .orcid("0000-0002-1269-2038")
                    .build()?,
            )
            .access_right(AccessRight::Open)
            .license("cc-by-4.0")
            .keyword("PubChem")
            .keyword("SMARTS")
            .keyword("SMILES")
            .keyword("smarts-rs")
            .version(version)
            .community_identifier("earth-metabolome")
            .build()?)
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

fn publication_version() -> String {
    Utc::now().format("%Y-%m-%d").to_string()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{
        errors::DynError,
        manifest::{BuildReport, ShardRecord},
        publisher::ZenodoPublisher,
    };

    fn report() -> BuildReport {
        BuildReport {
            shards: vec![ShardRecord {
                path: PathBuf::from("target-index-shard-000000-base-0-len-1.eps.zst"),
                pubchem_id_map_path: PathBuf::from(
                    "target-index-shard-000000-base-0-len-1.pubchem-cids.u32le.zst",
                ),
                base_target_id: 0,
                target_count: 1,
                disk_bytes: 12,
                raw_epserde_bytes: 24,
                pubchem_id_map_disk_bytes: 4,
                pubchem_id_map_raw_bytes: 4,
                compression: "zstd".to_owned(),
            }],
        }
    }

    #[test]
    fn zenodo_metadata_uses_personal_creator_with_orcid() -> Result<(), DynError> {
        let metadata = ZenodoPublisher::metadata_with_version(&report(), "2026-04-29".to_owned())?;

        assert_eq!(metadata.creators.len(), 1);
        assert_eq!(metadata.creators[0].name, "Luca Cappelletti");
        assert_eq!(
            metadata.creators[0].orcid.as_deref(),
            Some("0000-0002-1269-2038")
        );
        assert_eq!(metadata.version.as_deref(), Some("2026-04-29"));
        assert_eq!(metadata.communities.len(), 1);
        assert_eq!(metadata.communities[0].identifier, "earth-metabolome");
        Ok(())
    }
}
