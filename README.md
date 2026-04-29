# pubchem-smarts-index

[![CI](https://github.com/earth-metabolome-initiative/pubchem-smarts-index/actions/workflows/ci.yml/badge.svg)](https://github.com/earth-metabolome-initiative/pubchem-smarts-index/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/license/mit)

Build compressed `smarts-rs` target-index shards for `PubChem` and query them.

Create `.env` from `.env.example` and set `ZENODO_TOKEN`. Then build the full
`PubChem` index as 15M-target zstd shards under `data/shards`, store one
compressed `PubChem` CID map per shard, and publish the shards plus manifest to
Zenodo:

```sh
cargo run --release -- build
```

Use `--verbose` to show per-shard `indicatif` progress while `smarts-rs` builds
and stores each persisted shard:

```sh
cargo run --release -- build --verbose
```

Query a SMARTS pattern against the sharded index:

```sh
cargo run --release -- query '[#6]=[#8]'
```

`query` extracts `.eps.zst` shards and `.pubchem-cids.u32le.zst` maps to
adjacent raw files in parallel if needed, then runs the mmap-backed shard query
in parallel. It prints `target_id<TAB>pubchem_cid` rows to stdout.
