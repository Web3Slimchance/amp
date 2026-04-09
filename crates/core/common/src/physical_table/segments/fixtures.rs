use std::io::Read as _;

use datasets_common::{block_num::BlockNum, block_range::BlockRange, network_id::NetworkId};
use metadata_db::files::FileId;
use object_store::ObjectMeta;

use super::{BlockHash, Segment};

/// Load 52k real-world Arbitrum One segments from the compressed fixture file.
/// Used by tests and benchmarks.
pub fn load_arbitrum_fixture() -> Vec<Segment> {
    #[derive(serde::Deserialize)]
    struct Record {
        start: BlockNum,
        end: BlockNum,
        hash: BlockHash,
        prev_hash: BlockHash,
    }

    let fixture_path: std::path::PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "benches",
        "fixtures",
        "arbitrum_one_segments.postcard.gz",
    ]
    .iter()
    .collect();
    let file = std::fs::File::open(&fixture_path)
        .unwrap_or_else(|e| panic!("failed to open fixture {}: {e}", fixture_path.display()));
    let mut decoder = flate2::read::GzDecoder::new(file);
    let mut bytes = Vec::new();
    decoder
        .read_to_end(&mut bytes)
        .expect("failed to decompress fixture");
    let records: Vec<Record> = postcard::from_bytes(&bytes).expect("valid postcard fixture");

    let network: NetworkId = "arbitrum-one".parse().expect("valid network id");
    let object = ObjectMeta {
        location: Default::default(),
        last_modified: Default::default(),
        size: 0,
        e_tag: None,
        version: None,
    };
    records
        .into_iter()
        .enumerate()
        .map(|(i, r)| {
            let range = BlockRange {
                numbers: r.start..=r.end,
                network: network.clone(),
                hash: r.hash,
                prev_hash: r.prev_hash,
                timestamp: None,
            };
            Segment::new(
                FileId::try_from((i + 1) as i64).expect("valid file id"),
                object.clone(),
                vec![range],
            )
        })
        .collect()
}
