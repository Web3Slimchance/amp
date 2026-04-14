use alloy::primitives::BlockHash;
use common::physical_table::segments::{
    Segment, canonical_chain, load_arbitrum_fixture, missing_ranges,
};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use datasets_common::{block_num::BlockNum, block_range::BlockRange, network_id::NetworkId};
use metadata_db::files::FileId;
use object_store::ObjectMeta;
use rand::{Rng, RngExt as _, seq::SliceRandom};

fn random_hash(rng: &mut impl Rng) -> BlockHash {
    BlockHash::from(rng.random::<[u8; 32]>())
}

fn test_range(
    numbers: std::ops::RangeInclusive<BlockNum>,
    network: &NetworkId,
    hash: BlockHash,
    prev_hash: BlockHash,
) -> BlockRange {
    BlockRange {
        numbers,
        network: network.clone(),
        hash,
        prev_hash,
        timestamp: None,
    }
}

fn test_segment(range: BlockRange) -> Segment {
    let object = ObjectMeta {
        location: Default::default(),
        last_modified: Default::default(),
        size: 0,
        e_tag: None,
        version: None,
    };
    Segment::new(
        FileId::try_from(1i64).expect("FileId::MIN is 1"),
        object,
        vec![range],
    )
}

fn build_segments() -> Vec<Segment> {
    let canonical_len: u64 = 100_000;
    let segments_len = canonical_len + (canonical_len / 1000) + 2;
    let mut segments = Vec::with_capacity(segments_len as usize);

    let rng = &mut rand::rng();
    let network: NetworkId = "test".parse().expect("valid network id");

    let canonical_hashes: Vec<BlockHash> = (0..canonical_len).map(|_| random_hash(rng)).collect();

    for i in 0..canonical_len {
        let prev_hash = if i == 0 {
            Default::default()
        } else {
            canonical_hashes[(i - 1) as usize]
        };
        segments.push(test_segment(test_range(
            i..=i,
            &network,
            canonical_hashes[i as usize],
            prev_hash,
        )));

        // Add 1 fork segment every 1000 canonical segments
        if i > 0 && i % 1000 == 0 {
            segments.push(test_segment(test_range(
                i..=i,
                &network,
                random_hash(rng),
                canonical_hashes[(i - 1) as usize],
            )));
        }
    }

    // Add final fork of 2 segments extending beyond canonical chain
    let fork_start = canonical_len - 1;
    let fork_hash_0 = random_hash(rng);
    let fork_hash_1 = random_hash(rng);
    segments.push(test_segment(test_range(
        fork_start..=(fork_start + 1),
        &network,
        fork_hash_0,
        random_hash(rng), // unrelated prev_hash (fork diverges)
    )));
    segments.push(test_segment(test_range(
        (fork_start + 2)..=(fork_start + 3),
        &network,
        fork_hash_1,
        fork_hash_0,
    )));

    segments.shuffle(rng);

    segments
}

fn bench_chains(c: &mut Criterion) {
    c.bench_function("chains_100k_segments", |b| {
        let segments = build_segments();
        b.iter(|| {
            let chain = canonical_chain(&segments);
            black_box(chain);
        })
    });
}

fn bench_missing_ranges_arbitrum(c: &mut Criterion) {
    let segments = load_arbitrum_fixture();
    let latest_block = segments
        .iter()
        .map(|s| *s.ranges()[0].numbers.end())
        .max()
        .unwrap();
    let desired = 0..=latest_block;

    let mut group = c.benchmark_group("missing_ranges_arbitrum");
    group.sample_size(10);
    group.bench_function("52k_real_segments", |b| {
        b.iter(|| {
            let result = missing_ranges(black_box(&segments), black_box(desired.clone()));
            black_box(result);
        })
    });
    group.finish();
}

criterion_group!(benches, bench_chains, bench_missing_ranges_arbitrum);
criterion_main!(benches);
