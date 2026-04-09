use std::ops::RangeInclusive;

use alloy::primitives::BlockHash;
use datasets_common::{block_num::BlockNum, network_id::NetworkId};
use metadata_db::files::FileId;
use object_store::ObjectMeta;
use rand::{
    Rng as _, RngCore as _, SeedableRng as _,
    rngs::StdRng,
    seq::{IndexedRandom, SliceRandom},
};

use super::{BlockRange, Chain, Segment, canonical_chain, fork_chain, load_arbitrum_fixture};

fn test_hash(number: u8, fork: u8) -> BlockHash {
    let mut hash: BlockHash = Default::default();
    hash.0[0] = number;
    hash.0[31] = fork;
    hash
}

fn test_range(network: &str, numbers: RangeInclusive<BlockNum>, fork: (u8, u8)) -> BlockRange {
    BlockRange {
        numbers: numbers.clone(),
        network: network.parse().expect("valid network id"),
        hash: test_hash(*numbers.end() as u8, fork.1),
        prev_hash: if *numbers.start() == 0 {
            Default::default()
        } else {
            test_hash(*numbers.start() as u8 - 1, fork.0)
        },
        timestamp: None,
    }
}

fn test_object() -> ObjectMeta {
    ObjectMeta {
        location: Default::default(),
        last_modified: Default::default(),
        size: 0,
        e_tag: None,
        version: None,
    }
}

fn test_segment(numbers: RangeInclusive<BlockNum>, fork: (u8, u8)) -> Segment {
    let range = test_range("test", numbers.clone(), fork);
    Segment::new(
        FileId::try_from(1i64).expect("FileId::MIN is 1"),
        test_object(),
        vec![range],
    )
}

fn test_segment_multi(
    network_a: (RangeInclusive<BlockNum>, (u8, u8)),
    network_b: (RangeInclusive<BlockNum>, (u8, u8)),
) -> Segment {
    Segment::new(
        FileId::try_from(1i64).expect("FileId::MIN is 1"),
        test_object(),
        vec![
            test_range("a", network_a.0, network_a.1),
            test_range("b", network_b.0, network_b.1),
        ],
    )
}

fn check(segments: Vec<Segment>, expected_canonical: Option<Chain>, expected_fork: Option<Chain>) {
    let canonical = canonical_chain(&segments);
    let fork = canonical.as_ref().and_then(|c| fork_chain(c, &segments));
    if let Some(c) = &canonical {
        c.check_invariants();
    }
    if let Some(f) = &fork {
        f.check_invariants();
    }
    assert_eq!(canonical, expected_canonical);
    assert_eq!(fork, expected_fork);
}

/// Tests `canonical_chain` and `fork_chain` across representative scenarios.
///
/// Each block is an independent scenario: a comment names the case (acting as Given),
/// the function calls are the When, and the `assert_eq!` is the Then.
#[test]
fn chains_computes_canonical_and_fork() {
    // empty input
    check(vec![], None, None);

    // single segment
    check(
        vec![test_segment(0..=0, (0, 0))],
        Some(Chain(vec![test_segment(0..=0, (0, 0))])),
        None,
    );

    // 2 adjacent segments forming a canonical chain
    check(
        vec![test_segment(0..=2, (0, 0)), test_segment(3..=5, (0, 0))],
        Some(Chain(vec![
            test_segment(0..=2, (0, 0)),
            test_segment(3..=5, (0, 0)),
        ])),
        None,
    );

    // 3 adjacent segments forming a canonical chain
    check(
        vec![
            test_segment(0..=1, (0, 0)),
            test_segment(2..=3, (0, 0)),
            test_segment(4..=5, (0, 0)),
        ],
        Some(Chain(vec![
            test_segment(0..=1, (0, 0)),
            test_segment(2..=3, (0, 0)),
            test_segment(4..=5, (0, 0)),
        ])),
        None,
    );

    // non-adjacent segments
    check(
        vec![test_segment(0..=2, (0, 0)), test_segment(5..=7, (0, 0))],
        Some(Chain(vec![test_segment(0..=2, (0, 0))])),
        None,
    );

    // multiple non-adjacent segments
    check(
        vec![
            test_segment(5..=7, (0, 0)),
            test_segment(0..=2, (0, 0)),
            test_segment(10..=12, (0, 0)),
        ],
        Some(Chain(vec![test_segment(0..=2, (0, 0))])),
        None,
    );

    // overlapping segments outside canonical
    check(
        vec![
            test_segment(0..=2, (0, 0)),
            test_segment(3..=5, (0, 0)),
            test_segment(4..=5, (0, 0)),
            test_segment(4..=6, (0, 0)),
            test_segment(3..=6, (0, 0)),
        ],
        Some(Chain(vec![
            test_segment(0..=2, (0, 0)),
            test_segment(3..=6, (0, 0)),
        ])),
        None,
    );

    // simple fork at start block
    check(
        vec![
            test_segment(0..=2, (0, 0)),
            test_segment(3..=5, (0, 0)),
            test_segment(3..=6, (1, 0)),
        ],
        Some(Chain(vec![
            test_segment(0..=2, (0, 0)),
            test_segment(3..=5, (0, 0)),
        ])),
        Some(Chain(vec![test_segment(3..=6, (1, 0))])),
    );

    // reorg of multiple segments
    check(
        vec![
            test_segment(0..=2, (0, 0)),
            test_segment(3..=5, (0, 0)),
            test_segment(3..=5, (1, 1)),
            test_segment(6..=8, (1, 0)),
            test_segment(6..=8, (0, 0)),
        ],
        Some(Chain(vec![
            test_segment(0..=2, (0, 0)),
            test_segment(3..=5, (0, 0)),
            test_segment(6..=8, (0, 0)),
        ])),
        None,
    );

    // fork of multiple segments
    check(
        vec![
            test_segment(0..=2, (0, 0)),
            test_segment(3..=5, (0, 0)),
            test_segment(3..=5, (1, 1)),
            test_segment(6..=9, (1, 0)),
            test_segment(6..=8, (0, 0)),
        ],
        Some(Chain(vec![
            test_segment(0..=2, (0, 0)),
            test_segment(3..=5, (0, 0)),
            test_segment(6..=8, (0, 0)),
        ])),
        Some(Chain(vec![
            test_segment(3..=5, (1, 1)),
            test_segment(6..=9, (1, 0)),
        ])),
    );

    // multiple reorgs past canonical
    check(
        vec![
            test_segment(0..=3, (0, 0)),
            test_segment(4..=6, (1, 1)),
            test_segment(4..=8, (0, 0)),
            test_segment(7..=9, (1, 1)),
            test_segment(4..=9, (2, 2)),
        ],
        Some(Chain(vec![
            test_segment(0..=3, (0, 0)),
            test_segment(4..=8, (0, 0)),
        ])),
        Some(Chain(vec![test_segment(4..=9, (2, 2))])),
    );

    // multi-segment fork extending past canonical
    check(
        vec![
            test_segment(0..=3, (0, 0)),
            test_segment(4..=5, (1, 1)),
            test_segment(6..=7, (1, 1)),
            test_segment(8..=9, (1, 1)),
        ],
        Some(Chain(vec![test_segment(0..=3, (0, 0))])),
        Some(Chain(vec![
            test_segment(4..=5, (1, 1)),
            test_segment(6..=7, (1, 1)),
            test_segment(8..=9, (1, 1)),
        ])),
    );

    // canonical_chain maximizes end block, so it picks the longer path through
    // the post-reorg segments [6-10'] -> [11-15] over the shorter [6-10].
    check(
        vec![
            test_segment(0..=5, (0, 0)),
            test_segment(6..=10, (0, 0)),
            test_segment(3..=5, (0, 0)),
            test_segment(6..=10, (0, 1)),
            test_segment(11..=15, (1, 1)),
        ],
        Some(Chain(vec![
            test_segment(0..=5, (0, 0)),
            test_segment(6..=10, (0, 1)),
            test_segment(11..=15, (1, 1)),
        ])),
        None,
    );
}

/// Tests `canonical_chain` and `fork_chain` with multi-network segments.
#[test]
fn chains_multi_network_computes_canonical_and_fork() {
    // Case 1: Canonical chain with 2 networks
    check(
        vec![
            test_segment_multi((0..=2, (0, 0)), (0..=2, (0, 0))),
            test_segment_multi((3..=5, (0, 0)), (3..=5, (0, 0))),
        ],
        Some(Chain(vec![
            test_segment_multi((0..=2, (0, 0)), (0..=2, (0, 0))),
            test_segment_multi((3..=5, (0, 0)), (3..=5, (0, 0))),
        ])),
        None,
    );

    // Case 2: First network diverges to create a fork
    check(
        vec![
            test_segment_multi((0..=2, (0, 0)), (0..=2, (0, 0))),
            test_segment_multi((3..=5, (0, 0)), (3..=5, (0, 0))),
            test_segment_multi((3..=6, (0, 1)), (6..=7, (0, 0))),
        ],
        Some(Chain(vec![
            test_segment_multi((0..=2, (0, 0)), (0..=2, (0, 0))),
            test_segment_multi((3..=5, (0, 0)), (3..=5, (0, 0))),
        ])),
        Some(Chain(vec![test_segment_multi(
            (3..=6, (0, 1)),
            (6..=7, (0, 0)),
        )])),
    );

    // Case 3: canonical_chain maximizes end block, so it picks the longer path:
    //   [0..=2] -> [3..=6, 3..=7] -> [7..=8, 8..=9]
    check(
        vec![
            test_segment_multi((0..=2, (0, 0)), (0..=2, (0, 0))),
            test_segment_multi((3..=6, (0, 1)), (3..=7, (0, 1))),
            test_segment_multi((7..=8, (1, 1)), (8..=9, (1, 1))),
            test_segment_multi((3..=5, (0, 0)), (3..=5, (0, 0))),
        ],
        Some(Chain(vec![
            test_segment_multi((0..=2, (0, 0)), (0..=2, (0, 0))),
            test_segment_multi((3..=6, (0, 1)), (3..=7, (0, 1))),
            test_segment_multi((7..=8, (1, 1)), (8..=9, (1, 1))),
        ])),
        None,
    );
}

/// Tests `missing_ranges()` across gap and reorg scenarios.
///
/// Each `assert_eq!` is an independent case:
///   Given: a set of synced ranges + a desired range
///   When:  `missing_ranges(segments, desired)` is called
///   Then:  the returned gaps (and any reorg ranges) match expectations
#[test]
fn missing_ranges_includes_gaps_and_reorg_ranges() {
    fn missing_ranges(
        ranges: &[RangeInclusive<BlockNum>],
        desired: RangeInclusive<BlockNum>,
    ) -> Vec<RangeInclusive<BlockNum>> {
        let segments = ranges
            .iter()
            .enumerate()
            .map(|(fork, range)| test_segment(range.clone(), (fork as u8, fork as u8)))
            .collect::<Vec<_>>();
        super::missing_ranges(&segments, desired)
    }

    assert_eq!(missing_ranges(&[], 0..=10), vec![0..=10]);
    // just canonical ranges
    assert_eq!(missing_ranges(&[0..=10], 0..=10), vec![]);
    assert_eq!(missing_ranges(&[0..=5], 10..=15), vec![10..=15]);
    assert_eq!(missing_ranges(&[3..=7], 0..=10), vec![0..=2, 8..=10]);
    assert_eq!(missing_ranges(&[0..=15], 5..=10), vec![]);
    assert_eq!(missing_ranges(&[5..=15], 0..=10), vec![0..=4]);
    assert_eq!(missing_ranges(&[0..=5], 0..=10), vec![6..=10]);
    // non-overlapping segment groups
    assert_eq!(missing_ranges(&[0..=5, 8..=8], 0..=10), vec![6..=7, 9..=10]);
    assert_eq!(
        missing_ranges(&[1..=2, 4..=4, 8..=9], 0..=10),
        vec![0..=0, 3..=3, 5..=7, 10..=10],
    );
    // overlapping segment groups, no reorg
    assert_eq!(missing_ranges(&[0..=8, 2..=4], 0..=10), vec![9..=10]);
    assert_eq!(missing_ranges(&[0..=3, 5..=7], 0..=10), vec![4..=4, 8..=10]);
    // overlapping segment groups, reorg
    assert_eq!(
        missing_ranges(&[0..=3, 2..=5, 4..=7], 0..=10),
        vec![0..=3, 8..=10]
    );
    assert_eq!(missing_ranges(&[0..=2, 1..=3], 0..=3), vec![0..=0]);
    assert_eq!(missing_ranges(&[0..=2, 2..=3], 0..=2), vec![0..=1]);
    assert_eq!(
        missing_ranges(&[0..=3, 5..=7, 2..=12], 0..=15),
        vec![0..=1, 13..=15]
    );
    assert_eq!(missing_ranges(&[0..=5, 3..=8], 0..=7), vec![0..=2]);
    assert_eq!(missing_ranges(&[0..=20, 10..=30], 12..=18), vec![]);
    // reorg intersection with desired range
    assert_eq!(missing_ranges(&[0..=5, 3..=8], 2..=7), vec![2..=2]);
}

/// Property test: for any randomly generated canonical + reorg chains, `chains()` recovers
/// the canonical chain exactly and any returned fork satisfies connectivity invariants.
#[test]
fn chains_prop_canonical_and_fork_satisfy_invariants() {
    const MAX_BLOCK: BlockNum = 15;
    const MAX_FORK: u8 = 3;
    const SAMPLES: usize = 10_000;
    const MAX_REORGS: usize = 5;

    fn gen_chain(rng: &mut StdRng, numbers: RangeInclusive<BlockNum>, fork: u8) -> Chain {
        assert!(numbers.start() <= numbers.end());
        let mut segments: Vec<Segment> = Default::default();
        loop {
            let start = segments
                .last()
                .map(|s| s.ranges[0].end() + 1)
                .unwrap_or(*numbers.start());
            let end = rng.random_range(start..=*numbers.end());
            segments.push(test_segment(start..=end, (fork, fork)));
            if end == *numbers.end() {
                break;
            }
        }
        let chain = Chain(segments);
        chain.check_invariants();
        chain
    }

    for _ in 0..SAMPLES {
        //* Given
        let seed = rand::rng().next_u64();
        println!("seed: {seed}");
        let mut rng = StdRng::seed_from_u64(seed);

        let chain_head = rng.random_range(1..=MAX_BLOCK);
        let canonical = gen_chain(&mut rng, 0..=chain_head, 0);
        let other_chains: Vec<Chain> = (0..rng.random_range(0..MAX_REORGS))
            .map(|_| {
                let start = rng.random_range(1..=chain_head);
                let end = rng.random_range(start..=chain_head);
                let fork = rng.random_range(1..=MAX_FORK);
                gen_chain(&mut rng, start..=end, fork)
            })
            .collect();

        let mut segments = canonical.0.clone();
        for chain in &other_chains {
            segments.append(&mut chain.0.clone());
        }
        segments.shuffle(&mut rng);

        //* When
        let expected = canonical_chain(&segments)
            .expect("canonical_chain should return Some for non-empty segment set");
        let fork = fork_chain(&expected, &segments);

        //* Then
        assert_eq!(expected, canonical);
        if let Some(fork) = fork {
            assert!(fork.last_ranges()[0].end() > canonical.last_ranges()[0].end());
            assert!(fork.first_ranges()[0].start() > canonical.first_ranges()[0].start());
            assert!(fork.first_ranges()[0].start() <= canonical.last_ranges()[0].end() + 1);
        } else {
            assert!(
                other_chains
                    .iter()
                    .all(|c| c.last_ranges()[0].end() <= canonical.last_ranges()[0].end())
            );
        }
    }
}

/// Tests `merge_ranges()` across disjoint, adjacent, and overlapping inputs.
///
///   Given: an unordered list of block ranges (possibly overlapping or adjacent)
///   When:  `merge_ranges(ranges)` is called
///   Then:  the result is a sorted, non-overlapping, merged list of ranges
#[test]
fn merge_ranges_joins_overlapping_and_adjacent() {
    assert_eq!(super::merge_ranges(vec![]), vec![]);
    assert_eq!(super::merge_ranges(vec![1..=5]), vec![1..=5]);
    assert_eq!(
        super::merge_ranges(vec![1..=3, 5..=7, 9..=10]),
        vec![1..=3, 5..=7, 9..=10]
    );
    assert_eq!(super::merge_ranges(vec![1..=5, 3..=8]), vec![1..=8]);
    assert_eq!(super::merge_ranges(vec![1..=5, 5..=10]), vec![1..=10]);
    assert_eq!(super::merge_ranges(vec![1..=10, 3..=7]), vec![1..=10]);
    assert_eq!(
        super::merge_ranges(vec![1..=3, 2..=5, 4..=8, 7..=10]),
        vec![1..=10]
    );
    assert_eq!(
        super::merge_ranges(vec![10..=15, 1..=5, 3..=8]),
        vec![1..=8, 10..=15]
    );
    assert_eq!(
        super::merge_ranges(vec![1..=3, 2..=6, 8..=10, 9..=12, 15..=18]),
        vec![1..=6, 8..=12, 15..=18]
    );
    assert_eq!(
        super::merge_ranges(vec![5..=10, 5..=10, 5..=10]),
        vec![5..=10]
    );
    assert_eq!(super::merge_ranges(vec![1..=1, 2..=2, 3..=3]), vec![1..=3]);
    assert_eq!(
        super::merge_ranges(vec![1..=5, 7..=10]),
        vec![1..=5, 7..=10]
    );
}

#[test]
fn prop_canonical_chain_monotonically_increases_with_compaction() {
    fn hash(index: u8) -> BlockHash {
        let mut hash = BlockHash::default();
        hash[0] = index;
        hash
    }
    fn segment(
        numbers: RangeInclusive<BlockNum>,
        prev_hash: BlockHash,
        hash: BlockHash,
        timestamp: u8,
    ) -> Segment {
        let network = NetworkId::new_unchecked("test".into());
        let range = BlockRange {
            numbers,
            network,
            hash,
            prev_hash,
            timestamp: Some(timestamp as u64),
        };
        Segment::new(FileId::try_from(1_i64).unwrap(), test_object(), vec![range])
    }

    let seed = rand::rng().next_u64();
    println!("seed: {seed}");
    let mut rng = StdRng::seed_from_u64(seed);

    let mut index = 0;
    let mut segments = vec![segment(0..=0, BlockHash::default(), hash(index), index)];

    for _ in 0..100 {
        index += 1;
        let prev_canonical = canonical_chain(&segments).unwrap();
        match rng.random_range(0..2) {
            // add segment
            0 => {
                let parent = segments.choose(&mut rng).unwrap();
                let parent_range = &parent.ranges[0];
                let number = parent_range.end() + 1;
                let segment = segment(number..=number, parent_range.hash, hash(index), index);
                segments.push(segment);
            }
            // compact
            1 => {
                let chain = prev_canonical.clone();
                let start = rng.random_range(0..chain.0.len());
                let end = rng.random_range(start..chain.0.len());
                let compacted = segment(
                    *chain.0[start].ranges[0].numbers.start()
                        ..=*chain.0[end].ranges[0].numbers.end(),
                    chain.0[start].ranges[0].prev_hash,
                    chain.0[end].ranges[0].hash,
                    index,
                );
                segments.retain(|s| !chain.0[start..=end].contains(s));
                segments.push(compacted);
            }
            _ => unreachable!(),
        }
        let canonical = canonical_chain(&segments).unwrap();

        println!(" -- step {index} --");
        for s in &segments {
            let r = &s.ranges[0];
            println!(
                "  t: {} n: [{}-{}] h: [{}-{}]",
                r.timestamp.unwrap(),
                r.numbers.start(),
                r.numbers.end(),
                r.prev_hash[0],
                r.hash[0],
            );
        }
        println!(
            "canonical: [{}-{}]",
            canonical.first_segment().ranges[0].numbers.start(),
            canonical.last_segment().ranges[0].numbers.end(),
        );

        let block_height = |c: &Chain| -> u64 {
            let start = c.first_segment().ranges[0].numbers.start();
            let end = c.last_segment().ranges[0].numbers.end();
            end - start + 1
        };
        assert!(block_height(&prev_canonical) <= block_height(&canonical));
    }
}

/// Regression test: `missing_ranges` with real-world Arbitrum One segment data (52k segments).
/// This data represents some kind of GC failure where there are only 6 segments in the canonical
/// chain, and all the rest considered forks by the missing ranges algorithm.
/// This test exists to catch quadratic performance regressions — it should complete in seconds,
/// not minutes.
#[test]
fn missing_ranges_real_world_arbitrum() {
    let segments = load_arbitrum_fixture();

    let latest_block = segments
        .iter()
        .map(|s| *s.ranges()[0].numbers.end())
        .max()
        .unwrap();

    let start = std::time::Instant::now();
    let result = super::missing_ranges(&segments, 0..=latest_block);
    let elapsed = start.elapsed();

    eprintln!(
        "missing_ranges: {} segments, {} missing ranges, took {elapsed:?}",
        segments.len(),
        result.len()
    );
    assert!(
        elapsed.as_secs() < 30,
        "missing_ranges took {elapsed:?}, expected < 30s"
    );
}
