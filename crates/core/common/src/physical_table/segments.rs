use std::{collections::HashMap, ops::RangeInclusive};

use datasets_common::{block_num::BlockNum, block_range::BlockRange};
use metadata_db::files::FileId;
use object_store::ObjectMeta;

use crate::cursor::Watermark;

/// A block range associated with the metadata from a file in object storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    id: FileId,
    object: ObjectMeta,
    ranges: Vec<BlockRange>,
    /// For single-network segments, this is `None` as the watermark equals the end block number.
    /// For multi-network segments, this represents the cumulative watermark at the end of
    /// this segment.
    watermark: Option<Watermark>,
}

impl Segment {
    /// Returns a Segment, where the ranges are ordered by network.
    pub fn new(
        id: FileId,
        object: ObjectMeta,
        mut ranges: Vec<BlockRange>,
        watermark: Option<Watermark>,
    ) -> Self {
        ranges.sort_unstable_by(|a, b| a.network.cmp(&b.network));
        Self {
            id,
            object,
            ranges,
            watermark,
        }
    }

    /// Returns the file ID of this segment.
    pub fn id(&self) -> FileId {
        self.id
    }

    /// Returns a reference to the object metadata.
    pub fn object(&self) -> &ObjectMeta {
        &self.object
    }

    /// Returns a slice of all block ranges in this segment.
    pub fn ranges(&self) -> &[BlockRange] {
        &self.ranges
    }

    /// Returns the watermark for this segment.
    pub fn watermark(&self) -> Watermark {
        match self.watermark {
            Some(watermark) => watermark,
            None => self.single_range().end(),
        }
    }

    /// Returns the single block range for single-network segments.
    ///
    /// # Panics
    /// Panics if the segment does not have exactly one range. This is a temporary
    /// method to support the transition to multi-network streaming. Code outside this module
    /// should use this method when it can assume single-network segments. For now, it is safe to
    /// assume this because we cannot stream out multi-network segments, and therefore they cannot
    /// be materialized.
    pub fn single_range(&self) -> &BlockRange {
        assert_eq!(self.ranges.len(), 1);
        &self.ranges[0]
    }

    pub fn adjacent(&self, other: &Self) -> bool {
        self.ranges.len() == other.ranges.len()
            && std::iter::zip(&self.ranges, &other.ranges).all(|(a, b)| a.adjacent(b))
    }

    /// Lexicographic comparison of segments by start block numbers.
    ///
    /// Compares ranges in order: if the first network's starts differ, return that ordering.
    /// Otherwise, compare the second network's starts, and so on.
    fn cmp_by_start(&self, other: &Self) -> std::cmp::Ordering {
        for (ra, rb) in std::iter::zip(&self.ranges, &other.ranges) {
            match ra.start().cmp(&rb.start()) {
                std::cmp::Ordering::Equal => continue,
                ordering => return ordering,
            }
        }
        std::cmp::Ordering::Equal
    }

    /// Lexicographic comparison of segments by end block numbers.
    ///
    /// Compares ranges in order: if the first network's ends differ, return that ordering.
    /// Otherwise, compare the second network's ends, and so on.
    fn cmp_by_end(&self, other: &Self) -> std::cmp::Ordering {
        for (ra, rb) in std::iter::zip(&self.ranges, &other.ranges) {
            match ra.end().cmp(&rb.end()) {
                std::cmp::Ordering::Equal => continue,
                ordering => return ordering,
            }
        }
        std::cmp::Ordering::Equal
    }
}

/// A sequence of adjacent segments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chain(pub Vec<Segment>);

impl Chain {
    #[cfg(test)]
    fn check_invariants(&self) {
        assert!(!self.0.is_empty());
        for segments in self.0.windows(2) {
            assert!(segments[0].adjacent(&segments[1]));
        }
    }

    /// Return all ranges from the first segment.
    pub fn first_ranges(&self) -> &[BlockRange] {
        &self.0.first().unwrap().ranges
    }

    /// Return all ranges from the last segment.
    pub fn last_ranges(&self) -> &[BlockRange] {
        &self.0.last().unwrap().ranges
    }

    /// Return the first segment in the chain.
    pub fn first_segment(&self) -> &Segment {
        self.0.first().unwrap()
    }

    /// Return the last segment in the chain.
    pub fn last_segment(&self) -> &Segment {
        self.0.last().unwrap()
    }

    /// Number of networks (consistent across all segments in the chain).
    pub fn network_count(&self) -> usize {
        self.first_ranges().len()
    }

    /// Return the overall block range of this chain (single-network only).
    pub fn range(&self) -> BlockRange {
        let first = &self.first_ranges()[0];
        let last = &self.last_ranges()[0];
        BlockRange {
            numbers: first.start()..=last.end(),
            network: first.network.clone(),
            hash: last.hash,
            prev_hash: first.prev_hash,
            timestamp: last.timestamp,
        }
    }
}

impl IntoIterator for Chain {
    type Item = Segment;
    type IntoIter = std::vec::IntoIter<Segment>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Returns the canonical chain of segments.
///
/// The canonical chain starts from the earliest available block and extends to the greatest block
/// height reachable through adjacent segments. Adjacency is determined by matching block hashes:
/// a segment's `prev_hash` must equal its predecessor's `hash`. When multiple chains reach the
/// same end block, the one with fewer segments (more compacted) is preferred.
pub fn canonical_chain(segments: &[Segment]) -> Option<Chain> {
    if segments.is_empty() {
        return None;
    }

    // Index segments by their end hashes so we can find predecessors via prev_hash lookup in O(1).
    let mut hash_index: HashMap<Vec<_>, Vec<usize>> = Default::default();
    for (idx, seg) in segments.iter().enumerate() {
        let hashes: Vec<_> = seg.ranges.iter().map(|r| r.hash).collect();
        hash_index.entry(hashes).or_default().push(idx);
    }

    // For each segment, track the best chain ending there: (chain_length, predecessor).
    // Processing in start-block order ensures predecessors are resolved before successors.
    let mut dp: Vec<Option<(usize, Option<usize>)>> = vec![None; segments.len()];

    let mut order: Vec<usize> = (0..segments.len()).collect();
    order.sort_unstable_by(|&a, &b| segments[a].cmp_by_start(&segments[b]));

    let earliest = &segments[order[0]];

    for idx in order {
        let seg = &segments[idx];
        let prev_hashes: Vec<_> = seg.ranges.iter().map(|r| r.prev_hash).collect();

        let best_pred = hash_index
            .get(&prev_hashes)
            .into_iter()
            .flatten()
            .filter(|&&pred_idx| dp[pred_idx].is_some() && segments[pred_idx].adjacent(seg))
            .max_by(|&&a, &&b| {
                let (count_a, _) = dp[a].unwrap();
                let (count_b, _) = dp[b].unwrap();
                // Reversed: prefer fewer segments (more compacted).
                count_b.cmp(&count_a)
            });

        dp[idx] = match best_pred {
            Some(&pred_idx) => {
                let (count, _) = dp[pred_idx].unwrap();
                Some((count + 1, Some(pred_idx)))
            }
            // Only segments at the earliest start position can be roots.
            None if seg.cmp_by_start(earliest) == std::cmp::Ordering::Equal => Some((1, None)),
            None => None,
        };
    }

    // Select the chain that reaches the highest block, breaking ties by fewest segments.
    let best_idx = (0..segments.len())
        .filter(|&idx| dp[idx].is_some())
        .max_by(|&a, &b| {
            let (count_a, _) = dp[a].unwrap();
            let (count_b, _) = dp[b].unwrap();
            segments[a]
                .cmp_by_end(&segments[b])
                // Prefer fewer segments (more compacted).
                .then(count_b.cmp(&count_a))
        })?;

    // Walk predecessor links from best endpoint back to the root.
    let mut chain: Vec<Segment> = std::iter::successors(Some(best_idx), |&idx| dp[idx].unwrap().1)
        .map(|i| segments[i].clone())
        .collect();
    chain.reverse();

    Some(Chain(chain))
}

/// Return the block ranges missing from this table out of the given `desired` range. The
/// returned ranges are non-overlapping and sorted by their start block number.
///
/// This function is designed for single-network segments (raw datasets). All segments must
/// have exactly one network range.
///
/// To resolve reorgs, the missing ranges may include block ranges already indexed. A reorg is
/// detected when there is some fork, which is a chain of segments that has a greater block height
/// than the canonical chain. Divergence from the canonical chain is detected using the `hash` and
/// `prev_hash` fields on the block range. When a reorg is detected, the missing ranges will
/// include any canonical ranges that overlap with the fork minus 1 block.
///
///
/// ```text
///                ┌───────────────────────────────────────────────────────┐
///   desired:     │ 00-02 │ 03-05 │ 06-08 │ 09-11 │ 12-14 │ 15-17 │ 18-20 │
///                └───────────────────────────────────────────────────────┘
///                        ┌───────────────────────────────┐
///   canonical:           │ 03-05 │ 06-08 │ 09-11 │ 12-14 │
///                        └───────────────────────────────┘
///                                        ┌───────────────────────┐
///   fork:                                │ 09-11'│ 12-14'│ 15-17'│
///                                        └───────────────────────┘
///                ┌───────┐       ┌───────┐                       ┌───────┐
///   missing:     │ 00-02 │       │ 06-08 │                       │ 18-20 │
///                └───────┘       └───────┘                       └───────┘
/// ```
///
/// - Ranges 00-02 and 18-20 are missing due to block range gap.
/// - Range 06-08 is missing due to reorg. The canonical chain overlaps with the fork,
///   so we should re-extract the previous segment.
pub fn missing_ranges(
    segments: &[Segment],
    desired: RangeInclusive<BlockNum>,
) -> Vec<RangeInclusive<BlockNum>> {
    // Invariant: this function only works for single-network segments (raw datasets)
    if let Some(first_segment) = segments.first() {
        assert_eq!(first_segment.ranges.len(), 1);
    }

    let mut missing = vec![desired.clone()];

    // remove overlapping ranges from each segment
    for segment in segments {
        assert_eq!(segment.ranges.len(), 1);
        let segment_range = segment.ranges[0].numbers.clone();
        let mut index = 0;
        while index < missing.len() {
            if block_range_intersection(missing[index].clone(), segment_range.clone()).is_none() {
                index += 1;
                continue;
            }
            let ranges = missing_block_ranges(segment_range.clone(), missing[index].clone());
            let next_index = index + ranges.len();
            missing.splice(index..=index, ranges);
            index = next_index;
        }
    }

    // add canonical range overlapping with reorg
    if let Some(canonical) = canonical_chain(segments)
        && let Some(fork) = fork_chain(&canonical, segments)
    {
        let reorg_block = fork.first_ranges()[0].start().saturating_sub(1);
        let canonical_range = canonical
            .0
            .iter()
            .map(|s| s.ranges[0].numbers.clone())
            .rfind(|r| r.contains(&reorg_block));
        if let Some(canonical_range) = canonical_range {
            let reorg_range = *canonical_range.start()..=reorg_block;
            if let Some(missing_range) = block_range_intersection(reorg_range, desired.clone()) {
                missing.push(missing_range);
            }
        }
    }

    merge_ranges(missing)
}

/// Returns the fork chain: the chain with the greatest block height past the canonical chain,
/// where there is no gap between the canonical chain and the fork.
///
/// ```text
///              ┌───────────────┐
///   canonical: │ a │ b │ c │ d │
///              └───────────────┘
///                      ┌────────────┐
///   fork:              │ c'│ d'│ e' │
///                      └────────────┘
/// ```
fn fork_chain(canonical: &Chain, segments: &[Segment]) -> Option<Chain> {
    // Collect non-canonical segments, sorted by end block ascending so pop() yields the
    // highest-end fork candidates first.
    let mut non_canonical: Vec<&Segment> = segments
        .iter()
        .filter(|s| !canonical.0.contains(s))
        .collect();
    non_canonical.sort_unstable_by(|a, b| a.cmp_by_end(b));

    // Search for a valid fork chain. A fork must:
    // 1. Extend beyond the canonical chain's end block
    // 2. Connect back to at most canonical.last() + 1 (to form a valid divergence point)
    while let Some(fork_end) = non_canonical.pop() {
        // Early exit: remaining segments all have end <= canonical.end() (sorted ascending),
        // so none can form a fork that extends beyond canonical.
        if fork_end.cmp_by_end(canonical.last_segment()) != std::cmp::Ordering::Greater {
            break;
        }

        // Build a candidate fork chain backwards from fork_end.
        let mut fork_segments = vec![fork_end.clone()];
        for segment in non_canonical.iter().rev() {
            if segment.adjacent(fork_segments.first().unwrap()) {
                // Stop if adding this segment would extend the fork past the divergence point.
                // A segment that ends at the same block with the same hash as a canonical segment
                // is a logical duplicate of canonical data, not part of an actual chain split.
                let duplicates_canonical = canonical.0.iter().rev().any(|canonical_seg| {
                    std::iter::zip(&segment.ranges, &canonical_seg.ranges).all(
                        |(segment_range, canonical_range)| {
                            segment_range.end() == canonical_range.end()
                                && segment_range.hash == canonical_range.hash
                        },
                    )
                });
                if duplicates_canonical {
                    break;
                }

                fork_segments.insert(0, (*segment).clone());
            }
        }
        // Check if this fork connects back to a valid divergence point.
        if std::iter::zip(&fork_segments[0].ranges, &canonical.last_segment().ranges)
            .all(|(fork_range, canonical_range)| fork_range.start() <= canonical_range.end() + 1)
        {
            return Some(Chain(fork_segments));
        }
    }

    None
}

fn missing_block_ranges(
    synced: RangeInclusive<BlockNum>,
    desired: RangeInclusive<BlockNum>,
) -> Vec<RangeInclusive<BlockNum>> {
    // no overlap
    if (synced.end() < desired.start()) || (synced.start() > desired.end()) {
        return vec![desired];
    }
    // desired is subset of synced
    if (synced.start() <= desired.start()) && (synced.end() >= desired.end()) {
        return vec![];
    }
    // partial overlap
    let mut result = Vec::new();
    if desired.start() < synced.start() {
        result.push(*desired.start()..=(*synced.start() - 1));
    }
    if desired.end() > synced.end() {
        result.push((*synced.end() + 1)..=*desired.end());
    }
    result
}

pub fn merge_ranges(mut ranges: Vec<RangeInclusive<BlockNum>>) -> Vec<RangeInclusive<BlockNum>> {
    ranges.sort_by_key(|r| *r.start());
    let mut index = 1;
    while index < ranges.len() {
        let current_range = ranges[index - 1].clone();
        let next_range = ranges[index].clone();
        if *next_range.start() <= (*current_range.end() + 1) {
            ranges[index - 1] =
                *current_range.start()..=BlockNum::max(*current_range.end(), *next_range.end());
            ranges.remove(index);
        } else {
            index += 1;
        }
    }
    ranges
}

/// Returns the intersection of two block number ranges, or `None` if they don't overlap.
fn block_range_intersection(
    a: RangeInclusive<BlockNum>,
    b: RangeInclusive<BlockNum>,
) -> Option<RangeInclusive<BlockNum>> {
    let start = BlockNum::max(*a.start(), *b.start());
    let end = BlockNum::min(*a.end(), *b.end());
    if start <= end {
        Some(start..=end)
    } else {
        None
    }
}

#[cfg(test)]
mod test {
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

    use super::{BlockRange, Chain, Segment, canonical_chain, fork_chain};

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
            None,
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
            None,
        )
    }

    fn check(
        segments: Vec<Segment>,
        expected_canonical: Option<Chain>,
        expected_fork: Option<Chain>,
    ) {
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

    /// Tests the private `missing_block_ranges()` helper across boundary and overlap scenarios.
    ///
    ///   Given: a synced range and a desired range
    ///   When:  `missing_block_ranges(synced, desired)` is called
    ///   Then:  the portions of `desired` not covered by `synced` are returned
    #[test]
    fn missing_block_ranges_returns_uncovered_portions() {
        // no overlap, desired before synced
        assert_eq!(super::missing_block_ranges(10..=20, 0..=5), vec![0..=5]);
        // no overlap, desired after synced
        assert_eq!(super::missing_block_ranges(0..=5, 10..=20), vec![10..=20]);
        // desired is subset of synced
        assert_eq!(super::missing_block_ranges(0..=10, 2..=8), vec![]);
        // desired is same as synced
        assert_eq!(super::missing_block_ranges(0..=10, 0..=10), vec![]);
        // synced starts before desired, ends with desired
        assert_eq!(super::missing_block_ranges(0..=10, 0..=10), vec![]);
        // synced starts with desired, ends after desired
        assert_eq!(super::missing_block_ranges(0..=10, 0..=10), vec![]);
        // partial overlap, desired starts before synced
        assert_eq!(super::missing_block_ranges(5..=10, 0..=7), vec![0..=4]);
        // partial overlap, desired ends after synced
        assert_eq!(super::missing_block_ranges(0..=5, 3..=10), vec![6..=10]);
        // partial overlap, desired surrounds synced
        assert_eq!(
            super::missing_block_ranges(5..=10, 0..=15),
            vec![0..=4, 11..=15]
        );
        // desired starts same as synced, ends after synced
        assert_eq!(super::missing_block_ranges(0..=5, 0..=10), vec![6..=10]);
        // desired starts before synced, ends same as synced
        assert_eq!(super::missing_block_ranges(5..=10, 0..=10), vec![0..=4]);
        // adjacent ranges (desired just before synced)
        assert_eq!(super::missing_block_ranges(5..=10, 0..=4), vec![0..=4]);
        // adjacent ranges (desired just after synced)
        assert_eq!(super::missing_block_ranges(0..=5, 6..=10), vec![6..=10]);
        // single block ranges
        assert_eq!(super::missing_block_ranges(0..=0, 0..=0), vec![]);
        assert_eq!(super::missing_block_ranges(0..=0, 1..=1), vec![1..=1]);
        assert_eq!(super::missing_block_ranges(1..=1, 0..=0), vec![0..=0]);
        assert_eq!(super::missing_block_ranges(0..=2, 0..=3), vec![3..=3]);
        assert_eq!(super::missing_block_ranges(1..=3, 0..=3), vec![0..=0]);
        assert_eq!(super::missing_block_ranges(0..=2, 0..=3), vec![3..=3]);
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
            Segment::new(
                FileId::try_from(1_i64).unwrap(),
                test_object(),
                vec![range],
                None,
            )
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
}
