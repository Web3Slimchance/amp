use std::{collections::HashMap, ops::RangeInclusive};

use alloy::primitives::BlockHash;
use datasets_common::{block_num::BlockNum, block_range::BlockRange};
use metadata_db::files::FileId;
use object_store::ObjectMeta;

/// A block range associated with the metadata from a file in object storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    id: FileId,
    object: ObjectMeta,
    ranges: Vec<BlockRange>,
}

impl Segment {
    /// Returns a Segment, where the ranges are ordered by network.
    pub fn new(id: FileId, object: ObjectMeta, mut ranges: Vec<BlockRange>) -> Self {
        ranges.sort_unstable_by(|a, b| a.network.cmp(&b.network));
        Self { id, object, ranges }
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

    let mut missing = coverage_gaps(segments, &desired);

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
            if let Some(missing_range) = block_range_intersection(reorg_range, desired) {
                missing.push(missing_range);
            }
        }
    }

    merge_ranges(missing)
}

/// Returns the gaps in segment coverage within the desired range.
fn coverage_gaps(
    segments: &[Segment],
    desired: &RangeInclusive<BlockNum>,
) -> Vec<RangeInclusive<BlockNum>> {
    let mut covered: Vec<RangeInclusive<BlockNum>> = segments
        .iter()
        .map(|s| {
            assert_eq!(s.ranges.len(), 1);
            s.ranges[0].numbers.clone()
        })
        .collect();
    covered.sort_unstable_by_key(|r| *r.start());

    // Merge overlapping/adjacent ranges into a sorted, non-overlapping coverage list
    let mut merged: Vec<RangeInclusive<BlockNum>> = Vec::new();
    for range in covered {
        if let Some(last) = merged.last_mut()
            && *range.start() <= *last.end() + 1
        {
            if *range.end() > *last.end() {
                *last = *last.start()..=*range.end();
            }
            continue;
        }
        merged.push(range);
    }

    // Sweep the merged coverage to collect gaps within the desired range
    let mut gaps = Vec::new();
    let mut cursor = *desired.start();
    for range in &merged {
        if cursor > *desired.end() {
            break;
        }
        if *range.end() < cursor {
            continue;
        }
        if *range.start() > cursor {
            let gap_end = BlockNum::min(*range.start() - 1, *desired.end());
            gaps.push(cursor..=gap_end);
        }
        cursor = *range.end() + 1;
    }
    if cursor <= *desired.end() {
        gaps.push(cursor..=*desired.end());
    }

    gaps
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
    // Build a hash map from block hash to segment for O(1) predecessor lookups.
    // To build a chain backwards from a segment, we look up its prev_hash in this map
    // to find the predecessor (whose hash matches).
    let mut by_hash: HashMap<BlockHash, Vec<&Segment>> = HashMap::new();
    let mut non_canonical: Vec<&Segment> = Vec::new();
    for s in segments {
        if !canonical.0.contains(s) {
            by_hash.entry(s.ranges[0].hash).or_default().push(s);
            non_canonical.push(s);
        }
    }

    // Collect canonical end-block hashes for the duplicate check
    let canonical_end_hashes: std::collections::HashSet<(BlockNum, BlockHash)> = canonical
        .0
        .iter()
        .map(|s| (*s.ranges[0].numbers.end(), s.ranges[0].hash))
        .collect();

    // Only chain tips (segments with no successor) can be the end of a fork.
    // A segment has a successor if some other non-canonical segment's prev_hash matches its hash.
    let prev_hashes: std::collections::HashSet<BlockHash> = non_canonical
        .iter()
        .map(|s| s.ranges[0].prev_hash)
        .collect();
    let mut tips: Vec<&Segment> = non_canonical
        .into_iter()
        .filter(|s| !prev_hashes.contains(&s.ranges[0].hash))
        .filter(|s| s.cmp_by_end(canonical.last_segment()) == std::cmp::Ordering::Greater)
        .collect();
    tips.sort_unstable_by(|a, b| a.cmp_by_end(b));

    // Search for a valid fork chain. A fork must:
    // 1. Extend beyond the canonical chain's end block
    // 2. Connect back to at most canonical.last() + 1 (to form a valid divergence point)
    while let Some(fork_end) = tips.pop() {
        // Build chain backwards: push predecessors then reverse
        let mut fork_segments = vec![fork_end.clone()];
        loop {
            let head = fork_segments.last().unwrap();
            // Look up segments whose hash matches head's prev_hash (i.e. potential predecessors)
            let Some(predecessors) = by_hash.get(&head.ranges[0].prev_hash) else {
                break;
            };
            // Find a segment whose hash matches head's prev_hash and is adjacent
            let Some(pred) = predecessors.iter().find(|s| s.adjacent(head)) else {
                break;
            };
            // Stop if this segment duplicates canonical data
            let end = *pred.ranges[0].numbers.end();
            let hash = pred.ranges[0].hash;
            if canonical_end_hashes.contains(&(end, hash)) {
                break;
            }
            fork_segments.push((*pred).clone());
        }
        fork_segments.reverse();

        // Check if this fork connects back to a valid divergence point.
        if std::iter::zip(&fork_segments[0].ranges, &canonical.last_segment().ranges)
            .all(|(fork_range, canonical_range)| fork_range.start() <= canonical_range.end() + 1)
        {
            return Some(Chain(fork_segments));
        }
    }

    None
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

#[cfg(any(test, feature = "test-fixtures"))]
mod fixtures;
#[cfg(any(test, feature = "test-fixtures"))]
pub use fixtures::load_arbitrum_fixture;

#[cfg(test)]
mod tests;
