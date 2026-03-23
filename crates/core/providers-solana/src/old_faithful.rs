//! Old Faithful v1 (OF1) block streaming for Solana.
//!
//! Streams decoded Solana blocks from [Old Faithful] CAR archive files, either
//! from a remote HTTP source or from a local directory of pre-downloaded files.
//! Each epoch is stored as a single CAR file containing a DAG of blocks,
//! entries, transactions, and rewards that are reassembled into [`DecodedSlot`]
//! values.
//!
//! [Old Faithful]: https://docs.old-faithful.net

mod decode;
#[cfg(debug_assertions)]
mod epoch_supervision;
pub mod metrics;
mod reader;

#[cfg(debug_assertions)]
use std::{collections::HashSet, sync::Mutex};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

pub use decode::{DecodedBlockRewards, DecodedField, DecodedSlot, DecodedTransactionStatusMeta};
use futures::Stream;
use yellowstone_faithful_car_parser as car_parser;

use self::{decode::read_and_decode_slot, metrics::MonitoredAsyncRead};
use crate::{
    error::{CarReaderError, OldFaithfulStreamError},
    rpc_client,
};

/// Create a stream of decoded slots for the given epoch by reading from the
/// corresponding CAR file downloaded from the Old Faithful archive.
#[allow(clippy::too_many_arguments)]
pub fn stream(
    start: solana_clock::Slot,
    end: solana_clock::Slot,
    reqwest: Arc<reqwest::Client>,
    solana_rpc_client: Arc<rpc_client::SolanaRpcClient>,
    archive_dir: Option<PathBuf>,
    get_block_config: rpc_client::rpc_config::RpcBlockConfig,
    metrics: Option<metrics::Context>,
    #[cfg(debug_assertions)] epochs_in_progress: Arc<Mutex<HashSet<solana_clock::Epoch>>>,
) -> impl Stream<Item = Result<DecodedSlot, OldFaithfulStreamError>> {
    async_stream::stream! {
        // Pre-fetch the initial previous block hash via JSON-RPC so that we don't have to
        // (potentially) read multiple CAR files to find it.
        let mut prev_blockhash = if start == 0 {
            // Known previous blockhash for genesis mainnet block.
            bs58::decode("4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZAMdL4VZHirAn")
                .into_vec()
                .map(TryInto::try_into)
                .map_err(OldFaithfulStreamError::DecodeBase58)?
                .map_err(|vec: Vec<_>| OldFaithfulStreamError::TryIntoArray {
                    expected_len: 32,
                    actual_len: vec.len(),
                })?
        } else {
            let mut slot = start;
            loop {
                let metrics = metrics.clone().map(|m| m.registry);
                let resp = solana_rpc_client
                    .get_block(slot, get_block_config, metrics)
                    .await;

                match resp {
                    Ok(block) => {
                        break bs58::decode(block.previous_blockhash)
                            .into_vec()
                            .map(TryInto::try_into)
                            .map_err(OldFaithfulStreamError::DecodeBase58)?
                            .map_err(|vec: Vec<_>| OldFaithfulStreamError::TryIntoArray {
                                expected_len: 32,
                                actual_len: vec.len(),
                            })?;
                    }
                    Err(e) if rpc_client::is_block_missing_err(&e) => slot += 1,
                    Err(e) => {
                        yield Err(OldFaithfulStreamError::RpcClient(e));
                        return;
                    }
                }
            }
        };

        let start_epoch = start / solana_clock::DEFAULT_SLOTS_PER_EPOCH;
        let end_epoch = end / solana_clock::DEFAULT_SLOTS_PER_EPOCH;

        for epoch in start_epoch..=end_epoch {
            #[cfg(debug_assertions)]
            let _guard = epoch_supervision::Guard::new(epochs_in_progress.as_ref(), epoch);

            let mut node_reader = match make_reader(epoch, &reqwest, archive_dir.as_deref())
                .await
                .map(|r| MonitoredAsyncRead::new(r, epoch, metrics.clone()))
                .map(car_parser::node::NodeReader::new)
            {
                Ok(r) => r,
                // No more CAR files available, not an error.
                Err(CarReaderError::FileNotFound) => return,
                // Non-recoverable error from the CAR reader.
                Err(err) => {
                    yield Err(OldFaithfulStreamError::FileStream(err));
                    return;
                }
            };

            while let Some(slot) = read_and_decode_slot(&mut node_reader, prev_blockhash)
                .await
                .transpose()
            {
                let slot = match slot {
                    Ok(slot) => slot,
                    // IO errors from the node reader could come from the underlying CAR reader.
                    // Try to downcast to `CarReaderError` to determine how to map into
                    // `OldFaithfulStreamError`.
                    //
                    // NOTE: There should be no retry logic here because the CAR reader should handle
                    // all retry logic internally and only return an error when a non-recoverable
                    // error occurs.
                    Err(OldFaithfulStreamError::NodeParse(car_parser::node::NodeError::Io(io_err))) => {
                        match io_err.downcast::<CarReaderError>() {
                            // Non-recoverable error from the CAR reader.
                            Ok(car_err) => {
                                yield Err(OldFaithfulStreamError::FileStream(car_err));
                            }
                            // Non-recoverable IO error from the `NodeParser`.
                            Err(io_err) => {
                                let original_err = OldFaithfulStreamError::NodeParse(
                                    car_parser::node::NodeError::Io(io_err),
                                );
                                yield Err(original_err);
                            }
                        };
                        return;
                    }
                    Err(e) => {
                        yield Err(e);
                        return;
                    }
                };
                prev_blockhash = slot.blockhash;

                if slot.slot < start {
                    // Skip blocks before the start of the requested range.
                    continue;
                }

                match slot.slot.cmp(&end) {
                    std::cmp::Ordering::Less => {
                        yield Ok(slot);
                    }
                    std::cmp::Ordering::Equal => {
                        yield Ok(slot);
                        return;
                    }
                    std::cmp::Ordering::Greater => {
                        return;
                    }
                }
            }
        }
    }
}

/// Generates a filename that follows a consistent naming convention for Old Faithful CAR files.
///
/// Reference: <https://docs.old-faithful.net/references/of1-files>.
pub fn car_filename(epoch: solana_clock::Epoch) -> String {
    format!("epoch-{epoch}.car")
}

/// Generates the Old Faithful CAR download URL for the given epoch.
///
/// Reference: <https://docs.old-faithful.net/references/of1-files>.
pub fn car_download_url(epoch: solana_clock::Epoch) -> String {
    format!("https://files.old-faithful.net/{epoch}/epoch-{epoch}.car")
}

async fn make_reader(
    epoch: solana_clock::Epoch,
    reqwest: &Arc<reqwest::Client>,
    archive_dir: Option<&Path>,
) -> Result<Box<dyn tokio::io::AsyncRead + Unpin + Send>, CarReaderError> {
    match archive_dir {
        Some(dir) => {
            let r = reader::local(epoch, dir)?;
            Ok(Box::new(r) as Box<dyn tokio::io::AsyncRead + Unpin + Send>)
        }
        None => {
            let r = reader::remote(epoch, reqwest.clone()).await?;
            Ok(Box::new(r) as Box<dyn tokio::io::AsyncRead + Unpin + Send>)
        }
    }
}
