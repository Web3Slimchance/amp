//! The program downloads the requested range of CAR files from the Old Faithful archive
//! and saves them to the specified output directory.
//!
//! # Reference
//!
//! - [Old Faithful Archive](https://docs.old-faithful.net)

use std::{
    num::NonZeroU64,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use amp_providers_solana::old_faithful::{car_download_url, car_filename};
use anyhow::Context;
use backon::{ExponentialBuilder, Retryable};
use clap::Parser;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;

const CAR_DOWNLOAD_PROGRESS_REPORT_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Parser)]
#[command(
    name = "solana-car-download",
    about = "Download Solana CAR files from Old Faithful"
)]
struct Cli {
    /// Start epoch. Defaults to epoch 0 if not specified.
    #[arg(long, short = 's', default_value_t = 0)]
    start_epoch: u64,

    /// End epoch (inclusive). Defaults to the latest epoch if not specified.
    #[arg(long, short = 'e')]
    end_epoch: Option<u64>,

    /// Output directory to save the downloaded CAR files. Defaults to the current directory.
    #[arg(long, short = 'o')]
    output_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(env_filter_or_info())
        .init();

    let cli = Cli::parse();

    let end_epoch = match cli.end_epoch {
        Some(end) if end < cli.start_epoch => {
            tracing::warn!(
                start_epoch = cli.start_epoch,
                end_epoch = end,
                "end epoch must be greater than or equal to start epoch"
            );
            anyhow::bail!("end epoch must be greater than or equal to start epoch");
        }
        // Set to `u64::MAX` to indicate no upper limit, since the loop will break when
        // no more CAR files are available.
        end => end.unwrap_or(u64::MAX),
    };
    let output_dir = match cli.output_dir {
        Some(dir) => dir,
        None => std::env::current_dir().context("reading current directory")?,
    };

    if !tokio::fs::try_exists(&output_dir)
        .await
        .context("checking if output directory exists")?
    {
        tokio::fs::create_dir_all(&output_dir)
            .await
            .with_context(|| format!("creating output directory at {}", output_dir.display()))?;
    }

    tracing::info!(
        start_epoch = cli.start_epoch,
        end_epoch = cli.end_epoch,
        output_dir = %output_dir.display(),
        "running CAR download"
    );

    // Retry on all errors except 404 Not Found, which indicates that there are no more epochs to download.
    let should_retry = |err: &CarDownloadError| {
        !matches!(err, CarDownloadError::Http(reqwest::StatusCode::NOT_FOUND))
    };
    let reqwest = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .build()?;

    for epoch in cli.start_epoch..=end_epoch {
        let dest = output_dir.join(car_filename(epoch));
        let result = (|| ensure_car_file_exists(epoch, &reqwest, &dest))
            .retry(ExponentialBuilder::new().without_max_times())
            .when(should_retry)
            .notify(|err, retry_after| {
                tracing::warn!(
                    %epoch,
                    error = %err,
                    error_source = monitoring::logging::error_source(&err),
                    "error downloading CAR file; retrying in {:.1}s", retry_after.as_secs_f32()
                );
            })
            .await;

        match result {
            // No more CAR files available for download.
            Err(CarDownloadError::Http(reqwest::StatusCode::NOT_FOUND)) => break,
            Err(err) => {
                tracing::error!(
                    %epoch,
                    error = %err,
                    error_source = monitoring::logging::error_source(&err),
                    "failed to download CAR file after retries"
                );
                anyhow::bail!("failed to download CAR file for epoch {epoch}: {err}");
            }
            _ => {}
        }
    }

    Ok(())
}

/// Ensures that the entire CAR file for the given epoch exists at the specified destination path.
///
/// If the file was partially downloaded before, the download will resume from where it left off.
async fn ensure_car_file_exists(
    epoch: solana_clock::Epoch,
    reqwest: &reqwest::Client,
    dest: &Path,
) -> Result<(), CarDownloadError> {
    enum DownloadAction {
        Download,
        Resume(u64),
        Restart,
        Skip,
    }

    let download_url = car_download_url(epoch);

    // Get the actual file size from the server to determine if we need to resume, as well
    // as for download progress reports.
    let remote_file_size = {
        let head_response = reqwest.head(&download_url).send().await?;
        if head_response.status() != reqwest::StatusCode::OK {
            return Err(CarDownloadError::Http(head_response.status()));
        }

        let Some(content_length) = head_response.headers().get(reqwest::header::CONTENT_LENGTH)
        else {
            return Err(CarDownloadError::MissingContentLengthHeader);
        };

        content_length
            .to_str()
            .map_err(|_| CarDownloadError::ContentLengthParsing)?
            .parse()
            .map(NonZeroU64::new)
            .map_err(|_| CarDownloadError::ContentLengthParsing)?
            .ok_or(CarDownloadError::ZeroContentLength)?
    };

    let action = match tokio::fs::metadata(dest).await.map(|meta| meta.len()) {
        Ok(0) => DownloadAction::Download,
        Ok(local_file_size) => {
            match local_file_size.cmp(&remote_file_size.get()) {
                // Local file is partially downloaded, need to resume.
                std::cmp::Ordering::Less => DownloadAction::Resume(local_file_size),
                // Local file is larger than remote file, need to restart download.
                std::cmp::Ordering::Greater => DownloadAction::Restart,
                // File already fully downloaded.
                std::cmp::Ordering::Equal => DownloadAction::Skip,
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => DownloadAction::Download,
        Err(err) => return Err(CarDownloadError::Io(err)),
    };

    // Set up HTTP headers for range requests if the file already exists.
    let mut headers = reqwest::header::HeaderMap::new();
    let mut file = {
        let mut opts = tokio::fs::File::options();
        let open_fut = match action {
            DownloadAction::Download => {
                tracing::debug!(%download_url, "downloading CAR file");
                opts.create(true).append(true).open(&dest)
            }
            DownloadAction::Resume(download_offset) => {
                tracing::debug!(
                    %download_url,
                    %download_offset,
                    "resuming CAR file download"
                );
                let range_header = format!("bytes={download_offset}-");
                let range_header_value = reqwest::header::HeaderValue::from_str(&range_header)
                    .expect("invalid range header");
                headers.insert(reqwest::header::RANGE, range_header_value);
                opts.append(true).open(&dest)
            }
            DownloadAction::Restart => {
                tracing::debug!(
                    %download_url,
                    "local CAR file is larger than remote file, restarting download"
                );
                opts.write(true).truncate(true).open(&dest)
            }
            DownloadAction::Skip => {
                tracing::debug!(
                    %download_url,
                    "local CAR file already fully downloaded, skipping download"
                );
                return Ok(());
            }
        };
        open_fut.await?
    };

    let download_start = Instant::now();

    let download_response = reqwest.get(download_url).headers(headers).send().await?;
    let status = download_response.status();
    if !status.is_success() {
        return Err(CarDownloadError::Http(status));
    }

    let mut bytes_downloaded: u64 = if let DownloadAction::Resume(offset) = action {
        // Expecting a 206 Partial Content response when resuming.
        if status != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(CarDownloadError::PartialDownloadNotSupported);
        }
        offset
    } else {
        0
    };

    // Stream the file content since these files can be extremely large.
    let mut stream = download_response.bytes_stream();

    log_download_progress(epoch, bytes_downloaded, remote_file_size);
    let mut last_progress_report = download_start;

    while let Some(chunk) = stream.next().await.transpose()? {
        file.write_all(&chunk).await?;
        bytes_downloaded += chunk.len() as u64;

        // Report progress to the user.
        if last_progress_report.elapsed() >= CAR_DOWNLOAD_PROGRESS_REPORT_INTERVAL {
            log_download_progress(epoch, bytes_downloaded, remote_file_size);
            last_progress_report = Instant::now();
        }
    }
    file.sync_all().await?;

    let download_duration = {
        let dur = download_start.elapsed().as_secs_f32();
        format!("{dur:.1}")
    };
    tracing::info!(%epoch, %bytes_downloaded, duration_secs = %download_duration, "downloaded CAR file");

    Ok(())
}

/// Errors that can occur during the CAR file download process.
#[derive(Debug, thiserror::Error)]
enum CarDownloadError {
    /// Errors related to file I/O operations, such as opening or writing to the destination file.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// HTTP errors, such as non-success status codes returned by the server when attempting to download the CAR file.
    #[error("HTTP error with status code: {0}")]
    Http(reqwest::StatusCode),
    /// Errors originating from the `reqwest` library, such as network errors or issues with the HTTP request/response.
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    /// The `Content-Length` header is missing from the HTTP response.
    #[error("missing Content-Length header in HTTP response")]
    MissingContentLengthHeader,
    /// The `Content-Length` header value could not be parsed as a valid integer.
    #[error("error parsing Content-Length header")]
    ContentLengthParsing,
    /// The `Content-Length` header value is zero, which is invalid for a CAR file.
    #[error("Content-Length header value is zero")]
    ZeroContentLength,
    /// The server does not support partial downloads.
    #[error("partial downloads are not supported by the server")]
    PartialDownloadNotSupported,
}

fn log_download_progress(
    epoch: solana_clock::Epoch,
    bytes_downloaded: u64,
    bytes_total: NonZeroU64,
) {
    let percent_done = {
        let p = (bytes_downloaded as f64 / bytes_total.get() as f64) * 100.0;
        format!("{p:.2}")
    };
    tracing::info!(
        %epoch,
        %bytes_downloaded,
        %bytes_total,
        %percent_done,
        "downloading CAR file"
    );
}

fn env_filter_or_info() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
}
