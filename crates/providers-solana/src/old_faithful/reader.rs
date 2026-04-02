//! [`AsyncRead`](tokio::io::AsyncRead) implementations for reading CAR files.
//!
//! Provides two readers:
//! - [`LocalCarReader`]: memory-maps a local CAR file for zero-copy reads.
//! - [`RemoteCarReader`]: streams a CAR file over HTTP with automatic retry,
//!   exponential backoff, and resumption via `Range` headers.

use std::{pin::Pin, sync::Arc, time::Duration};

use futures::{FutureExt, Stream, StreamExt};

use crate::{
    error::CarReaderError,
    old_faithful::{car_download_url, car_filename},
};

type ConnectFuture = Pin<Box<dyn Future<Output = reqwest::Result<reqwest::Response>> + Send>>;
type ByteStream = Pin<Box<dyn Stream<Item = reqwest::Result<bytes::Bytes>> + Send>>;
type BackoffFuture = Pin<Box<tokio::time::Sleep>>;

pub(crate) async fn remote(
    epoch: solana_clock::Epoch,
    reqwest: Arc<reqwest::Client>,
) -> Result<impl tokio::io::AsyncRead + Send, CarReaderError> {
    RemoteCarReader::connect(epoch, reqwest).await
}

pub(crate) fn local(
    epoch: solana_clock::Epoch,
    archive_dir: &std::path::Path,
) -> Result<impl tokio::io::AsyncRead + Send + 'static, CarReaderError> {
    match LocalCarReader::new(epoch, archive_dir) {
        Ok(r) => Ok(r),
        Err(io_err) if io_err.kind() == std::io::ErrorKind::NotFound => {
            Err(CarReaderError::FileNotFound)
        }
        Err(io_err) => Err(CarReaderError::Io(io_err)),
    }
}

enum ReaderState {
    /// A single in-flight HTTP request to (re)connect.
    Connect(ConnectFuture),
    /// We have an active byte stream.
    Stream(ByteStream),
    /// We are waiting until a backoff deadline before attempting reconnect.
    Backoff(BackoffFuture),
}

struct RemoteCarReader {
    epoch: solana_clock::Epoch,
    reqwest: Arc<reqwest::Client>,
    state: ReaderState,
    overflow: Vec<u8>,
    bytes_read_total: u64,

    // Backoff control
    connect_attempt: u32,
    max_backoff: Duration,
    base_backoff: Duration,
}

impl RemoteCarReader {
    async fn connect(
        epoch: solana_clock::Epoch,
        reqwest: Arc<reqwest::Client>,
    ) -> Result<Self, CarReaderError> {
        let mut attempt = 0;
        let base_backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(30);

        // Initial retry loop - we want to verify the file exists and is reachable before
        // returning a reader that will error on first read.
        loop {
            let err = match request_car_file_with_offset(reqwest.clone(), epoch, 0).await {
                Ok(resp) => match resp.status() {
                    // File doesn't exist, no point retrying.
                    reqwest::StatusCode::NOT_FOUND => return Err(CarReaderError::FileNotFound),
                    // Transient HTTP error - backoff and retry.
                    status if !status.is_success() => CarReaderError::Http(status),
                    _ => {
                        // Successful connection, start streaming bytes.
                        return Ok(Self {
                            epoch,
                            reqwest,
                            state: ReaderState::Stream(Box::pin(resp.bytes_stream())),
                            overflow: Vec::new(),
                            bytes_read_total: 0,
                            connect_attempt: 0,
                            base_backoff,
                            max_backoff,
                        });
                    }
                },
                // Network error - backoff and retry.
                Err(e) => CarReaderError::Reqwest(e),
            };

            let backoff = compute_backoff(attempt, base_backoff, max_backoff);
            let backoff_str = format!("{:.1}s", backoff.as_secs_f32());
            attempt += 1;
            tracing::warn!(
                epoch = epoch,
                attempt = attempt,
                backoff = %backoff_str,
                error = ?err,
                error_source = monitoring::logging::error_source(&err),
                "Failed to connect to CAR file; retrying"
            );
            tokio::time::sleep(backoff).await;
        }
    }

    fn schedule_backoff(&mut self, err: CarReaderError) {
        let backoff = compute_backoff(self.connect_attempt, self.base_backoff, self.max_backoff);
        let backoff_str = format!("{:.1}s", backoff.as_secs_f32());
        self.connect_attempt = self.connect_attempt.saturating_add(1);
        tracing::warn!(
            epoch = self.epoch,
            bytes_read = self.bytes_read_total,
            attempt = self.connect_attempt,
            backoff = %backoff_str,
            error = ?err,
            error_source = monitoring::logging::error_source(&err),
            "CAR reader failed; scheduled retry"
        );
        let sleep = tokio::time::sleep(backoff);
        self.state = ReaderState::Backoff(Box::pin(sleep));
    }
}

impl tokio::io::AsyncRead for RemoteCarReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();

        // Drain overflow first.
        if !this.overflow.is_empty() {
            let to_copy = this.overflow.len().min(buf.remaining());
            buf.put_slice(&this.overflow[..to_copy]);
            this.overflow.drain(..to_copy);
            return std::task::Poll::Ready(Ok(()));
        }

        // Retry loop, return on successful read, EOF, or non-recoverable error (RangeRequestUnsupported).
        loop {
            match &mut this.state {
                ReaderState::Connect(conn) => match conn.as_mut().poll(cx) {
                    std::task::Poll::Ready(Ok(resp)) => {
                        let status = resp.status();
                        // Handle error codes.
                        match status {
                            reqwest::StatusCode::NOT_FOUND => {
                                let err = std::io::Error::other(CarReaderError::FileNotFound);
                                return std::task::Poll::Ready(Err(err));
                            }
                            status if !status.is_success() => {
                                this.schedule_backoff(CarReaderError::Http(status));
                                continue;
                            }
                            _ => {}
                        }

                        // Handle partial content.
                        if this.bytes_read_total > 0
                            && status != reqwest::StatusCode::PARTIAL_CONTENT
                        {
                            let e = std::io::Error::other(CarReaderError::RangeRequestUnsupported);
                            return std::task::Poll::Ready(Err(e));
                        }

                        // Initial connection succeeded, start reading the byte stream.
                        this.connect_attempt = 0;
                        this.state = ReaderState::Stream(Box::pin(resp.bytes_stream()));
                    }
                    std::task::Poll::Ready(Err(e)) => {
                        this.schedule_backoff(CarReaderError::Reqwest(e));
                    }
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                },
                ReaderState::Stream(stream) => match stream.poll_next_unpin(cx) {
                    // Reached EOF.
                    std::task::Poll::Ready(None) => {
                        return std::task::Poll::Ready(Ok(()));
                    }
                    // Read some bytes, account for possible overflow.
                    std::task::Poll::Ready(Some(Ok(bytes))) => {
                        let n_read = bytes.len();
                        let to_copy = n_read.min(buf.remaining());

                        buf.put_slice(&bytes[..to_copy]);
                        this.overflow.extend_from_slice(&bytes[to_copy..]);
                        this.bytes_read_total += n_read as u64;

                        return std::task::Poll::Ready(Ok(()));
                    }
                    std::task::Poll::Ready(Some(Err(e))) => {
                        this.schedule_backoff(CarReaderError::Reqwest(e));
                    }
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                },
                ReaderState::Backoff(backoff) => match backoff.poll_unpin(cx) {
                    std::task::Poll::Ready(()) => {
                        let conn = request_car_file_with_offset(
                            this.reqwest.clone(),
                            this.epoch,
                            this.bytes_read_total,
                        );
                        this.state = ReaderState::Connect(Box::pin(conn));
                    }
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                },
            }
        }
    }
}

struct LocalCarReader {
    cursor: std::io::Cursor<memmap2::Mmap>,
}

impl LocalCarReader {
    fn new(epoch: solana_clock::Epoch, archive_dir: &std::path::Path) -> std::io::Result<Self> {
        let path = archive_dir.join(car_filename(epoch));
        let file = fs_err::File::open(&path)?;
        // SAFETY: We rely on the file not being modified while mapped. This is acceptable
        // because CAR archive files are immutable once written.
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
        mmap.advise(memmap2::Advice::Sequential)?;
        Ok(Self {
            cursor: std::io::Cursor::new(mmap),
        })
    }
}

impl tokio::io::AsyncRead for LocalCarReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let pos = this.cursor.position() as usize;
        let src = this.cursor.get_ref();

        if pos >= src.len() {
            return std::task::Poll::Ready(Ok(()));
        }

        let remaining = &src[pos..];
        let to_copy = remaining.len().min(buf.remaining());
        buf.put_slice(&remaining[..to_copy]);
        let new_pos = (pos + to_copy) as u64;
        this.cursor.set_position(new_pos);
        std::task::Poll::Ready(Ok(()))
    }
}

async fn request_car_file_with_offset(
    reqwest: Arc<reqwest::Client>,
    epoch: solana_clock::Epoch,
    offset: u64,
) -> Result<reqwest::Response, reqwest::Error> {
    let mut req = reqwest.get(car_download_url(epoch));
    if offset > 0 {
        req = req.header(reqwest::header::RANGE, format!("bytes={offset}-"));
    }
    req.send().await
}

fn compute_backoff(attempt: u32, base_backoff: Duration, max_backoff: Duration) -> Duration {
    // attempt=0 => base, attempt=1 => 2*base, attempt=2 => 4*base, ...
    let factor = 1 << attempt.min(30);
    let backoff = base_backoff.saturating_mul(factor as u32);
    backoff.min(max_backoff)
}
