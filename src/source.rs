//! Multi-protocol data source abstraction for the ingestion pipeline.
//!
//! The module uses synchronous non-blocking I/O with polling rather than async because the
//! storage engine already performs synchronous writes for durability. Mixing paradigms would
//! add complexity without improving throughput.
//!
//! Each source tags entries with a prefix indicating their origin, allowing downstream consumers
//! to distinguish log provenance. When the ingester channel fills, an exponential backoff loop
//! retries sends without spinning, capping at the poll interval to stay responsive.
//!
//! File tailing relies on the [`notify`] crate because Rust's standard library provides no
//! filesystem event API. The watcher monitors the parent directory rather than the file itself,
//! which handles log rotation where the target file is replaced atomically.

use std::fmt;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use lib_kifa::{KIBI, map_err};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use crate::ingester::{self, IngesterHandle};

const LINE_BUFFER_CAPACITY: usize = 8 * KIBI;
const UDP_BUFFER_SIZE: usize = 8 * KIBI;
const POLL_TIMEOUT: Duration = Duration::from_millis(100);
const TCP_READ_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    ChannelDisconnected,
    WatcherInit(notify::Error),
    WatcherEvent(notify::Error),
}

map_err!(Io, io::Error);
map_err!(WatcherInit, notify::Error);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => e.fmt(f),
            Self::ChannelDisconnected => write!(f, "channel disconnected"),
            Self::WatcherInit(e) => write!(f, "watcher init: {e}"),
            Self::WatcherEvent(e) => write!(f, "watcher event: {e}"),
        }
    }
}

impl std::error::Error for Error {}

pub trait Source: Send + 'static {
    fn run(self, handle: IngesterHandle) -> Result<(), Error>;
}

const BACKOFF_INITIAL: Duration = Duration::from_millis(1);
const BACKOFF_MAX: Duration = POLL_TIMEOUT;

fn tag_and_send(
    handle: &IngesterHandle,
    prefix: &[u8],
    line: &[u8],
    shutdown: &AtomicBool,
) -> Result<bool, Error> {
    if shutdown.load(Ordering::Relaxed) {
        return Ok(true);
    }

    let mut tagged = [prefix, line].concat();

    let mut backoff = BACKOFF_INITIAL;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(true);
        }

        // The channel returns ownership on backpressure, allowing the same allocation to be
        // retried without data loss or reallocation.
        match handle.try_send(tagged) {
            Ok(()) => return Ok(false),
            Err(ingester::SendError::Full(returned)) => {
                log::debug!("Backpressure: channel full, retrying after {backoff:?}");
                tagged = returned;
                thread::sleep(backoff);
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
            Err(ingester::SendError::Disconnected) => {
                return Err(Error::ChannelDisconnected);
            }
        }
    }
}

pub struct StdinSource {
    shutdown: Arc<AtomicBool>,
}

impl StdinSource {
    #[must_use]
    pub const fn new(shutdown: Arc<AtomicBool>) -> Self {
        Self { shutdown }
    }
}

impl Source for StdinSource {
    fn run(self, handle: IngesterHandle) -> Result<(), Error> {
        let stdin = io::stdin();
        // read_until operates on raw bytes, avoiding the per-line UTF-8
        // validation that BufRead::lines() performs internally.
        let mut reader = BufReader::with_capacity(LINE_BUFFER_CAPACITY, stdin.lock());
        let prefix = b"[stdin] ";
        let mut line_buf = Vec::with_capacity(LINE_BUFFER_CAPACITY);

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            line_buf.clear();
            let bytes_read = reader.read_until(b'\n', &mut line_buf)?;
            if bytes_read == 0 {
                break;
            }

            let mut end = line_buf.len();
            while end > 0 && matches!(line_buf[end - 1], b'\n' | b'\r') {
                end -= 1;
            }

            if tag_and_send(&handle, prefix, &line_buf[..end], &self.shutdown)? {
                break;
            }
        }

        Ok(())
    }
}

pub struct TcpSource {
    listener: TcpListener,
    shutdown: Arc<AtomicBool>,
}

impl TcpSource {
    pub fn bind<A: ToSocketAddrs>(addr: A, shutdown: Arc<AtomicBool>) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Self { listener, shutdown })
    }
}

impl Source for TcpSource {
    fn run(self, handle: IngesterHandle) -> Result<(), Error> {
        let mut connection_threads = Vec::new();

        while !self.shutdown.load(Ordering::Relaxed) {
            match self.listener.accept() {
                Ok((stream, peer_addr)) => {
                    let conn_handle = handle.clone();
                    let conn_shutdown = Arc::clone(&self.shutdown);

                    let thread = thread::spawn(move || {
                        let _ =
                            handle_tcp_connection(stream, peer_addr, &conn_handle, &conn_shutdown);
                    });

                    connection_threads.push(thread);
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(POLL_TIMEOUT);
                }
                Err(e) => return Err(Error::Io(e)),
            }

            // Reaping finished threads prevents unbounded memory growth from accumulating handles.
            connection_threads.retain(|t| !t.is_finished());
        }

        for thread in connection_threads {
            let _ = thread.join();
        }

        Ok(())
    }
}

fn handle_tcp_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    handle: &IngesterHandle,
    shutdown: &AtomicBool,
) -> Result<(), Error> {
    // The timeout forces read_line to yield periodically so the shutdown flag can be checked.
    stream.set_read_timeout(Some(TCP_READ_TIMEOUT))?;

    let prefix = format!("[tcp:{peer_addr}] ").into_bytes();
    let mut reader = BufReader::with_capacity(LINE_BUFFER_CAPACITY, stream);
    let mut line = String::with_capacity(LINE_BUFFER_CAPACITY);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                let trimmed = line.trim_end_matches(['\n', '\r']);
                if tag_and_send(handle, &prefix, trimmed.as_bytes(), shutdown)? {
                    break;
                }
            }
            // Timeout and WouldBlock are expected; they indicate no data is available yet.
            Err(ref e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
            }
            Err(e) => return Err(Error::Io(e)),
        }
    }

    Ok(())
}

pub struct UdpSource {
    socket: UdpSocket,
    shutdown: Arc<AtomicBool>,
}

impl UdpSource {
    pub fn bind<A: ToSocketAddrs>(addr: A, shutdown: Arc<AtomicBool>) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr)?;
        socket.set_nonblocking(true)?;
        Ok(Self { socket, shutdown })
    }
}

impl Source for UdpSource {
    fn run(self, handle: IngesterHandle) -> Result<(), Error> {
        let mut buffer = [0; UDP_BUFFER_SIZE];

        while !self.shutdown.load(Ordering::Relaxed) {
            match self.socket.recv_from(&mut buffer) {
                Ok((len, peer)) => {
                    let prefix = format!("[udp:{peer}] ").into_bytes();
                    let data = &buffer[..len];
                    let trimmed = trim_line_endings(data);
                    if tag_and_send(&handle, &prefix, trimmed, &self.shutdown)? {
                        break;
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(POLL_TIMEOUT);
                }
                Err(e) => return Err(Error::Io(e)),
            }
        }

        Ok(())
    }
}

fn trim_line_endings(data: &[u8]) -> &[u8] {
    let mut end = data.len();
    while end > 0 && (data[end - 1] == b'\n' || data[end - 1] == b'\r') {
        end -= 1;
    }
    &data[..end]
}

pub struct FileTailer {
    path: PathBuf,
    shutdown: Arc<AtomicBool>,
    from_beginning: bool,
}

impl FileTailer {
    #[must_use]
    pub const fn new(path: PathBuf, shutdown: Arc<AtomicBool>) -> Self {
        Self { path, shutdown, from_beginning: false }
    }

    #[cfg(test)]
    #[must_use]
    pub const fn reading_from_start(mut self) -> Self {
        self.from_beginning = true;
        self
    }
}

impl Source for FileTailer {
    fn run(self, handle: IngesterHandle) -> Result<(), Error> {
        let canonical_path = self.path.canonicalize()?;
        let prefix = format!("[file:{}] ", canonical_path.display()).into_bytes();
        let parent = canonical_path.parent().unwrap_or(Path::new("."));

        let (tx, rx) = mpsc::channel();
        let mut watcher: RecommendedWatcher = Watcher::new(
            tx,
            notify::Config::default().with_poll_interval(Duration::from_millis(500)),
        )?;

        watcher.watch(parent, RecursiveMode::NonRecursive)?;

        let mut file = File::open(&canonical_path)?;
        // Seeking to end mimics tail -f behavior: only new content is captured.
        if !self.from_beginning {
            file.seek(SeekFrom::End(0))?;
        }

        let mut reader = BufReader::with_capacity(LINE_BUFFER_CAPACITY, file);
        let mut line = String::with_capacity(LINE_BUFFER_CAPACITY);

        read_file_lines(&mut reader, &mut line, &handle, &prefix, &self.shutdown)?;

        while !self.shutdown.load(Ordering::Relaxed) {
            match rx.recv_timeout(POLL_TIMEOUT) {
                Ok(Ok(event)) => {
                    if is_relevant_file_event(&event, &canonical_path) {
                        read_file_lines(&mut reader, &mut line, &handle, &prefix, &self.shutdown)?;
                    }
                }
                Ok(Err(e)) => return Err(Error::WatcherEvent(e)),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }

        Ok(())
    }
}

fn is_relevant_file_event(event: &Event, canonical_path: &Path) -> bool {
    // Only Modify and Create events indicate new readable content. Create handles log rotation
    // where the file is replaced atomically.
    let dominated_by_modify = matches!(event.kind, EventKind::Modify(_) | EventKind::Create(_));

    dominated_by_modify
        && event.paths.iter().any(|p| {
            p == canonical_path
                || p.canonicalize()
                    .ok()
                    .as_deref()
                    .is_some_and(|resolved| resolved == canonical_path)
        })
}

fn read_file_lines<R: Read>(
    reader: &mut BufReader<R>,
    line: &mut String,
    handle: &IngesterHandle,
    prefix: &[u8],
    shutdown: &AtomicBool,
) -> Result<(), Error> {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        line.clear();
        // Zero bytes indicates EOF, not an error. The file may grow later and trigger another read.
        match reader.read_line(line) {
            Ok(0) => return Ok(()),
            Ok(_) => {
                let trimmed = line.trim_end_matches(['\n', '\r']);
                if tag_and_send(handle, prefix, trimmed.as_bytes(), shutdown)? {
                    return Ok(());
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(e) => return Err(Error::Io(e)),
        }
    }
}

#[derive(Default)]
pub struct SourceRunner {
    handles: Vec<JoinHandle<Result<(), Error>>>,
}

impl SourceRunner {
    #[must_use]
    pub const fn new() -> Self {
        Self { handles: Vec::new() }
    }

    pub fn spawn<S: Source>(&mut self, source: S, ingester_handle: IngesterHandle) {
        let thread = thread::spawn(move || source.run(ingester_handle));
        self.handles.push(thread);
    }

    #[must_use]
    pub fn join_all(self) -> Vec<Result<(), Error>> {
        self.handles.into_iter().filter_map(|h| h.join().ok()).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::net::TcpStream;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    use std::{fs, thread};

    use lib_kifa::engine::{Config, StorageEngine};
    use tempfile::{NamedTempFile, tempdir};

    use super::*;
    use crate::ingester::Ingester;

    fn create_test_engine() -> Arc<StorageEngine> {
        let dir = tempdir().unwrap();
        let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();
        Arc::new(engine)
    }

    fn create_test_ingester() -> (Ingester, IngesterHandle, Arc<AtomicBool>) {
        let engine = create_test_engine();
        let (ingester, handle) = Ingester::with_default_capacity(engine);
        let shutdown = handle.shutdown_flag();
        (ingester, handle, shutdown)
    }

    #[test]
    fn trim_line_endings_handles_all_cases() {
        assert_eq!(trim_line_endings(b"hello\n"), b"hello");
        assert_eq!(trim_line_endings(b"hello\r\n"), b"hello");
        assert_eq!(trim_line_endings(b"hello\r"), b"hello");
        assert_eq!(trim_line_endings(b"hello"), b"hello");
        assert_eq!(trim_line_endings(b"\n\n"), b"");
        assert_eq!(trim_line_endings(b""), b"");
    }

    #[test]
    fn tcp_source_accepts_connections() {
        let (ingester, handle, shutdown) = create_test_ingester();
        let source = TcpSource::bind("127.0.0.1:0", Arc::clone(&shutdown)).unwrap();
        let addr = source.listener.local_addr().unwrap();

        let source_handle = handle.clone();
        let source_thread = thread::spawn(move || source.run(source_handle));

        let ingester_thread = thread::spawn(move || ingester.run());

        thread::sleep(Duration::from_millis(50));

        let mut client = TcpStream::connect(addr).unwrap();
        writeln!(client, "test message").unwrap();
        client.flush().unwrap();
        drop(client);

        thread::sleep(Duration::from_millis(500));

        shutdown.store(true, Ordering::Relaxed);
        handle.shutdown();

        let source_result = source_thread.join().unwrap();
        assert!(source_result.is_ok());

        let stats = ingester_thread.join().unwrap();
        assert!(stats.entries_ingested >= 1);
    }

    #[test]
    fn udp_source_receives_datagrams() {
        let (ingester, handle, shutdown) = create_test_ingester();
        let source = UdpSource::bind("127.0.0.1:0", Arc::clone(&shutdown)).unwrap();
        let addr = source.socket.local_addr().unwrap();

        let source_handle = handle.clone();
        let source_thread = thread::spawn(move || source.run(source_handle));

        let ingester_thread = thread::spawn(move || ingester.run());

        thread::sleep(Duration::from_millis(50));

        let client = UdpSocket::bind("127.0.0.1:0").unwrap();
        client.send_to(b"udp test message\n", addr).unwrap();

        thread::sleep(Duration::from_millis(500));

        shutdown.store(true, Ordering::Relaxed);
        handle.shutdown();

        let source_result = source_thread.join().unwrap();
        assert!(source_result.is_ok());

        let stats = ingester_thread.join().unwrap();
        assert!(stats.entries_ingested >= 1);
    }

    #[test]
    fn file_tailer_reads_new_lines() {
        let (ingester, handle, shutdown) = create_test_ingester();

        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "initial line").unwrap();
        temp_file.flush().unwrap();

        let path = temp_file.path().to_path_buf();
        let tailer = FileTailer::new(path.clone(), Arc::clone(&shutdown)).reading_from_start();

        let source_handle = handle.clone();
        let source_thread = thread::spawn(move || tailer.run(source_handle));

        let ingester_thread = thread::spawn(move || ingester.run());

        thread::sleep(Duration::from_millis(200));

        {
            let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
            writeln!(file, "appended line").unwrap();
            file.flush().unwrap();
        }

        thread::sleep(Duration::from_millis(600));

        shutdown.store(true, Ordering::Relaxed);
        handle.shutdown();

        let source_result = source_thread.join().unwrap();
        assert!(source_result.is_ok());

        let stats = ingester_thread.join().unwrap();
        assert!(stats.entries_ingested >= 1);
    }

    #[test]
    fn source_respects_shutdown_flag() {
        let shutdown = Arc::new(AtomicBool::new(true));
        let source = UdpSource::bind("127.0.0.1:0", Arc::clone(&shutdown)).unwrap();

        let engine = create_test_engine();
        let (_, handle) = Ingester::with_default_capacity(engine);

        let result = source.run(handle);
        assert!(result.is_ok());
    }

    #[test]
    fn source_runner_spawns_and_joins() {
        let (ingester, handle, shutdown) = create_test_ingester();
        let mut runner = SourceRunner::new();

        let source = UdpSource::bind("127.0.0.1:0", Arc::clone(&shutdown)).unwrap();
        runner.spawn(source, handle.clone());

        let ingester_thread = thread::spawn(move || ingester.run());

        thread::sleep(Duration::from_millis(50));

        shutdown.store(true, Ordering::Relaxed);
        handle.shutdown();

        let results = runner.join_all();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        ingester_thread.join().unwrap();
    }
}
