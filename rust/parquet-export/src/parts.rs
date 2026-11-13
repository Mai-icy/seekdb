// Copyright (c) 2026 OceanBase.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A bounded producer/consumer pipeline. Only owned batches cross threads;
//! database sessions and read snapshots remain on the calling query thread.
use crate::{Cell, Column, Result, Writer, MAX_BYTES};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
};
use tempfile::TempDir;

#[derive(Clone, Copy)]
pub enum Value {
    Null,
    Integer(i64),
    Decimal64(i64),
    Float(f64),
    Bytes { offset: usize, length: usize },
}

/// Row-major values with one owned byte arena. Construction checks offsets and
/// bounds before any worker accepts the batch; SQL rows never borrow this arena.
pub struct Batch {
    values: Vec<Value>,
    bytes: Vec<u8>,
    width: usize,
}
impl Batch {
    pub fn new(values: Vec<Value>, bytes: Vec<u8>, width: usize) -> Result<Self> {
        if width == 0
            || width > 1024
            || values.is_empty()
            || values.len() % width != 0
            || values.len() / width > 4096
            || bytes.len() > MAX_BYTES
            || values.len() * std::mem::size_of::<Value>() > MAX_BYTES
        {
            return Err("Invalid or oversized Parquet batch".into());
        }
        for row in values.chunks_exact(width) {
            let mut size = width * 16;
            for value in row {
                if let Value::Bytes { offset, length } = value {
                    if *offset > bytes.len() || *length > bytes.len() - *offset {
                        return Err("Parquet batch byte range is out of bounds".into());
                    }
                    size = size
                        .checked_add(*length)
                        .ok_or("Parquet row size overflow")?;
                }
            }
            if size > MAX_BYTES {
                return Err("Parquet export row exceeds 8 MiB".into());
            }
        }
        Ok(Self {
            values,
            bytes,
            width,
        })
    }
}

impl Writer {
    /// Append a validated owned batch to a single-file writer. The row scratch
    /// buffer is reused for the whole batch; cell bytes are borrowed until append
    /// copies them into the writer's bounded column buffers.
    pub fn append_batch(&mut self, batch: &Batch) -> Result<()> {
        if batch.width != self.columns.len() {
            self.failed = true;
            return Err("Parquet batch has a mismatching schema".into());
        }
        let mut cells = Vec::with_capacity(batch.width);
        for row in batch.values.chunks_exact(batch.width) {
            for value in row {
                cells.push(match value {
                    Value::Null => Cell::Null,
                    Value::Integer(v) => Cell::Integer(*v),
                    Value::Decimal64(v) => Cell::Decimal64(*v),
                    Value::Float(v) => Cell::Float(*v),
                    Value::Bytes { offset, length } => {
                        Cell::Bytes(&batch.bytes[*offset..*offset + *length])
                    }
                });
            }
            self.append(&cells)?;
            cells.clear();
        }
        Ok(())
    }
}

enum Completion {
    File(PathBuf),
    Done(Result<()>),
}

/// Each worker owns its writer and produces independent, complete files. The
/// queue holds at most two batches per worker plus one pending producer batch.
/// poll() never waits, so callers can check query cancellation under backpressure.
/// Dropping joins all workers before removing every file in the private directory.
pub struct PartsWriter {
    directory: TempDir,
    sender: Option<SyncSender<Batch>>,
    pending: Option<Batch>,
    completed: Receiver<Completion>,
    workers: Vec<JoinHandle<()>>,
    cancelled: Arc<AtomicBool>,
    remaining: usize,
    width: usize,
    has_rows: bool,
    columns: Vec<Column>,
    failed: bool,
    empty_pending: Option<PathBuf>,
}
impl PartsWriter {
    pub fn open(
        parent: &Path,
        columns: Vec<Column>,
        workers: usize,
        part_bytes: usize,
    ) -> Result<Self> {
        if !(1..=16).contains(&workers) || !(8192..=1024 * 1024 * 1024).contains(&part_bytes) {
            return Err("Invalid Parquet worker count or part size".into());
        }
        let directory = tempfile::Builder::new()
            .prefix("parts-")
            .tempdir_in(parent)?;
        // Validate schema even for an empty input and before starting threads.
        drop(Writer::open(
            &directory.path().join("validate.parquet"),
            columns.clone(),
        )?);
        let (sender, receiver) = mpsc::sync_channel(workers * 2);
        let receiver = Arc::new(Mutex::new(receiver));
        let (completed_sender, completed) = mpsc::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut this = Self {
            directory,
            sender: Some(sender),
            pending: None,
            completed,
            workers: vec![],
            cancelled,
            remaining: 0,
            width: columns.len(),
            has_rows: false,
            columns,
            failed: false,
            empty_pending: None,
        };
        for index in 0..workers {
            let receiver = Arc::clone(&receiver);
            let completed = completed_sender.clone();
            let cancelled = Arc::clone(&this.cancelled);
            let columns = this.columns.clone();
            let directory = this.directory.path().to_owned();
            let worker = thread::Builder::new()
                .name(format!("parquet-{index}"))
                .spawn(move || {
                    let result = write_parts(
                        index, &directory, &columns, part_bytes, &receiver, &completed, &cancelled,
                    );
                    if result.is_err() {
                        cancelled.store(true, Ordering::Release);
                    }
                    let _ = completed.send(Completion::Done(result));
                })?;
            this.workers.push(worker);
            this.remaining += 1;
        }
        Ok(this)
    }
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
    pub fn submit(&mut self, batch: Batch) -> Result<()> {
        if self.failed
            || self.sender.is_none()
            || self.pending.is_some()
            || batch.width != self.width
        {
            return Err("Parquet pipeline is closed, busy, or has a mismatching schema".into());
        }
        self.has_rows = true;
        self.pending = Some(batch);
        Ok(())
    }
    pub fn ready(&self) -> bool {
        !self.failed && self.sender.is_some() && self.pending.is_none()
    }
    pub fn done(&self) -> bool {
        !self.failed && self.sender.is_none() && self.remaining == 0
    }
    pub fn poll(&mut self) -> Result<Vec<PathBuf>> {
        let result = self.poll_inner();
        if result.is_err() {
            self.failed = true;
            self.cancelled.store(true, Ordering::Release);
        }
        result
    }
    fn poll_inner(&mut self) -> Result<Vec<PathBuf>> {
        if self.failed {
            return Err("Parquet pipeline failed".into());
        }
        let mut files: Vec<PathBuf> = self.empty_pending.take().into_iter().collect();
        loop {
            match self.completed.try_recv() {
                Ok(Completion::File(path)) => files.push(path),
                Ok(Completion::Done(result)) => {
                    self.remaining -= 1;
                    result?;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) if self.remaining == 0 => break,
                Err(TryRecvError::Disconnected) => {
                    return Err("Parquet worker exited unexpectedly".into())
                }
            }
        }
        if let Some(batch) = self.pending.take() {
            match self
                .sender
                .as_ref()
                .ok_or("Parquet pipeline is closed")?
                .try_send(batch)
            {
                Ok(()) => {}
                Err(TrySendError::Full(batch)) => self.pending = Some(batch),
                Err(TrySendError::Disconnected(_)) => {
                    return Err("Parquet workers disconnected".into())
                }
            }
        }
        Ok(files)
    }
    /// Call only after pending batches have been accepted. Completion is observed
    /// through poll()/done(), allowing cancellation while writers close and fsync.
    pub fn finish_input(&mut self) -> Result<()> {
        if !self.ready() {
            return Err("Parquet pipeline is closed or has a pending batch".into());
        }
        if !self.has_rows {
            let path = self.directory.path().join("empty.parquet");
            let result = Writer::open(&path, self.columns.clone()).and_then(|mut w| w.finish());
            if let Err(e) = result {
                self.failed = true;
                return Err(e);
            }
            self.empty_pending = Some(path);
        }
        self.sender.take();
        Ok(())
    }
}
impl Drop for PartsWriter {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        self.pending.take();
        self.sender.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
        // TempDir drops only after all workers and writers have stopped.
    }
}

fn write_parts(
    index: usize,
    directory: &Path,
    columns: &[Column],
    limit: usize,
    receiver: &Mutex<Receiver<Batch>>,
    completed: &mpsc::Sender<Completion>,
    cancelled: &AtomicBool,
) -> Result<()> {
    let mut writer: Option<Writer> = None;
    let mut part = 0;
    let mut bytes = 0;
    let mut path = PathBuf::new();
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Ok(());
        }
        let batch = receiver
            .lock()
            .map_err(|_| "Parquet queue poisoned")?
            .recv();
        let Ok(batch) = batch else {
            break;
        };
        let mut cells = Vec::with_capacity(columns.len());
        for row in batch.values.chunks_exact(batch.width) {
            if cancelled.load(Ordering::Acquire) {
                return Ok(());
            }
            if writer.is_none() {
                path = directory.join(format!("part-{index}-{part}.parquet"));
                writer = Some(Writer::open(&path, columns.to_vec())?);
                bytes = 0;
                part += 1;
            }
            let mut row_bytes = row.len() * 16;
            for value in row {
                cells.push(match value {
                    Value::Null => Cell::Null,
                    Value::Integer(v) => Cell::Integer(*v),
                    Value::Decimal64(v) => Cell::Decimal64(*v),
                    Value::Float(v) => Cell::Float(*v),
                    Value::Bytes { offset, length } => {
                        row_bytes += length;
                        Cell::Bytes(&batch.bytes[*offset..*offset + *length])
                    }
                });
            }
            writer
                .as_mut()
                .ok_or("Parquet writer missing")?
                .append(&cells)?;
            cells.clear();
            bytes += row_bytes;
            if bytes >= limit {
                writer.take().ok_or("Parquet writer missing")?.finish()?;
                completed.send(Completion::File(path.clone()))?;
            }
        }
    }
    if !cancelled.load(Ordering::Acquire) {
        if let Some(mut writer) = writer {
            writer.finish()?;
            completed.send(Completion::File(path))?;
        }
    }
    Ok(())
}
