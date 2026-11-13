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

//! Bounded row-group Parquet export. Files become visible only after finish().
//! A writer belongs to one query thread. Dropping it aborts an unfinished export.
mod bridge;
mod parts;
pub use parts::{Batch, PartsWriter, Value};

use parquet::{
    basic::{Compression, LogicalType, Repetition, Type as Physical},
    data_type::{
        ByteArray, ByteArrayType, DoubleType, FixedLenByteArray, FixedLenByteArrayType, FloatType,
        Int32Type, Int64Type,
    },
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::types::Type,
};
use std::{
    collections::HashSet,
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};
use tempfile::NamedTempFile;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[derive(Clone, Copy, Debug)]
pub enum Kind {
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
    String,
    Binary,
    Date,
    Decimal { precision: u8, scale: u8 },
}
impl TryFrom<u8> for Kind {
    type Error = Box<dyn std::error::Error + Send + Sync>;
    fn try_from(v: u8) -> Result<Self> {
        Ok(match v {
            0 => Self::I8,
            1 => Self::I16,
            2 => Self::I32,
            3 => Self::I64,
            4 => Self::F32,
            5 => Self::F64,
            6 => Self::String,
            7 => Self::Binary,
            8 => Self::Date,
            _ => return Err("Unsupported Parquet column type".into()),
        })
    }
}
#[derive(Clone)]
pub struct Column {
    pub name: String,
    pub kind: Kind,
}
pub enum Cell<'a> {
    Null,
    Integer(i64),
    /// Exact unscaled decimal at the column schema's scale.
    Decimal64(i64),
    Float(f64),
    Bytes(&'a [u8]),
}
enum Buffer {
    I32(Vec<i32>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Bytes(Vec<ByteArray>),
    Decimal(Vec<FixedLenByteArray>),
}
struct BufferedColumn {
    values: Buffer,
    levels: Vec<i16>,
}
pub struct Writer {
    columns: Vec<Column>,
    buffers: Vec<BufferedColumn>,
    writer: Option<SerializedFileWriter<File>>,
    temporary: Option<NamedTempFile>,
    destination: PathBuf,
    rows: usize,
    bytes: usize,
    failed: bool,
}
const MAX_ROWS: usize = 8192;
const MAX_BYTES: usize = 8 * 1024 * 1024;

// Decimal64 avoids text conversion when the source already has an unscaled i64.
// Wider values and scale conversion retain the exact text path; neither uses floats.
fn checked_decimal64(value: i64, kind: Kind) -> Result<i128> {
    let Kind::Decimal { precision, .. } = kind else {
        return Err("Decimal cell requires a decimal schema".into());
    };
    let value = i128::from(value);
    let limit = 10i128.pow(u32::from(precision));
    if value <= -limit || value >= limit {
        return Err("Decimal value exceeds declared precision".into());
    }
    Ok(value)
}
fn decimal_value(text: &[u8], precision: u8, scale: u8) -> Result<i128> {
    let (negative, text) = match text.first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let mut value = 0i128;
    let mut fraction = None;
    let mut digits = 0usize;
    for &c in text {
        if c == b'.' && fraction.is_none() {
            fraction = Some(0usize);
        } else if c.is_ascii_digit() {
            digits += 1;
            if let Some(n) = &mut fraction {
                *n += 1;
                if *n > usize::from(scale) {
                    if c != b'0' {
                        return Err("Decimal value would lose fractional digits".into());
                    }
                    continue;
                }
            }
            value = value
                .checked_mul(10)
                .and_then(|v| v.checked_add(i128::from(c - b'0')))
                .ok_or("Decimal value overflow")?;
        } else {
            return Err("Invalid decimal text".into());
        }
    }
    if digits == 0 {
        return Err("Empty decimal value".into());
    }
    for _ in fraction.unwrap_or(0)..usize::from(scale) {
        value = value.checked_mul(10).ok_or("Decimal value overflow")?;
    }
    if value >= 10i128.pow(u32::from(precision)) {
        return Err("Decimal value exceeds declared precision".into());
    }
    if negative {
        value = -value;
    }
    Ok(value)
}

impl Writer {
    pub fn open(path: &Path, columns: Vec<Column>) -> Result<Self> {
        if columns.is_empty() || columns.len() > 1024 {
            return Err("Expected 1..1024 columns".into());
        }
        if path.as_os_str().is_empty() || path.exists() {
            return Err("Output path is empty or already exists".into());
        }
        let mut names = HashSet::new();
        let mut fields = Vec::new();
        let mut buffers = Vec::new();
        for c in &columns {
            if c.name.is_empty() || !names.insert(&c.name) {
                return Err("Column names must be nonempty and unique; use SQL aliases".into());
            }
            let (physical, logical, values) = match c.kind {
                Kind::I8 => (
                    Physical::INT32,
                    Some(LogicalType::integer(8, true)),
                    Buffer::I32(vec![]),
                ),
                Kind::I16 => (
                    Physical::INT32,
                    Some(LogicalType::integer(16, true)),
                    Buffer::I32(vec![]),
                ),
                Kind::I32 => (
                    Physical::INT32,
                    Some(LogicalType::integer(32, true)),
                    Buffer::I32(vec![]),
                ),
                Kind::I64 => (
                    Physical::INT64,
                    Some(LogicalType::integer(64, true)),
                    Buffer::I64(vec![]),
                ),
                Kind::F32 => (Physical::FLOAT, None, Buffer::F32(vec![])),
                Kind::F64 => (Physical::DOUBLE, None, Buffer::F64(vec![])),
                Kind::String => (
                    Physical::BYTE_ARRAY,
                    Some(LogicalType::String),
                    Buffer::Bytes(vec![]),
                ),
                Kind::Binary => (Physical::BYTE_ARRAY, None, Buffer::Bytes(vec![])),
                Kind::Date => (
                    Physical::INT32,
                    Some(LogicalType::Date),
                    Buffer::I32(vec![]),
                ),
                Kind::Decimal { precision, scale } => {
                    if !(1..=38).contains(&precision) || scale > precision {
                        return Err(
                            "Parquet decimal requires precision 1..38 and scale 0..precision"
                                .into(),
                        );
                    }
                    let (physical, buffer) = if precision <= 9 {
                        (Physical::INT32, Buffer::I32(vec![]))
                    } else if precision <= 18 {
                        (Physical::INT64, Buffer::I64(vec![]))
                    } else {
                        (Physical::FIXED_LEN_BYTE_ARRAY, Buffer::Decimal(vec![]))
                    };
                    (
                        physical,
                        Some(LogicalType::decimal(i32::from(scale), i32::from(precision))),
                        buffer,
                    )
                }
            };
            let mut builder = Type::primitive_type_builder(&c.name, physical)
                .with_repetition(Repetition::OPTIONAL)
                .with_logical_type(logical);
            if let Kind::Decimal { precision, scale } = c.kind {
                builder = builder
                    .with_precision(i32::from(precision))
                    .with_scale(i32::from(scale));
                if precision > 18 {
                    builder = builder.with_length(16);
                }
            }
            fields.push(Arc::new(builder.build()?));
            buffers.push(BufferedColumn {
                values,
                levels: vec![],
            });
        }
        let schema = Arc::new(
            Type::group_type_builder("query")
                .with_fields(fields)
                .build()?,
        );
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let temporary = tempfile::Builder::new()
            .prefix(".parquet-export-")
            .tempfile_in(parent)?;
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_dictionary_enabled(false)
                .build(),
        );
        let writer = SerializedFileWriter::new(temporary.reopen()?, schema, props)?;
        Ok(Self {
            columns,
            buffers,
            writer: Some(writer),
            temporary: Some(temporary),
            destination: path.to_owned(),
            rows: 0,
            bytes: 0,
            failed: false,
        })
    }
    pub fn append(&mut self, cells: &[Cell<'_>]) -> Result<()> {
        if self.failed || self.writer.is_none() {
            return Err("Parquet writer is closed or failed".into());
        }
        let result = self.append_inner(cells);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn append_inner(&mut self, cells: &[Cell<'_>]) -> Result<()> {
        if cells.len() != self.columns.len() {
            return Err("Column count mismatch".into());
        }
        let size = cells
            .iter()
            .try_fold(cells.len() * 16, |n, v| {
                n.checked_add(match v {
                    Cell::Bytes(b) => b.len(),
                    _ => 0,
                })
            })
            .ok_or("Row size overflow")?;
        if size > MAX_BYTES {
            return Err("Parquet export row exceeds 8 MiB".into());
        }
        if self.rows > 0 && self.bytes + size > MAX_BYTES {
            self.flush()?;
        }
        for ((c, b), v) in self.columns.iter().zip(&mut self.buffers).zip(cells) {
            if matches!(v, Cell::Null) {
                b.levels.push(0);
                continue;
            }
            match (&mut b.values, v) {
                (Buffer::I32(values), Cell::Decimal64(v)) => {
                    values.push(i32::try_from(checked_decimal64(*v, c.kind)?)?);
                }
                (Buffer::I64(values), Cell::Decimal64(v)) => {
                    values.push(i64::try_from(checked_decimal64(*v, c.kind)?)?);
                }
                (Buffer::Decimal(values), Cell::Decimal64(v)) => {
                    values.push(FixedLenByteArray::from(
                        checked_decimal64(*v, c.kind)?.to_be_bytes().to_vec(),
                    ));
                }
                (Buffer::I32(values), Cell::Bytes(v)) => {
                    if let Kind::Decimal { precision, scale } = c.kind {
                        values.push(i32::try_from(decimal_value(v, precision, scale)?)?);
                    } else {
                        return Err("Cell does not match Parquet schema".into());
                    }
                }
                (Buffer::I64(values), Cell::Bytes(v)) => {
                    if let Kind::Decimal { precision, scale } = c.kind {
                        values.push(i64::try_from(decimal_value(v, precision, scale)?)?);
                    } else {
                        return Err("Cell does not match Parquet schema".into());
                    }
                }
                (Buffer::I32(values), Cell::Integer(v)) => {
                    match c.kind {
                        Kind::I8 => {
                            i8::try_from(*v)?;
                        }
                        Kind::I16 => {
                            i16::try_from(*v)?;
                        }
                        Kind::Decimal { .. } => return Err("Decimal requires exact text".into()),
                        _ => {}
                    }
                    values.push(i32::try_from(*v)?);
                }
                (Buffer::I64(values), Cell::Integer(v)) => {
                    if matches!(c.kind, Kind::Decimal { .. }) {
                        return Err("Decimal requires exact text".into());
                    }
                    values.push(*v);
                }
                (Buffer::F32(values), Cell::Float(v)) => values.push(*v as f32),
                (Buffer::F64(values), Cell::Float(v)) => values.push(*v),
                (Buffer::Bytes(values), Cell::Bytes(v)) => {
                    if matches!(c.kind, Kind::String) {
                        std::str::from_utf8(v)?;
                    }
                    values.push(ByteArray::from(v.to_vec()));
                }
                (Buffer::Decimal(values), Cell::Bytes(v)) => {
                    if let Kind::Decimal { precision, scale } = c.kind {
                        values.push(FixedLenByteArray::from(
                            decimal_value(v, precision, scale)?.to_be_bytes().to_vec(),
                        ));
                    } else {
                        return Err("Invalid decimal schema".into());
                    }
                }
                _ => return Err("Cell does not match Parquet schema".into()),
            }
            b.levels.push(1);
        }
        self.rows += 1;
        self.bytes += size;
        if self.rows >= MAX_ROWS {
            self.flush()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let mut group = self
            .writer
            .as_mut()
            .ok_or("Writer closed")?
            .next_row_group()?;
        for b in &mut self.buffers {
            let mut column = group.next_column()?.ok_or("Missing Parquet column")?;
            macro_rules! write {
                ($t:ty,$v:expr) => {
                    column
                        .typed::<$t>()
                        .write_batch($v, Some(&b.levels), None)?;
                    $v.clear();
                };
            }
            match &mut b.values {
                Buffer::I32(v) => {
                    write!(Int32Type, v);
                }
                Buffer::I64(v) => {
                    write!(Int64Type, v);
                }
                Buffer::F32(v) => {
                    write!(FloatType, v);
                }
                Buffer::F64(v) => {
                    write!(DoubleType, v);
                }
                Buffer::Bytes(v) => {
                    write!(ByteArrayType, v);
                }
                Buffer::Decimal(v) => {
                    write!(FixedLenByteArrayType, v);
                }
            }
            column.close()?;
            b.levels.clear();
        }
        group.close()?;
        self.rows = 0;
        self.bytes = 0;
        Ok(())
    }
    pub fn finish(&mut self) -> Result<()> {
        if self.failed || self.writer.is_none() {
            return Err("Parquet writer is closed or failed".into());
        }
        self.failed = true; // Any finalization error permanently prevents publication.
        self.flush()?;
        self.writer.take().ok_or("Writer closed")?.close()?;
        let temporary = self.temporary.take().ok_or("Temporary file missing")?;
        temporary.as_file().sync_all()?;
        // Atomic no-clobber publication in the same directory. Drop removes the
        // temporary file on any error, including a destination created meanwhile.
        match temporary.persist_noclobber(&self.destination) {
            Ok(_file) => Ok(()),
            Err(error) => Err(error.error.into()),
        }
    }
}
