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

use crate::{Batch, Column, Kind, PartsWriter, Value, Writer};

#[cxx::bridge(namespace = "oceanbase::parquet_export")]
mod ffi {
    struct ColumnSpec {
        name: String,
        kind: u8,
        precision: u8,
        scale: u8,
    }
    struct BatchCell {
        is_null: bool,
        decimal_integer: bool,
        integer: i64,
        floating: f64,
        offset: u32,
        length: u32,
    }
    extern "Rust" {
        type PartsWriter;
        fn open_parts(
            path: &str,
            columns: &[ColumnSpec],
            workers: usize,
            part_bytes: usize,
        ) -> Result<Box<PartsWriter>>;
        fn submit_parts(writer: &mut PartsWriter, cells: &[BatchCell], bytes: &[u8]) -> Result<()>;
        fn poll_parts(writer: &mut PartsWriter) -> Result<Vec<String>>;
        fn parts_ready(writer: &PartsWriter) -> bool;
        fn parts_done(writer: &PartsWriter) -> bool;
        fn finish_parts_input(writer: &mut PartsWriter) -> Result<()>;

        type Writer;
        fn open_writer(path: &str, columns: &[ColumnSpec]) -> Result<Box<Writer>>;
        fn append_batch(writer: &mut Writer, cells: &[BatchCell], bytes: &[u8]) -> Result<()>;
        fn finish_writer(writer: &mut Writer) -> Result<()>;
    }
}
fn open_writer(path: &str, columns: &[ffi::ColumnSpec]) -> crate::Result<Box<Writer>> {
    let columns = column_specs(columns)?;
    Ok(Box::new(Writer::open(std::path::Path::new(path), columns)?))
}
fn column_specs(columns: &[ffi::ColumnSpec]) -> crate::Result<Vec<Column>> {
    columns
        .iter()
        .map(|c| {
            Ok(Column {
                name: c.name.clone(),
                kind: if c.kind == 9 {
                    Kind::Decimal {
                        precision: c.precision,
                        scale: c.scale,
                    }
                } else {
                    Kind::try_from(c.kind)?
                },
            })
        })
        .collect::<crate::Result<Vec<_>>>()
}
fn append_batch(writer: &mut Writer, cells: &[ffi::BatchCell], bytes: &[u8]) -> crate::Result<()> {
    let batch = batch_from_cells(&writer.columns, cells, bytes)?;
    writer.append_batch(&batch)
}
fn finish_writer(writer: &mut Writer) -> crate::Result<()> {
    writer.finish()
}

fn open_parts(
    path: &str,
    columns: &[ffi::ColumnSpec],
    workers: usize,
    part_bytes: usize,
) -> crate::Result<Box<PartsWriter>> {
    Ok(Box::new(PartsWriter::open(
        std::path::Path::new(path),
        column_specs(columns)?,
        workers,
        part_bytes,
    )?))
}
fn submit_parts(
    writer: &mut PartsWriter,
    cells: &[ffi::BatchCell],
    bytes: &[u8],
) -> crate::Result<()> {
    writer.submit(batch_from_cells(writer.columns(), cells, bytes)?)
}
fn batch_from_cells(
    columns: &[Column],
    cells: &[ffi::BatchCell],
    bytes: &[u8],
) -> crate::Result<Batch> {
    let width = columns.len();
    if width == 0
        || cells.is_empty()
        || cells.len() % width != 0
        || cells.len() / width > 4096
        || bytes.len() > crate::MAX_BYTES
        || cells.len() * std::mem::size_of::<Value>() > crate::MAX_BYTES
    {
        return Err("Invalid or oversized Parquet batch".into());
    }
    let values = cells
        .iter()
        .zip(columns.iter().cycle())
        .map(|(v, c)| {
            Ok(if v.is_null {
                Value::Null
            } else if v.decimal_integer {
                if !matches!(c.kind, Kind::Decimal { .. }) {
                    return Err("Decimal integer requires a decimal schema".into());
                }
                Value::Decimal64(v.integer)
            } else {
                match c.kind {
                    Kind::I8 | Kind::I16 | Kind::I32 | Kind::I64 | Kind::Date => {
                        Value::Integer(v.integer)
                    }
                    Kind::F32 | Kind::F64 => Value::Float(v.floating),
                    _ => Value::Bytes {
                        offset: v.offset as usize,
                        length: v.length as usize,
                    },
                }
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Batch::new(values, bytes.to_vec(), width)
}
fn poll_parts(writer: &mut PartsWriter) -> crate::Result<Vec<String>> {
    writer
        .poll()?
        .into_iter()
        .map(|p| {
            p.into_os_string()
                .into_string()
                .map_err(|_| "Parquet path is not UTF-8".into())
        })
        .collect()
}
fn parts_ready(writer: &PartsWriter) -> bool {
    writer.ready()
}
fn parts_done(writer: &PartsWriter) -> bool {
    writer.done()
}
fn finish_parts_input(writer: &mut PartsWriter) -> crate::Result<()> {
    writer.finish_input()
}
