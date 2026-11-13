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

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet_export::{Cell, Column, Kind, Writer};
use std::fs::File;
fn columns() -> Vec<Column> {
    vec![
        Column {
            name: "id".into(),
            kind: Kind::I64,
        },
        Column {
            name: "label".into(),
            kind: Kind::String,
        },
    ]
}
#[test]
fn publishes_complete_multigroup_file_only_at_finish() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("data.parquet");
    let mut writer = Writer::open(&path, columns()).unwrap();
    for i in 0..17000 {
        writer
            .append(&[
                Cell::Integer(i),
                if i % 2 == 0 {
                    Cell::Null
                } else {
                    Cell::Bytes("中文".as_bytes())
                },
            ])
            .unwrap();
    }
    assert!(!path.exists());
    writer.finish().unwrap();
    let reader = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 17000);
    assert_eq!(reader.num_row_groups(), 3);
    assert_eq!(reader.get_row_iter(None).unwrap().count(), 17000);
}
#[test]
fn aborted_and_invalid_exports_leave_no_files() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("data.parquet");
    {
        let mut w = Writer::open(&p, columns()).unwrap();
        w.append(&[Cell::Integer(1), Cell::Bytes(b"x")]).unwrap();
    }
    assert_eq!(d.path().read_dir().unwrap().count(), 0);
    {
        let mut w = Writer::open(&p, columns()).unwrap();
        assert!(w.append(&[Cell::Integer(1), Cell::Bytes(&[255])]).is_err());
        assert!(w.finish().is_err());
    }
    assert_eq!(d.path().read_dir().unwrap().count(), 0);
}
#[test]
fn existing_or_racing_destination_is_preserved() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("data.parquet");
    let mut w = Writer::open(&p, columns()).unwrap();
    std::fs::write(&p, b"original").unwrap();
    assert!(w.finish().is_err());
    drop(w);
    assert_eq!(std::fs::read(&p).unwrap(), b"original");
    assert!(Writer::open(&p, columns()).is_err());
    assert_eq!(d.path().read_dir().unwrap().count(), 1);
}
#[test]
fn empty_export_has_schema_and_zero_rows() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("data.parquet");
    Writer::open(&p, columns()).unwrap().finish().unwrap();
    let r = SerializedFileReader::new(File::open(p).unwrap()).unwrap();
    assert_eq!(r.metadata().file_metadata().num_rows(), 0);
    assert_eq!(r.metadata().file_metadata().schema_descr().num_columns(), 2);
}

#[test]
fn decimal_schema_and_exact_values() {
    use parquet::record::RowAccessor;
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("decimal.parquet");
    let mut w = Writer::open(
        &p,
        vec![Column {
            name: "value".into(),
            kind: Kind::Decimal {
                precision: 38,
                scale: 2,
            },
        }],
    )
    .unwrap();
    let values = [
        "999999999999999999999999999999999999.99",
        "-999999999999999999999999999999999999.99",
        "-0.01",
        "123.4500",
        "0",
    ];
    for v in values {
        w.append(&[Cell::Bytes(v.as_bytes())]).unwrap();
    }
    w.append(&[Cell::Null]).unwrap();
    w.finish().unwrap();
    let reader = SerializedFileReader::new(File::open(p).unwrap()).unwrap();
    let schema = reader.metadata().file_metadata().schema_descr().column(0);
    assert_eq!(schema.type_precision(), 38);
    assert_eq!(schema.type_scale(), 2);
    let expected = [10i128.pow(38) - 1, -(10i128.pow(38) - 1), -1, 12345, 0];
    let rows = reader
        .get_row_iter(None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for (row, expected) in rows.iter().zip(expected) {
        assert_eq!(row.get_decimal(0).unwrap().data(), &expected.to_be_bytes());
    }
    assert!(matches!(
        rows[5].get_column_iter().next().unwrap().1,
        parquet::record::Field::Null
    ));
}

#[test]
fn decimal_rejects_loss_overflow_and_invalid_schema() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("decimal.parquet");
    for text in [
        "1000",
        "-1000",
        "1.001",
        "NaN",
        "1e2",
        "",
        ".",
        "1.2.3",
        "999999999999999999999999999999999999999999999",
    ] {
        let mut w = Writer::open(
            &p,
            vec![Column {
                name: "value".into(),
                kind: Kind::Decimal {
                    precision: 5,
                    scale: 2,
                },
            }],
        )
        .unwrap();
        assert!(w.append(&[Cell::Bytes(text.as_bytes())]).is_err(), "{text}");
        assert!(w.finish().is_err());
        drop(w);
        assert_eq!(d.path().read_dir().unwrap().count(), 0);
    }
    for (precision, scale) in [(0, 0), (39, 0), (2, 3)] {
        assert!(Writer::open(
            &p,
            vec![Column {
                name: "value".into(),
                kind: Kind::Decimal { precision, scale },
            }]
        )
        .is_err());
    }
}

#[test]
fn decimal_physical_width_matches_precision_without_rounding() {
    use parquet::{basic::Type, record::RowAccessor};
    let d = tempfile::tempdir().unwrap();
    for (precision, physical, width) in [
        (9, Type::INT32, 4),
        (18, Type::INT64, 8),
        (19, Type::FIXED_LEN_BYTE_ARRAY, 16),
        (38, Type::FIXED_LEN_BYTE_ARRAY, 16),
    ] {
        let p = d.path().join(format!("decimal-{precision}.parquet"));
        let mut w = Writer::open(
            &p,
            vec![Column {
                name: "value".into(),
                kind: Kind::Decimal {
                    precision,
                    scale: 0,
                },
            }],
        )
        .unwrap();
        let value = -(10i128.pow(u32::from(precision)) - 1);
        w.append(&[Cell::Bytes(value.to_string().as_bytes())])
            .unwrap();
        w.finish().unwrap();
        let reader = SerializedFileReader::new(File::open(p).unwrap()).unwrap();
        assert_eq!(
            reader
                .metadata()
                .file_metadata()
                .schema_descr()
                .column(0)
                .physical_type(),
            physical
        );
        let row = reader.get_row_iter(None).unwrap().next().unwrap().unwrap();
        assert_eq!(
            row.get_decimal(0).unwrap().data(),
            &value.to_be_bytes()[16 - width..]
        );
    }
}

fn pipeline_batch(start: i64, count: i64) -> parquet_export::Batch {
    use parquet_export::{Batch, Value};
    let mut values = Vec::new();
    let mut bytes = Vec::new();
    for id in start..start + count {
        values.push(Value::Integer(id));
        if id % 3 == 0 {
            values.push(Value::Null);
        } else {
            let text = format!("行\0{id}");
            values.push(Value::Bytes {
                offset: bytes.len(),
                length: text.len(),
            });
            bytes.extend_from_slice(text.as_bytes());
        }
    }
    Batch::new(values, bytes, 2).unwrap()
}

#[test]
fn parallel_parts_preserve_every_row_and_null_and_cleanup_after_consumption() {
    use parquet::record::{Field, RowAccessor};
    use parquet_export::PartsWriter;
    use std::{
        thread,
        time::{Duration, Instant},
    };
    for workers in [1, 4, 8] {
        let root = tempfile::tempdir().unwrap();
        let mut writer = PartsWriter::open(root.path(), columns(), workers, 8192).unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut files = Vec::new();
        for batch in 0..30 {
            writer.submit(pipeline_batch(batch * 1000, 1000)).unwrap();
            loop {
                files.extend(writer.poll().unwrap());
                if writer.ready() {
                    break;
                }
                assert!(Instant::now() < deadline);
                thread::sleep(Duration::from_millis(1));
            }
        }
        writer.finish_input().unwrap();
        loop {
            files.extend(writer.poll().unwrap());
            if writer.done() {
                break;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        assert!(files.len() > workers);
        let mut ids = std::collections::BTreeSet::new();
        for path in files {
            let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
            for row in reader.get_row_iter(None).unwrap() {
                let row = row.unwrap();
                let id = row.get_long(0).unwrap();
                assert!(ids.insert(id), "duplicate row {id}");
                if id % 3 == 0 {
                    assert!(matches!(
                        row.get_column_iter().nth(1).unwrap().1,
                        Field::Null
                    ));
                } else {
                    assert_eq!(row.get_string(1).unwrap(), &format!("行\0{id}"));
                }
            }
        }
        assert_eq!(
            ids.into_iter().collect::<Vec<_>>(),
            (0..30000).collect::<Vec<_>>()
        );
        drop(writer);
        assert_eq!(root.path().read_dir().unwrap().count(), 0);
    }
}

#[test]
fn parallel_empty_input_has_one_complete_file() {
    use parquet_export::PartsWriter;
    let root = tempfile::tempdir().unwrap();
    let mut writer = PartsWriter::open(root.path(), columns(), 4, 8192).unwrap();
    writer.finish_input().unwrap();
    let mut files = Vec::new();
    for _ in 0..10000 {
        files.extend(writer.poll().unwrap());
        if writer.done() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert!(writer.done());
    assert_eq!(files.len(), 1);
    let reader = SerializedFileReader::new(File::open(&files[0]).unwrap()).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 0);
    assert_eq!(
        reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .num_columns(),
        2
    );
    drop(writer);
    assert!(!files[0].exists());
}

#[test]
fn parallel_failure_and_cancellation_remove_all_parts_and_join_workers() {
    use parquet_export::{Batch, PartsWriter, Value};
    let root = tempfile::tempdir().unwrap();
    {
        let mut writer = PartsWriter::open(root.path(), columns(), 4, 8192).unwrap();
        for n in 0..16 {
            writer.submit(pipeline_batch(n * 4096, 4096)).unwrap();
            while !writer.ready() {
                writer.poll().unwrap();
            }
        }
        // Drop with accepted and in-flight work, without finish_input().
    }
    assert_eq!(root.path().read_dir().unwrap().count(), 0);
    {
        let mut writer = PartsWriter::open(root.path(), columns(), 4, 8192).unwrap();
        writer
            .submit(
                Batch::new(
                    vec![
                        Value::Integer(1),
                        Value::Bytes {
                            offset: 0,
                            length: 1,
                        },
                    ],
                    vec![255],
                    2,
                )
                .unwrap(),
            )
            .unwrap();
        let mut failed = false;
        for _ in 0..10000 {
            if writer.poll().is_err() {
                failed = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(failed);
        assert!(!writer.done());
        assert!(writer.finish_input().is_err());
    }
    assert_eq!(root.path().read_dir().unwrap().count(), 0);
    assert!(Batch::new(
        vec![Value::Bytes {
            offset: usize::MAX,
            length: 1
        }],
        vec![0],
        1
    )
    .is_err());
    assert!(Batch::new(vec![Value::Null], vec![], 2).is_err());
}

#[test]
fn decimal_integer_and_text_paths_have_identical_values_and_schema() {
    use parquet::record::RowAccessor;
    for precision in [9u8, 18, 38] {
        let root = tempfile::tempdir().unwrap();
        let schema = vec![Column {
            name: "value".into(),
            kind: Kind::Decimal {
                precision,
                scale: 2,
            },
        }];
        let bound = if precision == 9 {
            999_999_999
        } else {
            i64::MAX
        };
        let values = [0i64, 1, -1, 12345, -12345, bound, -bound];
        let text_path = root.path().join("text.parquet");
        let integer_path = root.path().join("integer.parquet");
        let mut text = Writer::open(&text_path, schema.clone()).unwrap();
        let mut integer = Writer::open(&integer_path, schema).unwrap();
        for value in values {
            // precision 18 cannot represent i64::MAX; retain the full i64 range
            // in the precision-38 case, including i64::MIN below.
            let value = if precision == 18 { value / 10 } else { value };
            let magnitude = i128::from(value).abs();
            let formatted = format!(
                "{}{}.{:02}",
                if value < 0 { "-" } else { "" },
                magnitude / 100,
                magnitude % 100
            );
            text.append(&[Cell::Bytes(formatted.as_bytes())]).unwrap();
            integer.append(&[Cell::Decimal64(value)]).unwrap();
        }
        if precision == 38 {
            text.append(&[Cell::Bytes(b"-92233720368547758.08")])
                .unwrap();
            integer.append(&[Cell::Decimal64(i64::MIN)]).unwrap();
        }
        text.append(&[Cell::Null]).unwrap();
        integer.append(&[Cell::Null]).unwrap();
        text.finish().unwrap();
        integer.finish().unwrap();
        let a = SerializedFileReader::new(File::open(text_path).unwrap()).unwrap();
        let b = SerializedFileReader::new(File::open(integer_path).unwrap()).unwrap();
        assert_eq!(
            a.metadata().file_metadata().schema_descr(),
            b.metadata().file_metadata().schema_descr()
        );
        let a = a
            .get_row_iter(None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let b = b
            .get_row_iter(None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(a, b);
        assert!(a[0].get_decimal(0).is_ok());
    }
}

#[test]
fn decimal_integer_rejects_precision_overflow_and_non_decimal_schema() {
    for (kind, values) in [
        (
            Kind::Decimal {
                precision: 2,
                scale: 2,
            },
            vec![-100i64, 100, i64::MIN, i64::MAX],
        ),
        (Kind::I64, vec![1]),
        (Kind::Date, vec![1]),
    ] {
        for value in values {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("invalid.parquet");
            let mut w = Writer::open(
                &path,
                vec![Column {
                    name: "v".into(),
                    kind,
                }],
            )
            .unwrap();
            assert!(w.append(&[Cell::Decimal64(value)]).is_err());
            assert!(w.finish().is_err());
            drop(w);
            assert_eq!(root.path().read_dir().unwrap().count(), 0);
        }
    }
}

#[test]
fn parallel_decimal_integer_preserves_signed_values_and_nulls() {
    use parquet::record::RowAccessor;
    use parquet_export::{Batch, PartsWriter, Value};
    let root = tempfile::tempdir().unwrap();
    let columns = vec![Column {
        name: "v".into(),
        kind: Kind::Decimal {
            precision: 18,
            scale: 2,
        },
    }];
    let mut writer = PartsWriter::open(root.path(), columns, 4, 8192).unwrap();
    let values = (0..4096)
        .map(|i| {
            if i % 7 == 0 {
                Value::Null
            } else {
                Value::Decimal64(i - 2048)
            }
        })
        .collect();
    writer
        .submit(Batch::new(values, vec![], 1).unwrap())
        .unwrap();
    let mut files = Vec::new();
    while !writer.ready() {
        files.extend(writer.poll().unwrap());
    }
    writer.finish_input().unwrap();
    while !writer.done() {
        files.extend(writer.poll().unwrap());
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let mut actual = Vec::new();
    for p in files {
        let reader = SerializedFileReader::new(File::open(p).unwrap()).unwrap();
        for row in reader.get_row_iter(None).unwrap() {
            let row = row.unwrap();
            actual.push(
                row.get_decimal(0)
                    .ok()
                    .map(|d| i64::from_be_bytes(d.data().try_into().unwrap())),
            );
        }
    }
    let mut expected: Vec<_> = (0..4096)
        .map(|i| if i % 7 == 0 { None } else { Some(i - 2048) })
        .collect();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
    drop(writer);
    assert_eq!(root.path().read_dir().unwrap().count(), 0);
}

// Targeted writer benchmark; does not include SQL scanning or network transfer.
#[test]
#[ignore]
fn benchmark_decimal_encodings() {
    use std::time::Instant;
    let root = tempfile::tempdir().unwrap();
    let schema: Vec<_> = (0..4)
        .map(|i| Column {
            name: format!("d{i}"),
            kind: Kind::Decimal {
                precision: 15,
                scale: 2,
            },
        })
        .collect();
    let text: Vec<_> = (0..4096i64)
        .map(|v| format!("{}.{:02}", v / 100, v % 100))
        .collect();
    for round in 0..3 {
        for binary in if round % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let path = root.path().join("bench.parquet");
            let started = Instant::now();
            let mut w = Writer::open(&path, schema.clone()).unwrap();
            for i in 0..2_000_000 {
                let value = (i % 4096) as i64;
                if binary {
                    w.append(&[
                        Cell::Decimal64(value),
                        Cell::Decimal64(value),
                        Cell::Decimal64(value),
                        Cell::Decimal64(value),
                    ])
                    .unwrap();
                } else {
                    let value = text[value as usize].as_bytes();
                    w.append(&[
                        Cell::Bytes(value),
                        Cell::Bytes(value),
                        Cell::Bytes(value),
                        Cell::Bytes(value),
                    ])
                    .unwrap();
                }
            }
            w.finish().unwrap();
            let seconds = started.elapsed().as_secs_f64();
            drop(w);
            println!("{{\"round\":{round},\"integer\":{binary},\"rows\":2000000,\"columns\":4,\"seconds\":{seconds}}}");
            std::fs::remove_file(path).unwrap();
        }
    }
}

#[test]
fn single_file_batches_preserve_order_and_borrowed_bytes() {
    use parquet::record::{Field, RowAccessor};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("batch.parquet");
    let mut writer = Writer::open(&path, columns()).unwrap();
    for batch in 0..5 {
        // The caller can drop each batch immediately after append returns.
        writer
            .append_batch(&pipeline_batch(batch * 4096, 4096))
            .unwrap();
    }
    writer.append_batch(&pipeline_batch(20480, 7)).unwrap();
    writer.finish().unwrap();
    let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 20487);
    for (i, row) in reader.get_row_iter(None).unwrap().enumerate() {
        let row = row.unwrap();
        assert_eq!(row.get_long(0).unwrap(), i as i64);
        if i % 3 == 0 {
            assert!(matches!(
                row.get_column_iter().nth(1).unwrap().1,
                Field::Null
            ));
        } else {
            assert_eq!(row.get_string(1).unwrap(), &format!("行\0{i}"));
        }
    }
}

#[test]
fn failed_batch_cannot_publish_a_partial_file() {
    use parquet_export::{Batch, Value};
    for bad in [
        Batch::new(vec![Value::Integer(1)], vec![], 1).unwrap(),
        Batch::new(
            vec![
                Value::Integer(1),
                Value::Bytes {
                    offset: 0,
                    length: 1,
                },
            ],
            vec![255],
            2,
        )
        .unwrap(),
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("bad.parquet");
        let mut writer = Writer::open(&path, columns()).unwrap();
        writer.append_batch(&pipeline_batch(0, 4096)).unwrap();
        assert!(writer.append_batch(&bad).is_err());
        assert!(writer.finish().is_err());
        drop(writer);
        assert_eq!(root.path().read_dir().unwrap().count(), 0);
    }
}
