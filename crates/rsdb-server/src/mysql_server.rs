use std::io;
use std::net::TcpListener;
use std::thread;

use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;
use chrono::NaiveDate;
use msql_srv::{
    Column, ColumnFlags, ColumnType, ErrorKind, InitWriter, MysqlIntermediary, MysqlShim,
    ParamParser, QueryResultWriter, StatementMetaWriter,
};

use rsdb_common::NodeAddr;
use rsdb_coordinator::Coordinator;

pub fn spawn_mysql_server(
    addr: NodeAddr,
    coordinator: std::sync::Arc<tokio::sync::Mutex<Coordinator>>,
) -> io::Result<thread::JoinHandle<()>> {
    let bind_addr = addr.to_string();
    let listener = TcpListener::bind(&bind_addr)?;

    let handle = tokio::runtime::Handle::current();
    let jh = thread::spawn(move || {
        tracing::info!("MySQL protocol server listening on {}", bind_addr);
        for incoming in listener.incoming() {
            match incoming {
                Ok(stream) => {
                    let backend = RsdbMysqlBackend {
                        handle: handle.clone(),
                        coordinator: coordinator.clone(),
                        current_db: "default".to_string(),
                    };
                    thread::spawn(move || {
                        if let Err(e) = MysqlIntermediary::run_on_tcp(backend, stream) {
                            tracing::warn!("MySQL connection ended with error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!("MySQL accept error: {}", e);
                    break;
                }
            }
        }
    });

    Ok(jh)
}

struct RsdbMysqlBackend {
    handle: tokio::runtime::Handle,
    coordinator: std::sync::Arc<tokio::sync::Mutex<Coordinator>>,
    current_db: String,
}

impl<W: io::Read + io::Write> MysqlShim<W> for RsdbMysqlBackend {
    type Error = io::Error;

    fn on_prepare(&mut self, _query: &str, info: StatementMetaWriter<'_, W>) -> io::Result<()> {
        info.error(
            ErrorKind::ER_NOT_SUPPORTED_YET,
            b"prepared statements are not supported",
        )
    }

    fn on_execute(
        &mut self,
        _id: u32,
        _params: ParamParser<'_>,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        results.error(
            ErrorKind::ER_NOT_SUPPORTED_YET,
            b"prepared statements are not supported",
        )
    }

    fn on_close(&mut self, _stmt: u32) {}

    fn on_init(&mut self, schema: &str, writer: InitWriter<'_, W>) -> io::Result<()> {
        self.current_db = schema.to_string();
        writer.ok()
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<'_, W>) -> io::Result<()> {
        let q = query.trim();
        if q.is_empty() {
            return results.completed(0, 0);
        }

        // Many clients issue SET statements on connect; accept them as no-ops.
        let q_upper = q.to_ascii_uppercase();
        if q_upper.starts_with("SET ") || q_upper.starts_with("SET@@") {
            return results.completed(0, 0);
        }
        if q_upper.starts_with("USE ") {
            let db = q[3..].trim().trim_matches('`');
            self.current_db = db.to_string();
            return results.completed(0, 0);
        }

        if q_upper == "SELECT DATABASE()" {
            let cols = [Column {
                table: "".to_string(),
                column: "DATABASE()".to_string(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            }];
            let mut rw = results.start(&cols)?;
            rw.write_col(self.current_db.as_str())?;
            return rw.finish();
        }

        if q_upper == "SHOW DATABASES" {
            let cols = [Column {
                table: "".to_string(),
                column: "Database".to_string(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            }];
            let mut rw = results.start(&cols)?;
            rw.write_col("default")?;
            return rw.finish();
        }

        if q_upper.starts_with("SHOW TABLES") {
            let tables = self.handle.block_on(async {
                let coord = self.coordinator.lock().await;
                coord.list_tables()
            });
            let cols = [Column {
                table: "".to_string(),
                column: format!("Tables_in_{}", self.current_db),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            }];
            let mut rw = results.start(&cols)?;
            for t in tables {
                rw.write_col(t.as_str())?;
                rw.end_row()?;
            }
            return rw.finish();
        }

        let batches = match self.handle.block_on(async {
            let mut coord = self.coordinator.lock().await;
            coord.execute_query(q).await
        }) {
            Ok(b) => b,
            Err(e) => {
                return results.error(ErrorKind::ER_UNKNOWN_ERROR, e.to_string().as_bytes());
            }
        };

        write_batches_as_resultset(&batches, results)
    }
}

fn write_batches_as_resultset<W: io::Read + io::Write>(
    batches: &[RecordBatch],
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    let schema = if let Some(b) = batches.first() {
        b.schema()
    } else {
        return results.start(&[])?.finish();
    };

    let cols: Vec<Column> = schema
        .fields()
        .iter()
        .map(|f| Column {
            table: "".to_string(),
            column: f.name().to_string(),
            coltype: arrow_type_to_mysql_type(f.data_type()),
            colflags: ColumnFlags::empty(),
        })
        .collect();

    let mut rw = results.start(&cols)?;
    for batch in batches {
        for row in 0..batch.num_rows() {
            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                write_one_value(
                    &mut rw,
                    array.as_ref(),
                    row,
                    schema.field(col_idx).data_type(),
                )?;
            }
            rw.end_row()?;
        }
    }
    rw.finish()
}

fn arrow_type_to_mysql_type(dt: &DataType) -> ColumnType {
    match dt {
        DataType::Int8 | DataType::UInt8 | DataType::Boolean => ColumnType::MYSQL_TYPE_TINY,
        DataType::Int16 | DataType::UInt16 => ColumnType::MYSQL_TYPE_SHORT,
        DataType::Int32 | DataType::UInt32 => ColumnType::MYSQL_TYPE_LONG,
        DataType::Int64 | DataType::UInt64 => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::Float32 => ColumnType::MYSQL_TYPE_FLOAT,
        DataType::Float64 => ColumnType::MYSQL_TYPE_DOUBLE,
        DataType::Utf8 | DataType::LargeUtf8 => ColumnType::MYSQL_TYPE_VAR_STRING,
        DataType::Date32 => ColumnType::MYSQL_TYPE_DATE,
        DataType::Timestamp(_, _) => ColumnType::MYSQL_TYPE_DATETIME,
        _ => ColumnType::MYSQL_TYPE_STRING,
    }
}

fn write_one_value<W: io::Read + io::Write>(
    rw: &mut msql_srv::RowWriter<'_, W>,
    array: &dyn Array,
    row: usize,
    dt: &DataType,
) -> io::Result<()> {
    if array.is_null(row) {
        return rw.write_col::<Option<u8>>(None);
    }

    match dt {
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad boolean array"))?;
            let v: u8 = if a.value(row) { 1 } else { 0 };
            rw.write_col(v)
        }
        DataType::Int8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::Int8Array>()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad int8 array"))?;
            rw.write_col(a.value(row))
        }
        DataType::Int16 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::Int16Array>()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad int16 array"))?;
            rw.write_col(a.value(row))
        }
        DataType::Int32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad int32 array"))?;
            rw.write_col(a.value(row))
        }
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad int64 array"))?;
            rw.write_col(a.value(row))
        }
        DataType::UInt8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::UInt8Array>()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad uint8 array"))?;
            rw.write_col(a.value(row))
        }
        DataType::UInt16 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::UInt16Array>()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad uint16 array"))?;
            rw.write_col(a.value(row))
        }
        DataType::UInt32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::UInt32Array>()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad uint32 array"))?;
            rw.write_col(a.value(row))
        }
        DataType::UInt64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad uint64 array"))?;
            rw.write_col(a.value(row))
        }
        DataType::Float32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::Float32Array>()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad float32 array"))?;
            rw.write_col(a.value(row))
        }
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad float64 array"))?;
            rw.write_col(a.value(row))
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad utf8 array"))?;
            rw.write_col(a.value(row))
        }
        DataType::LargeUtf8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "bad large utf8 array")
                })?;
            rw.write_col(a.value(row))
        }
        DataType::Date32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::Date32Array>()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad date32 array"))?;
            let days = a.value(row);
            // Date32 is days since UNIX epoch.
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::Duration::days(days as i64))
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "date overflow"))?;
            rw.write_col(date)
        }
        DataType::Timestamp(unit, _) => {
            // Convert to nanoseconds since epoch based on the unit.
            let v = match unit {
                arrow_schema::TimeUnit::Second => {
                    let a = array
                        .as_any()
                        .downcast_ref::<arrow_array::TimestampSecondArray>()
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "bad timestamp(second) array",
                            )
                        })?;
                    a.value(row) * 1_000_000_000
                }
                arrow_schema::TimeUnit::Millisecond => {
                    let a = array
                        .as_any()
                        .downcast_ref::<arrow_array::TimestampMillisecondArray>()
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "bad timestamp(ms) array")
                        })?;
                    a.value(row) * 1_000_000
                }
                arrow_schema::TimeUnit::Microsecond => {
                    let a = array
                        .as_any()
                        .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "bad timestamp(us) array")
                        })?;
                    a.value(row) * 1_000
                }
                arrow_schema::TimeUnit::Nanosecond => {
                    let a = array
                        .as_any()
                        .downcast_ref::<arrow_array::TimestampNanosecondArray>()
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "bad timestamp(ns) array")
                        })?;
                    a.value(row)
                }
            };
            let secs = v / 1_000_000_000;
            let nsec = (v % 1_000_000_000) as u32;
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsec)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "timestamp overflow"))?
                .naive_utc();
            rw.write_col(dt)
        }
        _ => {
            // Fallback: use Arrow's debug formatting.
            let s = format!("{:?}", array);
            rw.write_col(s)
        }
    }
}
