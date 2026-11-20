//! The staging Parquet files: `wikidata_reference_events` and `wikidata_reference_snaks`,
//! as `wcd_schema.staging` defines them (the parity test in `packages/schema` holds this
//! file's two schemas to that definition).

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::DateTime;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::fold::{Event, Sink, SinkError, SnakRow};

/// The events dataset's name.
pub const EVENTS: &str = "wikidata_reference_events";
/// The snaks dataset's name.
pub const SNAKS: &str = "wikidata_reference_snaks";

const ROW_GROUP: usize = 100_000;

/// The schema of [`EVENTS`].
#[must_use]
pub fn events_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("page_id", DataType::Int32, false),
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("revision_id", DataType::Int64, false),
        // `timestamp[s]` in `wcd_schema`. Parquet has no seconds unit: pyarrow stores
        // such a column in milliseconds, and so does this, since a seconds column would
        // be written as a bare int64 that DuckDB and pyarrow read back as a number.
        Field::new(
            "revision_timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("statement_id", DataType::Utf8, false),
        Field::new("statement_property", DataType::Utf8, false),
        Field::new("reference_hash", DataType::Utf8, false),
        Field::new("added", DataType::Boolean, false),
        Field::new("baseline", DataType::Boolean, false),
    ]))
}

/// The schema of [`SNAKS`].
#[must_use]
pub fn snaks_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("page_id", DataType::Int32, false),
        Field::new("reference_hash", DataType::Utf8, false),
        Field::new("snak_order", DataType::Int32, false),
        Field::new("property", DataType::Utf8, false),
        Field::new("snaktype", DataType::Utf8, false),
        Field::new("value_type", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
    ]))
}

fn err(e: impl std::fmt::Display) -> SinkError {
    SinkError(e.to_string())
}

fn int32(n: u64, what: &str) -> Result<i32, SinkError> {
    i32::try_from(n).map_err(|_| SinkError(format!("{what} {n} does not fit the schema's int32")))
}

fn int64(n: u64, what: &str) -> Result<i64, SinkError> {
    i64::try_from(n).map_err(|_| SinkError(format!("{what} {n} does not fit the schema's int64")))
}

struct Out {
    writer: ArrowWriter<File>,
    tmp: PathBuf,
    path: PathBuf,
}

impl Out {
    fn create(dir: &Path, dataset: &str, stem: &str, schema: SchemaRef) -> Result<Self, SinkError> {
        let dir = dir.join(dataset);
        fs::create_dir_all(&dir).map_err(err)?;
        let path = dir.join(format!("{stem}.parquet"));
        let tmp = dir.join(format!("{stem}.parquet.tmp"));
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_max_row_group_size(ROW_GROUP)
            .build();
        let writer = ArrowWriter::try_new(File::create(&tmp).map_err(err)?, schema, Some(props))
            .map_err(err)?;
        Ok(Self { writer, tmp, path })
    }

    fn finish(self) -> Result<PathBuf, SinkError> {
        self.writer.close().map_err(err)?;
        fs::rename(&self.tmp, &self.path).map_err(err)?;
        Ok(self.path)
    }
}

#[derive(Default)]
struct EventColumns {
    page_id: Int32Builder,
    entity_id: StringBuilder,
    revision_id: Int64Builder,
    revision_timestamp: TimestampMillisecondBuilder,
    statement_id: StringBuilder,
    statement_property: StringBuilder,
    reference_hash: StringBuilder,
    added: BooleanBuilder,
    baseline: BooleanBuilder,
    rows: usize,
}

#[derive(Default)]
struct SnakColumns {
    page_id: Int32Builder,
    reference_hash: StringBuilder,
    snak_order: Int32Builder,
    property: StringBuilder,
    snaktype: StringBuilder,
    value_type: StringBuilder,
    value: StringBuilder,
    rows: usize,
}

/// Writes one bundle's rows to `<dir>/<dataset>/<stem>.parquet`, each through a `.tmp`
/// file renamed when it is complete, so a file that exists is whole and a re-run replaces it.
pub struct ParquetSink {
    events: Out,
    snaks: Out,
    event_columns: EventColumns,
    snak_columns: SnakColumns,
}

impl ParquetSink {
    /// Opens both files for a bundle named `stem`.
    pub fn create(dir: &Path, stem: &str) -> Result<Self, SinkError> {
        Ok(Self {
            events: Out::create(dir, EVENTS, stem, events_schema())?,
            snaks: Out::create(dir, SNAKS, stem, snaks_schema())?,
            event_columns: EventColumns::default(),
            snak_columns: SnakColumns::default(),
        })
    }

    fn flush_events(&mut self) -> Result<(), SinkError> {
        let c = &mut self.event_columns;
        if c.rows == 0 {
            return Ok(());
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(c.page_id.finish()),
            Arc::new(c.entity_id.finish()),
            Arc::new(c.revision_id.finish()),
            Arc::new(c.revision_timestamp.finish()),
            Arc::new(c.statement_id.finish()),
            Arc::new(c.statement_property.finish()),
            Arc::new(c.reference_hash.finish()),
            Arc::new(c.added.finish()),
            Arc::new(c.baseline.finish()),
        ];
        c.rows = 0;
        let batch = RecordBatch::try_new(events_schema(), columns).map_err(err)?;
        self.events.writer.write(&batch).map_err(err)
    }

    fn flush_snaks(&mut self) -> Result<(), SinkError> {
        let c = &mut self.snak_columns;
        if c.rows == 0 {
            return Ok(());
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(c.page_id.finish()),
            Arc::new(c.reference_hash.finish()),
            Arc::new(c.snak_order.finish()),
            Arc::new(c.property.finish()),
            Arc::new(c.snaktype.finish()),
            Arc::new(c.value_type.finish()),
            Arc::new(c.value.finish()),
        ];
        c.rows = 0;
        let batch = RecordBatch::try_new(snaks_schema(), columns).map_err(err)?;
        self.snaks.writer.write(&batch).map_err(err)
    }

    /// Completes both files and returns their paths, events first.
    pub fn finish(mut self) -> Result<(PathBuf, PathBuf), SinkError> {
        self.flush_events()?;
        self.flush_snaks()?;
        Ok((self.events.finish()?, self.snaks.finish()?))
    }
}

impl Sink for ParquetSink {
    fn event(&mut self, e: Event) -> Result<(), SinkError> {
        let c = &mut self.event_columns;
        c.page_id.append_value(int32(e.page_id, "page_id")?);
        c.entity_id.append_value(&e.entity_id);
        c.revision_id
            .append_value(int64(e.revision_id, "revision_id")?);
        c.revision_timestamp.append_option(
            DateTime::parse_from_rfc3339(&e.timestamp)
                .ok()
                .map(|t| t.timestamp_millis()),
        );
        c.statement_id.append_value(&e.statement_id);
        c.statement_property.append_value(&e.statement_property);
        c.reference_hash.append_value(&e.reference_hash);
        c.added.append_value(e.added);
        c.baseline.append_value(e.baseline);
        c.rows += 1;
        if c.rows >= ROW_GROUP {
            self.flush_events()?;
        }
        Ok(())
    }

    fn snak(&mut self, s: SnakRow) -> Result<(), SinkError> {
        let c = &mut self.snak_columns;
        c.page_id.append_value(int32(s.page_id, "page_id")?);
        c.reference_hash.append_value(&s.reference_hash);
        c.snak_order
            .append_value(i32::try_from(s.snak_order).map_err(err)?);
        c.property.append_value(&s.property);
        c.snaktype.append_value(s.snaktype);
        c.value_type.append_option(s.value_type.as_deref());
        c.value.append_option(s.value.as_deref());
        c.rows += 1;
        if c.rows >= ROW_GROUP {
            self.flush_snaks()?;
        }
        Ok(())
    }
}
