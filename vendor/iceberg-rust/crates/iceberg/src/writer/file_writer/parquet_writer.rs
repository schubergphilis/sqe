// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The module contains the file writer for parquet file format.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::SchemaRef as ArrowSchemaRef;
use bytes::Bytes;
use futures::future::BoxFuture;
use itertools::Itertools;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_writer::AsyncFileWriter as ArrowAsyncFileWriter;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet::file::metadata::{KeyValue, ParquetMetaData};
use parquet::file::properties::{CdcOptions, WriterProperties};
use parquet::file::statistics::Statistics;

use super::{FileWriter, FileWriterBuilder};
use crate::arrow::{
    ArrowFileReader, DEFAULT_MAP_FIELD_NAME, FieldMatchMode, NanValueCountVisitor,
    get_parquet_stat_max_as_datum, get_parquet_stat_min_as_datum,
};
use crate::compression::CompressionCodec;
use crate::encryption::{EncryptionManager, StandardKeyMetadata};
use crate::error::invalid_data;
use crate::io::{FileIO, FileWrite, OutputFile};
use crate::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, Datum, ListType, Literal, MapType,
    NestedFieldRef, PartitionSpec, PrimitiveType, Schema, SchemaRef, SchemaVisitor, Struct,
    StructType, TableMetadata, TableProperties, Type, VariantType, visit_schema,
};
use crate::transform::create_transform_function;
use crate::writer::{CurrentFileStatus, DataFile};
use crate::{Error, ErrorKind, Result};

const ICEBERG_SCHEMA_KEY: &str = "iceberg.schema";

/// ParquetWriterBuilder is used to builder a [`ParquetWriter`]
#[derive(Clone, Debug)]
pub struct ParquetWriterBuilder {
    props: WriterProperties,
    schema: SchemaRef,
    match_mode: FieldMatchMode,
    encryption_manager: Option<Arc<EncryptionManager>>,
}

impl ParquetWriterBuilder {
    /// Create a new `ParquetWriterBuilder`
    /// To construct the write result, the schema should contain the `PARQUET_FIELD_ID_META_KEY` metadata for each field.
    ///
    /// When writing into an existing Iceberg table, prefer
    /// [`Self::from_table_properties`], which derives `WriterProperties` from
    /// the table's `write.parquet.*` properties.
    pub fn new(props: WriterProperties, schema: SchemaRef) -> Self {
        Self::new_with_match_mode(props, schema, FieldMatchMode::Id)
    }

    /// Create a new `ParquetWriterBuilder` with custom match mode
    pub fn new_with_match_mode(
        props: WriterProperties,
        schema: SchemaRef,
        match_mode: FieldMatchMode,
    ) -> Self {
        let props = Self::add_iceberg_schema_metadata(props, schema.as_ref());
        Self {
            props,
            schema,
            match_mode,
            encryption_manager: None,
        }
    }

    fn add_iceberg_schema_metadata(props: WriterProperties, schema: &Schema) -> WriterProperties {
        let schema_json =
            serde_json::to_string(schema).expect("Iceberg schema serialization should not fail");
        let mut metadata = props.key_value_metadata().cloned().unwrap_or_default();
        metadata.retain(|entry| entry.key != ICEBERG_SCHEMA_KEY);
        metadata.push(KeyValue::new(ICEBERG_SCHEMA_KEY.to_string(), schema_json));

        props
            .into_builder()
            .set_key_value_metadata(Some(metadata))
            .build()
    }

    /// Build a `ParquetWriterBuilder` from Iceberg table properties and a
    /// schema, translating `write.parquet.*` settings into `WriterProperties`
    /// instead of using parquet-rs defaults.
    pub fn from_table_properties(
        table_props: &TableProperties<'_>,
        schema: SchemaRef,
    ) -> Result<Self> {
        let cdc = if table_props.cdc_enabled()? {
            Some(CdcOptions {
                min_chunk_size: table_props.cdc_min_chunk_size()?,
                max_chunk_size: table_props.cdc_max_chunk_size()?,
                norm_level: table_props.cdc_norm_level()?,
            })
        } else {
            None
        };
        let compression = parquet_compression(table_props.parquet_compression_codec()?)?;
        let props = WriterProperties::builder()
            .set_content_defined_chunking(cdc)
            .set_compression(compression)
            .set_max_row_group_bytes(Some(table_props.parquet_row_group_size_bytes()?))
            .set_data_page_size_limit(table_props.parquet_page_size_bytes()?)
            .set_data_page_row_count_limit(table_props.parquet_page_row_limit()?)
            .set_dictionary_page_size_limit(table_props.parquet_dict_size_bytes()?)
            .build();
        Ok(Self::new_with_match_mode(props, schema, FieldMatchMode::Id))
    }

    /// Set the field match mode used to map Arrow fields to Iceberg fields.
    ///
    /// Defaults to [`FieldMatchMode::Id`]. Use [`FieldMatchMode::Name`] when the
    /// incoming Arrow schema does not carry Iceberg field-id metadata.
    pub fn with_match_mode(mut self, match_mode: FieldMatchMode) -> Self {
        self.match_mode = match_mode;
        self
    }

    /// Set the [`EncryptionManager`] used to encrypt written files.
    pub fn with_encryption_manager(mut self, encryption_manager: Arc<EncryptionManager>) -> Self {
        self.encryption_manager = Some(encryption_manager);
        self
    }
}

fn parquet_compression(codec: CompressionCodec) -> Result<Compression> {
    let compression = match codec {
        CompressionCodec::None => Compression::UNCOMPRESSED,
        CompressionCodec::Snappy => Compression::SNAPPY,
        CompressionCodec::Lzo => Compression::LZO,
        CompressionCodec::Lz4 => Compression::LZ4,
        CompressionCodec::Lz4Raw => Compression::LZ4_RAW,
        CompressionCodec::Zstd(level) => {
            let level =
                ZstdLevel::try_new(level as i32).map_err(|e| invalid_level_error("zstd", e))?;
            Compression::ZSTD(level)
        }
        CompressionCodec::Gzip(level) => {
            let level =
                GzipLevel::try_new(level as u32).map_err(|e| invalid_level_error("gzip", e))?;
            Compression::GZIP(level)
        }
        CompressionCodec::Brotli(level) => {
            let level =
                BrotliLevel::try_new(level as u32).map_err(|e| invalid_level_error("brotli", e))?;
            Compression::BROTLI(level)
        }
    };
    Ok(compression)
}

fn invalid_level_error(codec: &str, source: impl Into<anyhow::Error>) -> Error {
    invalid_data!("Invalid {codec} compression level").with_source(source)
}

impl FileWriterBuilder for ParquetWriterBuilder {
    type R = ParquetWriter;

    async fn build(&self, output_file: OutputFile) -> Result<Self::R> {
        let key_metadata = self
            .encryption_manager
            .as_ref()
            .map(|em| em.generate_key_metadata());
        let writer_properties =
            resolve_writer_properties(self.props.clone(), key_metadata.as_ref())?;
        Ok(ParquetWriter {
            schema: self.schema.clone(),
            inner_writer: None,
            writer_properties,
            current_row_num: 0,
            output_file,
            nan_value_count_visitor: NanValueCountVisitor::new_with_match_mode(self.match_mode),
            key_metadata,
            prune_unknown: false,
        })
    }
}

/// The two physical Parquet leaves an unshredded variant column is stored as.
///
/// Must stay in sync with the Arrow struct built for a variant in
/// `crate::arrow::schema`, which is where these names originate.
const VARIANT_METADATA_LEAF: &str = "metadata";
const VARIANT_VALUE_LEAF: &str = "value";

/// A mapping from Parquet column path names to internal field id
struct IndexByParquetPathName {
    name_to_id: HashMap<String, i32>,

    /// Field ids that are variant columns.
    ///
    /// Needed because a variant is the one type whose logical column maps to *two* entries in
    /// `name_to_id`, so callers walking column chunks must not treat each leaf as an independent
    /// column. See the statistics loop in `to_data_file_builder`.
    variant_field_ids: HashSet<i32>,

    field_names: Vec<String>,

    field_id: i32,
}

impl IndexByParquetPathName {
    /// Creates a new, empty `IndexByParquetPathName`
    pub fn new() -> Self {
        Self {
            name_to_id: HashMap::new(),
            variant_field_ids: HashSet::new(),
            field_names: Vec::new(),
            field_id: 0,
        }
    }

    /// Retrieves the internal field ID
    pub fn get(&self, name: &str) -> Option<&i32> {
        self.name_to_id.get(name)
    }

    /// Whether `field_id` is a variant column, i.e. one whose statistics come from two physical
    /// Parquet leaves rather than one.
    pub fn is_variant_field(&self, field_id: i32) -> bool {
        self.variant_field_ids.contains(&field_id)
    }

    fn insert_path(&mut self, full_name: String) -> Result<()> {
        let field_id = self.field_id;
        if let Some(existing_field_id) = self.name_to_id.get(full_name.as_str()) {
            return Err(invalid_data!(
                "Invalid schema: multiple fields for name {full_name}: {field_id} and {existing_field_id}"
            ));
        } else {
            self.name_to_id.insert(full_name, field_id);
        }
        Ok(())
    }

    fn current_path(&self) -> String {
        self.field_names.iter().map(String::as_str).join(".")
    }

    fn insert_current_path(&mut self) -> Result<()> {
        let full_name = self.current_path();
        self.insert_path(full_name)
    }

    /// Indexes a variant column against **both** of its physical Parquet leaves.
    ///
    /// A variant is logically a single column but is written as a struct of two required binary
    /// fields, so the logical path never appears as a Parquet column chunk. Indexing only the
    /// logical path makes every lookup miss and silently drops the column's statistics.
    fn insert_current_variant_path(&mut self) -> Result<()> {
        let base = self.current_path();
        self.variant_field_ids.insert(self.field_id);
        self.insert_path(format!("{base}.{VARIANT_METADATA_LEAF}"))?;
        self.insert_path(format!("{base}.{VARIANT_VALUE_LEAF}"))
    }
}

impl Default for IndexByParquetPathName {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaVisitor for IndexByParquetPathName {
    type T = ();

    fn before_struct_field(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.push(field.name.to_string());
        self.field_id = field.id;
        Ok(())
    }

    fn after_struct_field(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_list_element(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.push(format!("list.{}", field.name));
        self.field_id = field.id;
        Ok(())
    }

    fn after_list_element(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_map_key(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names
            .push(format!("{DEFAULT_MAP_FIELD_NAME}.key"));
        self.field_id = field.id;
        Ok(())
    }

    fn after_map_key(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_map_value(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names
            .push(format!("{DEFAULT_MAP_FIELD_NAME}.value"));
        self.field_id = field.id;
        Ok(())
    }

    fn after_map_value(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn schema(&mut self, _schema: &Schema, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn field(&mut self, _field: &NestedFieldRef, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn r#struct(&mut self, _struct: &StructType, _results: Vec<Self::T>) -> Result<Self::T> {
        Ok(())
    }

    fn list(&mut self, _list: &ListType, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn map(&mut self, _map: &MapType, _key_value: Self::T, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn primitive(&mut self, _p: &PrimitiveType) -> Result<Self::T> {
        self.insert_current_path()
    }

    fn variant(&mut self, _v: &VariantType) -> Result<Self::T> {
        self.insert_current_variant_path()
    }
}

/// `ParquetWriter`` is used to write arrow data into parquet file on storage.
pub struct ParquetWriter {
    schema: SchemaRef,
    output_file: OutputFile,
    inner_writer: Option<AsyncArrowWriter<AsyncFileWriter>>,
    writer_properties: WriterProperties,
    current_row_num: usize,
    nan_value_count_visitor: NanValueCountVisitor,
    key_metadata: Option<StandardKeyMetadata>,
    /// SQE PATCH (v3-unknown): true when the schema holds an `unknown`
    /// field at any depth; every batch is then pruned before it is written.
    prune_unknown: bool,
}

/// SQE PATCH (v3-unknown): the spec never writes a V3 `unknown` field to a
/// data file, and Spark rejects a parquet column with the Null logical type.
/// `prune_unknown_type` drops every Null (`unknown`) member from `data_type`;
/// `None` means the whole value is unknown. A struct left with no members,
/// a list of unknown and a map with an unknown key or value cannot be
/// written without the field, so they are refused.
fn prune_unknown_type(
    name: &str,
    data_type: &arrow_schema::DataType,
) -> Result<Option<arrow_schema::DataType>> {
    use arrow_schema::DataType;
    let refuse = |what: &str| {
        Error::new(
            ErrorKind::FeatureUnsupported,
            format!("Column `{name}` has {what}, which cannot be written to a data file."),
        )
    };
    let prune_field = |f: &arrow_schema::FieldRef| -> Result<Option<arrow_schema::FieldRef>> {
        Ok(prune_unknown_type(name, f.data_type())?.map(|dt| {
            if &dt == f.data_type() {
                f.clone()
            } else {
                Arc::new(f.as_ref().clone().with_data_type(dt))
            }
        }))
    };
    let element = |f: &arrow_schema::FieldRef| match prune_field(f)? {
        Some(f) => Ok(f),
        None => Err(refuse("a list of unknown")),
    };
    Ok(Some(match data_type {
        DataType::Null => return Ok(None),
        DataType::Struct(fields) => {
            let mut kept = Vec::with_capacity(fields.len());
            for f in fields {
                kept.extend(prune_field(f)?);
            }
            if kept.is_empty() && !fields.is_empty() {
                return Err(refuse("a struct whose members are all unknown"));
            }
            DataType::Struct(kept.into())
        }
        DataType::List(f) => DataType::List(element(f)?),
        DataType::LargeList(f) => DataType::LargeList(element(f)?),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(element(f)?, *n),
        DataType::Map(f, sorted) => {
            if let DataType::Struct(kv) = f.data_type()
                && kv.iter().any(|f| f.data_type() == &DataType::Null)
            {
                return Err(refuse("a map with an unknown key or value"));
            }
            DataType::Map(element(f)?, *sorted)
        }
        other => other.clone(),
    }))
}

/// SQE PATCH (v3-unknown): `array` without its `unknown` members, shaped as
/// `prune_unknown_type` shapes its type; `None` when the array is unknown.
fn prune_unknown_array(
    name: &str,
    array: &arrow_array::ArrayRef,
) -> Result<Option<arrow_array::ArrayRef>> {
    use arrow_array::{
        Array, FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray,
    };
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType;
    let Some(target) = prune_unknown_type(name, array.data_type())? else {
        return Ok(None);
    };
    if &target == array.data_type() {
        return Ok(Some(array.clone()));
    }
    let child = |a: &arrow_array::ArrayRef| -> Result<arrow_array::ArrayRef> {
        prune_unknown_array(name, a)?.ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "unknown child kept by prune_unknown_type")
        })
    };
    let rebuild = |err: arrow_schema::ArrowError| {
        Error::new(ErrorKind::Unexpected, "Failed to drop unknown fields.").with_source(err)
    };
    let pruned: arrow_array::ArrayRef = match &target {
        DataType::Struct(fields) => {
            let source = array.as_struct();
            let mut children = Vec::with_capacity(fields.len());
            for column in source.columns() {
                children.extend(prune_unknown_array(name, column)?);
            }
            Arc::new(
                StructArray::try_new(fields.clone(), children, source.nulls().cloned())
                    .map_err(rebuild)?,
            )
        }
        DataType::List(f) => {
            let source = array.as_list::<i32>();
            Arc::new(
                ListArray::try_new(
                    f.clone(),
                    source.offsets().clone(),
                    child(source.values())?,
                    source.nulls().cloned(),
                )
                .map_err(rebuild)?,
            )
        }
        DataType::LargeList(f) => {
            let source = array.as_list::<i64>();
            Arc::new(
                LargeListArray::try_new(
                    f.clone(),
                    source.offsets().clone(),
                    child(source.values())?,
                    source.nulls().cloned(),
                )
                .map_err(rebuild)?,
            )
        }
        DataType::FixedSizeList(f, n) => {
            let source = array.as_fixed_size_list();
            Arc::new(
                FixedSizeListArray::try_new(
                    f.clone(),
                    *n,
                    child(source.values())?,
                    source.nulls().cloned(),
                )
                .map_err(rebuild)?,
            )
        }
        DataType::Map(f, sorted) => {
            let source = array.as_map();
            let entries: arrow_array::ArrayRef = Arc::new(source.entries().clone());
            Arc::new(
                MapArray::try_new(
                    f.clone(),
                    source.offsets().clone(),
                    child(&entries)?.as_struct().clone(),
                    source.nulls().cloned(),
                    *sorted,
                )
                .map_err(rebuild)?,
            )
        }
        _ => {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Column `{name}` changed type while dropping unknown fields."),
            ));
        }
    };
    Ok(Some(pruned))
}

/// SQE PATCH (v3-unknown): `schema` without its `unknown` fields.
fn prune_unknown_schema(schema: &arrow_schema::Schema) -> Result<arrow_schema::Schema> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        if let Some(dt) = prune_unknown_type(f.name(), f.data_type())? {
            fields.push(f.as_ref().clone().with_data_type(dt));
        }
    }
    Ok(arrow_schema::Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
}

/// SQE PATCH (v3-unknown): `batch` without its `unknown` fields.
fn prune_unknown_batch(batch: &arrow_array::RecordBatch) -> Result<arrow_array::RecordBatch> {
    let schema = Arc::new(prune_unknown_schema(batch.schema_ref())?);
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (f, column) in batch.schema_ref().fields().iter().zip(batch.columns()) {
        columns.extend(prune_unknown_array(f.name(), column)?);
    }
    arrow_array::RecordBatch::try_new_with_options(
        schema,
        columns,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|err| {
        Error::new(ErrorKind::Unexpected, "Failed to drop unknown fields.").with_source(err)
    })
}

/// SQE PATCH (v3-unknown): true when `data_type` is or holds a Null
/// (`unknown`) field.
fn contains_null(data_type: &arrow_schema::DataType) -> bool {
    use arrow_schema::DataType;
    match data_type {
        DataType::Null => true,
        DataType::Struct(fields) => fields.iter().any(|f| contains_null(f.data_type())),
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => contains_null(f.data_type()),
        _ => false,
    }
}

/// Used to aggregate min and max value of each column.
struct MinMaxColAggregator {
    lower_bounds: HashMap<i32, Datum>,
    upper_bounds: HashMap<i32, Datum>,
    schema: SchemaRef,
}

impl MinMaxColAggregator {
    /// Creates new and empty `MinMaxColAggregator`
    fn new(schema: SchemaRef) -> Self {
        Self {
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            schema,
        }
    }

    fn update_state_min(&mut self, field_id: i32, datum: Datum) {
        self.lower_bounds
            .entry(field_id)
            .and_modify(|e| {
                if *e > datum {
                    *e = datum.clone()
                }
            })
            .or_insert(datum);
    }

    fn update_state_max(&mut self, field_id: i32, datum: Datum) {
        self.upper_bounds
            .entry(field_id)
            .and_modify(|e| {
                if *e < datum {
                    *e = datum.clone()
                }
            })
            .or_insert(datum);
    }

    /// Update statistics
    fn update(&mut self, field_id: i32, value: &Statistics) -> Result<()> {
        let Some(ty) = self
            .schema
            .field_by_id(field_id)
            .map(|f| f.field_type.as_ref())
        else {
            // Following java implementation: https://github.com/apache/iceberg/blob/29a2c456353a6120b8c882ed2ab544975b168d7b/parquet/src/main/java/org/apache/iceberg/parquet/ParquetUtil.java#L163
            // Ignore the field if it is not in schema.
            return Ok(());
        };
        let Type::Primitive(ty) = ty.clone() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Composed type {ty} is not supported for min max aggregation."),
            ));
        };

        if value.min_is_exact() {
            let Some(min_datum) = get_parquet_stat_min_as_datum(&ty, value)? else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Statistics {value} is not match with field type {ty}."),
                ));
            };

            self.update_state_min(field_id, min_datum);
        }

        if value.max_is_exact() {
            let Some(max_datum) = get_parquet_stat_max_as_datum(&ty, value)? else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Statistics {value} is not match with field type {ty}."),
                ));
            };

            self.update_state_max(field_id, max_datum);
        }

        Ok(())
    }

    /// Returns lower and upper bounds
    fn produce(self) -> (HashMap<i32, Datum>, HashMap<i32, Datum>) {
        (self.lower_bounds, self.upper_bounds)
    }
}

impl ParquetWriter {
    /// Converts parquet files to data files
    #[allow(dead_code)]
    pub(crate) async fn parquet_files_to_data_files(
        file_io: &FileIO,
        file_paths: Vec<String>,
        table_metadata: &TableMetadata,
    ) -> Result<Vec<DataFile>> {
        // TODO: support adding to partitioned table
        let mut data_files: Vec<DataFile> = Vec::new();

        for file_path in file_paths {
            let input_file = file_io.new_input(&file_path)?;
            let file_metadata = input_file.metadata().await?;
            let file_size_in_bytes = file_metadata.size as usize;
            let reader = input_file.reader().await?;

            let mut parquet_reader = ArrowFileReader::new(file_metadata, reader);
            let parquet_metadata = parquet_reader
                .get_metadata(None)
                .await
                .map_err(|err| invalid_data!("Error reading Parquet metadata: {err}"))?;
            let mut builder = ParquetWriter::parquet_to_data_file_builder(
                table_metadata.current_schema().clone(),
                parquet_metadata,
                file_size_in_bytes,
                file_path,
                // TODO: Implement nan_value_counts here
                HashMap::new(),
                None,
            )?;
            builder.partition_spec_id(table_metadata.default_partition_spec_id());
            let data_file = builder.build().unwrap();
            data_files.push(data_file);
        }

        Ok(data_files)
    }

    /// `ParquetMetadata` to data file builder
    pub(crate) fn parquet_to_data_file_builder(
        schema: SchemaRef,
        metadata: Arc<ParquetMetaData>,
        written_size: usize,
        file_path: String,
        nan_value_counts: HashMap<i32, u64>,
        key_metadata: Option<StandardKeyMetadata>,
    ) -> Result<DataFileBuilder> {
        let index_by_parquet_path = {
            let mut visitor = IndexByParquetPathName::new();
            visit_schema(&schema, &mut visitor)?;
            visitor
        };

        let (column_sizes, value_counts, null_value_counts, (lower_bounds, upper_bounds)) = {
            let mut per_col_size: HashMap<i32, u64> = HashMap::new();
            let mut per_col_val_num: HashMap<i32, u64> = HashMap::new();
            let mut per_col_null_val_num: HashMap<i32, u64> = HashMap::new();
            let mut min_max_agg = MinMaxColAggregator::new(schema);

            for row_group in metadata.row_groups() {
                for column_chunk_metadata in row_group.columns() {
                    let parquet_path = column_chunk_metadata.column_descr().path().string();

                    let Some(&field_id) = index_by_parquet_path.get(&parquet_path) else {
                        continue;
                    };

                    // Size always accumulates. For a variant both physical leaves are required
                    // to read the column, so its size is the sum of the two.
                    *per_col_size.entry(field_id).or_insert(0) +=
                        column_chunk_metadata.compressed_size() as u64;

                    // A variant is stored as two leaves that describe the *same* logical
                    // values, so each statistic is attributed to exactly one leaf to avoid
                    // double counting, and bounds are skipped outright (a variant is not an
                    // orderable primitive, so the spec defines no bounds for it and the
                    // min/max aggregator rejects non-primitive types):
                    //
                    // - `value_counts` (row count) comes from the `value` leaf. Both leaves
                    //   carry the same `num_values`, so either works; `value` is chosen once.
                    // - `null_value_counts` comes from the **`metadata`** leaf. `metadata` is
                    //   required and present iff the variant is logically non-null, so its null
                    //   count is the true logical null count. The `value` leaf is *nullable*
                    //   (absent for a shredded variant whose typed value lives elsewhere), so
                    //   counting nulls there would over-report nulls for logically non-null
                    //   variants and could make a planner wrongly prune an `IS NOT NULL`
                    //   predicate.
                    if index_by_parquet_path.is_variant_field(field_id) {
                        match parquet_path.rsplit('.').next() {
                            Some(VARIANT_VALUE_LEAF) => {
                                *per_col_val_num.entry(field_id).or_insert(0) +=
                                    column_chunk_metadata.num_values() as u64;
                            }
                            Some(VARIANT_METADATA_LEAF) => {
                                if let Some(null_count) = column_chunk_metadata
                                    .statistics()
                                    .and_then(|statistics| statistics.null_count_opt())
                                {
                                    *per_col_null_val_num.entry(field_id).or_insert(0) +=
                                        null_count;
                                }
                            }
                            _ => {}
                        }
                        continue;
                    }

                    *per_col_val_num.entry(field_id).or_insert(0) +=
                        column_chunk_metadata.num_values() as u64;

                    if let Some(statistics) = column_chunk_metadata.statistics() {
                        if let Some(null_count) = statistics.null_count_opt() {
                            *per_col_null_val_num.entry(field_id).or_insert(0) += null_count;
                        }

                        min_max_agg.update(field_id, statistics)?;
                    }
                }
            }
            (
                per_col_size,
                per_col_val_num,
                per_col_null_val_num,
                min_max_agg.produce(),
            )
        };

        let key_metadata = match key_metadata {
            Some(m) => Some(m.encode()?.into_vec()),
            None => None,
        };

        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::Data)
            .file_path(file_path)
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .record_count(metadata.file_metadata().num_rows() as u64)
            .file_size_in_bytes(written_size as u64)
            .column_sizes(column_sizes)
            .value_counts(value_counts)
            .null_value_counts(null_value_counts)
            .nan_value_counts(nan_value_counts)
            // # NOTE:
            // - We can ignore implementing distinct_counts due to this: https://lists.apache.org/thread/j52tsojv0x4bopxyzsp7m7bqt23n5fnd
            .lower_bounds(lower_bounds)
            .upper_bounds(upper_bounds)
            .key_metadata(key_metadata)
            .split_offsets(Some(
                metadata
                    .row_groups()
                    .iter()
                    .filter_map(|group| group.file_offset())
                    .collect(),
            ));

        Ok(builder)
    }

    #[allow(dead_code)]
    fn partition_value_from_bounds(
        table_spec: Arc<PartitionSpec>,
        lower_bounds: &HashMap<i32, Datum>,
        upper_bounds: &HashMap<i32, Datum>,
    ) -> Result<Struct> {
        let mut partition_literals: Vec<Option<Literal>> = Vec::new();

        for field in table_spec.fields() {
            if let (Some(lower), Some(upper)) = (
                lower_bounds.get(&field.source_id),
                upper_bounds.get(&field.source_id),
            ) {
                if !field.transform.preserves_order() {
                    return Err(invalid_data!(
                        "cannot infer partition value for non linear partition field (needs to preserve order): {} with transform {}",
                        field.name,
                        field.transform
                    ));
                }

                if lower != upper {
                    return Err(invalid_data!(
                        "multiple partition values for field {}: lower: {:?}, upper: {:?}",
                        field.name,
                        lower,
                        upper
                    ));
                }

                let transform_fn = create_transform_function(&field.transform)?;
                let transform_literal =
                    Literal::from(transform_fn.transform_literal_result(lower)?);

                partition_literals.push(Some(transform_literal));
            } else {
                partition_literals.push(None);
            }
        }

        let partition_struct = Struct::from_iter(partition_literals);

        Ok(partition_struct)
    }
}

fn resolve_writer_properties(
    writer_properties: WriterProperties,
    key_metadata: Option<&StandardKeyMetadata>,
) -> Result<WriterProperties> {
    let Some(key_metadata) = key_metadata else {
        return Ok(writer_properties);
    };

    if writer_properties.file_encryption_properties().is_some() {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "Parquet writer properties already have file encryption properties set",
        ));
    }

    let mut builder =
        FileEncryptionProperties::builder(key_metadata.encryption_key().as_bytes().to_vec());
    if let Some(aad) = key_metadata.aad_prefix() {
        builder = builder.with_aad_prefix(aad.to_vec());
    }
    let file_encryption_properties = builder.build().map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            "Failed to build parquet file encryption properties",
        )
        .with_source(e)
    })?;

    Ok(writer_properties
        .into_builder()
        .with_file_encryption_properties(file_encryption_properties)
        .build())
}

impl FileWriter for ParquetWriter {
    async fn write(&mut self, batch: &arrow_array::RecordBatch) -> Result<()> {
        // Skip empty batch
        if batch.num_rows() == 0 {
            return Ok(());
        }

        self.current_row_num += batch.num_rows();

        let batch_c = batch.clone();
        self.nan_value_count_visitor
            .compute(self.schema.clone(), batch_c)?;

        // Lazy initialize the writer
        let writer = if let Some(writer) = &mut self.inner_writer {
            writer
        } else {
            let arrow_schema: ArrowSchemaRef = {
                let schema: arrow_schema::Schema = self.schema.as_ref().try_into()?;
                let mut metadata = schema.metadata().clone();
                metadata.insert(
                    ICEBERG_SCHEMA_KEY.to_string(),
                    serde_json::to_string(self.schema.as_ref())
                        .expect("Iceberg schema serialization should not fail"),
                );
                // SQE PATCH (v3-unknown): `unknown` fields, top-level or
                // nested, are left out of the file; readers fill them with
                // NULLs.
                if schema.fields().iter().any(|f| contains_null(f.data_type())) {
                    self.prune_unknown = true;
                    Arc::new(prune_unknown_schema(&schema)?.with_metadata(metadata))
                } else {
                    Arc::new(schema.with_metadata(metadata))
                }
            };
            let inner_writer = self.output_file.writer().await?;
            let async_writer = AsyncFileWriter::new(inner_writer);
            let writer = AsyncArrowWriter::try_new(
                async_writer,
                arrow_schema.clone(),
                Some(self.writer_properties.clone()),
            )
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "Failed to build parquet writer.")
                    .with_source(err)
            })?;
            self.inner_writer = Some(writer);
            self.inner_writer.as_mut().unwrap()
        };

        let pruned;
        let batch = if self.prune_unknown {
            pruned = prune_unknown_batch(batch)?;
            &pruned
        } else {
            batch
        };
        writer.write(batch).await.map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                "Failed to write using parquet writer.",
            )
            .with_source(err)
        })?;

        Ok(())
    }

    async fn close(mut self) -> Result<Vec<DataFileBuilder>> {
        let mut writer = match self.inner_writer.take() {
            Some(writer) => writer,
            None => return Ok(vec![]),
        };

        let metadata = writer.finish().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Failed to finish parquet writer.").with_source(err)
        })?;

        let written_size = writer.bytes_written();

        if self.current_row_num == 0 {
            self.output_file.delete().await.map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to delete empty parquet file.",
                )
                .with_source(err)
            })?;
            Ok(vec![])
        } else {
            let parquet_metadata = Arc::new(metadata);

            Ok(vec![Self::parquet_to_data_file_builder(
                self.schema,
                parquet_metadata,
                written_size,
                self.output_file.location().to_string(),
                self.nan_value_count_visitor.nan_value_counts,
                self.key_metadata,
            )?])
        }
    }
}

impl CurrentFileStatus for ParquetWriter {
    fn current_file_path(&self) -> String {
        self.output_file.location().to_string()
    }

    fn current_row_num(&self) -> usize {
        self.current_row_num
    }

    fn current_written_size(&self) -> usize {
        if let Some(inner) = self.inner_writer.as_ref() {
            // inner/AsyncArrowWriter contains sync and async writers
            // written size = bytes flushed to inner's async writer + bytes buffered in the inner's sync writer
            inner.bytes_written() + inner.in_progress_size()
        } else {
            // inner writer is not initialized yet
            0
        }
    }
}

/// AsyncFileWriter is a wrapper of FileWrite to make it compatible with tokio::io::AsyncWrite.
///
/// # NOTES
///
/// We keep this wrapper been used inside only.
struct AsyncFileWriter(Box<dyn FileWrite>);

impl AsyncFileWriter {
    /// Create a new `AsyncFileWriter` with the given writer.
    pub fn new(writer: Box<dyn FileWrite>) -> Self {
        Self(writer)
    }
}

impl ArrowAsyncFileWriter for AsyncFileWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async {
            self.0
                .write(bs)
                .await
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async {
            // TODO(encryption): retain the stored file size in data-file key metadata.
            self.0
                .close()
                .await
                .map(|_| ())
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use anyhow::Result;
    use arrow_array::builder::{Float32Builder, Int32Builder, MapBuilder};
    use arrow_array::types::{Float32Type, Int64Type};
    use arrow_array::{
        Array, ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int32Array,
        Int64Array, ListArray, MapArray, RecordBatch, StructArray,
    };
    use arrow_schema::{DataType, Field, Fields, SchemaRef as ArrowSchemaRef};
    use arrow_select::concat::concat_batches;
    use futures::TryStreamExt;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
    use parquet::file::statistics::ValueStatistics;
    use parquet::schema::types::ColumnPath;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::Runtime;
    use crate::arrow::{ArrowReaderBuilder, schema_to_arrow_schema};
    use crate::io::FileIO;
    use crate::scan::{FileScanTask, FileScanTaskStream};
    use crate::spec::decimal_utils::{decimal_mantissa, decimal_new, decimal_scale};
    use crate::spec::{PrimitiveLiteral, Struct, *};
    use crate::test_utils::make_encryption_manager;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator, FileNameGenerator, LocationGenerator,
    };
    use crate::writer::tests::check_parquet_data_file;

    fn schema_for_all_type() -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(0, "boolean", Type::Primitive(PrimitiveType::Boolean)).into(),
                NestedField::optional(1, "int", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "long", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(3, "float", Type::Primitive(PrimitiveType::Float)).into(),
                NestedField::optional(4, "double", Type::Primitive(PrimitiveType::Double)).into(),
                NestedField::optional(5, "string", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(6, "binary", Type::Primitive(PrimitiveType::Binary)).into(),
                NestedField::optional(7, "date", Type::Primitive(PrimitiveType::Date)).into(),
                NestedField::optional(8, "time", Type::Primitive(PrimitiveType::Time)).into(),
                NestedField::optional(9, "timestamp", Type::Primitive(PrimitiveType::Timestamp))
                    .into(),
                NestedField::optional(
                    10,
                    "timestamptz",
                    Type::Primitive(PrimitiveType::Timestamptz),
                )
                .into(),
                NestedField::optional(
                    11,
                    "timestamp_ns",
                    Type::Primitive(PrimitiveType::TimestampNs),
                )
                .into(),
                NestedField::optional(
                    12,
                    "timestamptz_ns",
                    Type::Primitive(PrimitiveType::TimestamptzNs),
                )
                .into(),
                NestedField::optional(
                    13,
                    "decimal",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 10,
                        scale: 5,
                    }),
                )
                .into(),
                NestedField::optional(14, "uuid", Type::Primitive(PrimitiveType::Uuid)).into(),
                NestedField::optional(15, "fixed", Type::Primitive(PrimitiveType::Fixed(10)))
                    .into(),
                // Parquet Statistics will use different representation for Decimal with precision 38 and scale 5,
                // so we need to add a new field for it.
                NestedField::optional(
                    16,
                    "decimal_38",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 38,
                        scale: 5,
                    }),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    fn nested_schema_for_test() -> Schema {
        // Int, Struct(Int,Int), String, List(Int), Struct(Struct(Int)), Map(String, List(Int))
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(0, "col0", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(
                    1,
                    "col1",
                    Type::Struct(StructType::new(vec![
                        NestedField::required(5, "col_1_5", Type::Primitive(PrimitiveType::Long))
                            .into(),
                        NestedField::required(6, "col_1_6", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    ])),
                )
                .into(),
                NestedField::required(2, "col2", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(
                    3,
                    "col3",
                    Type::List(ListType::new(
                        NestedField::required(7, "element", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    )),
                )
                .into(),
                NestedField::required(
                    4,
                    "col4",
                    Type::Struct(StructType::new(vec![
                        NestedField::required(
                            8,
                            "col_4_8",
                            Type::Struct(StructType::new(vec![
                                NestedField::required(
                                    9,
                                    "col_4_8_9",
                                    Type::Primitive(PrimitiveType::Long),
                                )
                                .into(),
                            ])),
                        )
                        .into(),
                    ])),
                )
                .into(),
                NestedField::required(
                    10,
                    "col5",
                    Type::Map(MapType::new(
                        NestedField::required(11, "key", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(
                            12,
                            "value",
                            Type::List(ListType::new(
                                NestedField::required(
                                    13,
                                    "item",
                                    Type::Primitive(PrimitiveType::Long),
                                )
                                .into(),
                            )),
                        )
                        .into(),
                    )),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_index_by_parquet_path() {
        let expect = HashMap::from([
            ("col0".to_string(), 0),
            ("col1.col_1_5".to_string(), 5),
            ("col1.col_1_6".to_string(), 6),
            ("col2".to_string(), 2),
            ("col3.list.element".to_string(), 7),
            ("col4.col_4_8.col_4_8_9".to_string(), 9),
            ("col5.key_value.key".to_string(), 11),
            ("col5.key_value.value.list.item".to_string(), 13),
        ]);
        let mut visitor = IndexByParquetPathName::new();
        visit_schema(&nested_schema_for_test(), &mut visitor).unwrap();
        assert_eq!(visitor.name_to_id, expect);
    }

    /// A variant column must survive a real write byte-for-byte, and the written file must
    /// identify the column as a variant.
    ///
    /// This closes the last untested leg of the variant round-trip. Everything else that
    /// exercises variants either reads files written by *another* engine or asserts structure and
    /// row counts rather than payload bytes — so nothing verified that a variant written through
    /// this writer comes back unchanged, nor that the output is self-describing as a variant to a
    /// reader that does not have the Iceberg schema to hand.
    ///
    /// Three properties are pinned:
    ///
    /// 1. **Byte fidelity** of both variant leaves, including across a null row. The payloads are
    ///    opaque to the writer, so a corruption here would be silent.
    /// 2. **`LogicalType::Variant` and the field id on the variant group.** These do hold today,
    ///    but nothing asserted them; losing either would make our files unidentifiable as
    ///    variants without out-of-band schema.
    /// 3. **The two sub-leaves carry no field id.** That absence is not incidental — it is the
    ///    exact property that made variant statistics get dropped (the path index has to map both
    ///    physical leaves onto the group's field id, because the leaves cannot be resolved by id).
    ///    Pinned deliberately so that if arrow/parquet ever starts emitting sub-field ids, this
    ///    test fails and points at the statistics logic that assumes they are absent.
    #[tokio::test]
    async fn test_parquet_writer_variant_round_trip() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "v", Type::Variant(VariantType)).into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let variant_fields = match arrow_schema.field(1).data_type() {
            DataType::Struct(fields) => fields.clone(),
            other => panic!("variant must lower to a struct, got {other:?}"),
        };

        // Deliberately awkward payloads: varying lengths, a zero byte, a high byte, and an empty
        // value. Fixed-width or ASCII-only payloads would hide off-by-one and truncation bugs.
        let metadata_payloads: Vec<&[u8]> = vec![
            &[0x01, 0x00, 0x00],
            &[0x01, 0x01, 0x00, 0x02, 0x61, 0x62],
            &[0x01, 0x00, 0x00],
            &[0x01, 0x00, 0x00],
        ];
        let value_payloads: Vec<&[u8]> =
            vec![&[0x0c, 0x00], &[0x02, 0x01, 0x00, 0x00, 0xff], &[], &[
                0x0c, 0x7f,
            ]];
        let metadata_arr = Arc::new(arrow_array::BinaryArray::from_vec(
            metadata_payloads.clone(),
        )) as ArrayRef;
        let value_arr =
            Arc::new(arrow_array::BinaryArray::from_vec(value_payloads.clone())) as ArrayRef;

        // Row 2 is a null variant. The child arrays still hold bytes underneath, so this also
        // checks that a null entry does not disturb its neighbours' payloads.
        let validity = arrow_buffer::NullBuffer::from(vec![true, true, false, true]);
        let variant_arr = Arc::new(StructArray::new(
            variant_fields,
            vec![metadata_arr, value_arr],
            Some(validity),
        )) as ArrayRef;
        let id_arr = Arc::new(Int64Array::from_iter_values(0..4)) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![id_arr, variant_arr])?;

        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;
        let file_path = output_file.location().to_string();
        let mut pw = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone())
            .build(output_file)
            .await?;
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);

        let bytes = file_io.new_input(&file_path)?.read().await?;

        // ---- 1. byte fidelity, read back through Arrow ----
        {
            use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
                .unwrap()
                .build()
                .unwrap();
            let batches = reader.map(|b| b.unwrap()).collect::<Vec<_>>();
            let read = concat_batches(&batches[0].schema(), &batches).unwrap();
            assert_eq!(read.num_rows(), 4);

            let v = read
                .column_by_name("v")
                .expect("variant column must be present")
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("variant must read back as a struct");
            assert!(v.is_null(2), "row 2 must read back as a null variant");
            for (row, expected_null) in [false, false, true, false].into_iter().enumerate() {
                assert_eq!(v.is_null(row), expected_null, "null mask at row {row}");
            }

            let md = v
                .column_by_name(VARIANT_METADATA_LEAF)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .unwrap();
            let vv = v
                .column_by_name(VARIANT_VALUE_LEAF)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .unwrap();

            // Compare the non-null rows byte-for-byte. The null row's payload is not meaningful.
            for row in [0usize, 1, 3] {
                assert_eq!(
                    md.value(row),
                    metadata_payloads[row],
                    "variant metadata bytes differ at row {row}"
                );
                assert_eq!(
                    vv.value(row),
                    value_payloads[row],
                    "variant value bytes differ at row {row}"
                );
            }
        }

        // ---- 2 & 3. the written file identifies the column as a variant ----
        {
            use parquet::basic::LogicalType;
            use parquet::file::reader::{FileReader, SerializedFileReader};
            let reader = SerializedFileReader::new(bytes).unwrap();
            let schema_descr = reader.metadata().file_metadata().schema_descr();

            let variant_group = schema_descr
                .root_schema()
                .get_fields()
                .iter()
                .find(|f| f.name() == "v")
                .expect("variant group must be present in the parquet schema")
                .clone();
            let info = variant_group.get_basic_info();

            assert!(
                matches!(info.logical_type_ref(), Some(LogicalType::Variant { .. })),
                "variant group must carry LogicalType::Variant, got {:?}",
                info.logical_type_ref()
            );
            assert!(info.has_id(), "variant group must carry a parquet field id");
            assert_eq!(
                info.id(),
                2,
                "variant group field id must be the iceberg field id"
            );

            // The two physical leaves deliberately have no field id -- see the doc comment.
            for leaf in schema_descr.columns() {
                let path = leaf.path().string();
                if path.starts_with("v.") {
                    assert!(
                        !leaf.self_type().get_basic_info().has_id(),
                        "{path} unexpectedly has a field id; the statistics path index assumes \
                         variant leaves cannot be resolved by id and maps them by path instead"
                    );
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_index_by_parquet_path_variant() {
        // A variant is logically one column but is *stored* as two physical Parquet leaves,
        // `metadata` and `value`, so the logical path `v` never appears as a column chunk.
        // Both leaves must therefore be indexed against the variant's field id, or every
        // per-column statistic for a variant column is silently dropped on write.
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "v", Type::Variant(VariantType)).into(),
            ])
            .build()
            .unwrap();
        let mut visitor = IndexByParquetPathName::new();
        visit_schema(&schema, &mut visitor).unwrap();
        assert_eq!(
            visitor.name_to_id,
            HashMap::from([
                ("id".to_string(), 1),
                ("v.metadata".to_string(), 2),
                ("v.value".to_string(), 2),
            ]),
            "both variant leaves must map to the variant's field id"
        );
        assert!(
            visitor.is_variant_field(2),
            "field 2 must be recognised as a variant so bounds are skipped and rows counted once"
        );
        assert!(!visitor.is_variant_field(1));
    }

    #[test]
    fn test_index_by_parquet_path_nested_variant() {
        // A variant nested in a struct must pick up the full dotted prefix on both leaves.
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(
                    1,
                    "outer",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(2, "v", Type::Variant(VariantType)).into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let mut visitor = IndexByParquetPathName::new();
        visit_schema(&schema, &mut visitor).unwrap();
        assert_eq!(
            visitor.name_to_id,
            HashMap::from([
                ("outer.v.metadata".to_string(), 2),
                ("outer.v.value".to_string(), 2),
            ])
        );
    }

    /// A variant column must carry per-column statistics through a real write.
    ///
    /// Regression test: `IndexByParquetPathName` used to register the *logical* path `v`, but
    /// the Parquet file's column chunks are `v.metadata` and `v.value`. Every lookup missed, so
    /// `column_sizes`, `value_counts` and `null_value_counts` were silently dropped for the
    /// variant column on every file written — including every compaction output, which meant
    /// compaction *erased* statistics its inputs had.
    #[tokio::test]
    async fn test_parquet_writer_variant_statistics() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "v", Type::Variant(VariantType)).into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());

        // Minimal unshredded variant payloads: `metadata` is a v1 header with an empty
        // dictionary, `value` a primitive. Contents are irrelevant to statistics — the writer
        // never decodes them — but they must be the two-binary-leaf physical layout.
        let variant_fields = match arrow_schema.field(1).data_type() {
            DataType::Struct(fields) => fields.clone(),
            other => panic!("variant must lower to a struct, got {other:?}"),
        };
        let metadata_arr = Arc::new(arrow_array::BinaryArray::from_vec(vec![
            &[0x01, 0x00, 0x00][..],
            &[0x01, 0x00, 0x00][..],
            &[0x01, 0x00, 0x00][..],
        ])) as ArrayRef;
        let value_arr = Arc::new(arrow_array::BinaryArray::from_vec(vec![
            &[0x0c, 0x01][..],
            &[0x0c, 0x02][..],
            &[0x0c, 0x03][..],
        ])) as ArrayRef;
        let variant_arr = Arc::new(StructArray::new(
            variant_fields,
            vec![metadata_arr, value_arr],
            None,
        )) as ArrayRef;
        let id_arr = Arc::new(Int64Array::from_iter_values(0..3)) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![id_arr, variant_arr])?;

        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;
        let mut pw = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone())
            .build(output_file)
            .await?;
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            .content(DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        assert_eq!(data_file.record_count(), 3);

        // The whole point: field 2 must be present in the per-column stats.
        assert!(
            data_file.column_sizes().contains_key(&2),
            "variant column size was dropped; column_sizes = {:?}",
            data_file.column_sizes()
        );
        assert_eq!(
            data_file.value_counts().get(&2),
            Some(&3),
            "variant value count must be the row count, counted once across the two physical \
             leaves rather than doubled; value_counts = {:?}",
            data_file.value_counts()
        );

        // `column_sizes` must span *both* leaves: they are both required to read the column, so
        // reporting only one under-reports the column's real on-disk footprint.
        let id_size = *data_file.column_sizes().get(&1).unwrap();
        let variant_size = *data_file.column_sizes().get(&2).unwrap();
        assert!(
            variant_size > id_size,
            "variant size ({variant_size}) should cover both leaves and so exceed the single \
             long column ({id_size})"
        );

        // A variant is not an orderable primitive, so the spec defines no bounds for it. These
        // must stay absent rather than being fabricated from the raw binary payloads.
        assert!(
            !data_file.lower_bounds().contains_key(&2),
            "variant must not get lower bounds; got {:?}",
            data_file.lower_bounds()
        );
        assert!(
            !data_file.upper_bounds().contains_key(&2),
            "variant must not get upper bounds; got {:?}",
            data_file.upper_bounds()
        );

        Ok(())
    }

    /// Regression test for the shredded-variant null-count edge case (PR #206 review).
    ///
    /// A variant's Arrow storage has a **required** `metadata` leaf and a **nullable**
    /// `value` leaf. For a shredded variant the typed value lives elsewhere, so the untyped
    /// `value` child is null even though the variant is logically non-null. Deriving
    /// `null_value_counts` from the `value` leaf would then count that row as null and let a
    /// planner wrongly prune an `IS NOT NULL` predicate. The null count must come from the
    /// required `metadata` leaf instead.
    #[tokio::test]
    async fn test_parquet_writer_variant_null_count_from_metadata_leaf() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(2, "v", Type::Variant(VariantType)).into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let variant_fields = match arrow_schema.field(0).data_type() {
            DataType::Struct(fields) => fields.clone(),
            other => panic!("variant must lower to a struct, got {other:?}"),
        };

        // Two present variants and one NULL variant. In the unshredded layout both
        // leaves are required, so a leaf is null exactly where the variant is null
        // (masked by the struct validity); the stats are read off the `metadata` leaf.
        let metadata_arr = Arc::new(arrow_array::BinaryArray::from_opt_vec(vec![
            Some(&[0x01, 0x00, 0x00][..]),
            Some(&[0x01, 0x00, 0x00][..]),
            None,
        ])) as ArrayRef;
        let value_arr = Arc::new(arrow_array::BinaryArray::from_opt_vec(vec![
            Some(&[0x0c, 0x01][..]),
            Some(&[0x0c, 0x02][..]),
            None,
        ])) as ArrayRef;
        let variant_arr = Arc::new(StructArray::new(
            variant_fields,
            vec![metadata_arr, value_arr],
            Some(arrow_buffer::NullBuffer::from(vec![true, true, false])),
        )) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![variant_arr])?;

        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;
        let mut pw = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone())
            .build(output_file)
            .await?;
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            .content(DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        assert_eq!(data_file.record_count(), 3);
        assert_eq!(
            data_file.value_counts().get(&2),
            Some(&3),
            "value_counts must be the row count; got {:?}",
            data_file.value_counts()
        );
        assert_eq!(
            data_file.null_value_counts().get(&2),
            Some(&1),
            "the NULL variant row must be the only logical null, counted from the \
             `metadata` leaf; got {:?}",
            data_file.null_value_counts()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        let schema = {
            let fields =
                vec![
                    Field::new("col", DataType::Int64, true).with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "0".to_string(),
                    )])),
                ];
            Arc::new(arrow_schema::Schema::new(fields))
        };
        let col = Arc::new(Int64Array::from_iter_values(0..1024)) as ArrayRef;
        let null_col = Arc::new(Int64Array::new_null(1024)) as ArrayRef;
        let to_write = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();
        let to_write_null = RecordBatch::try_new(schema.clone(), vec![null_col]).unwrap();

        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;
        let iceberg_schema: SchemaRef = Arc::new(to_write.schema().as_ref().try_into().unwrap());

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(128))
                .build(),
            iceberg_schema.clone(),
        )
        .build(output_file)
        .await?;
        pw.write(&to_write).await?;
        pw.write(&to_write_null).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 2048);
        assert_eq!(*data_file.value_counts(), HashMap::from([(0, 2048)]));
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([(0, Datum::long(0))])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([(0, Datum::long(1023))])
        );
        assert_eq!(*data_file.null_value_counts(), HashMap::from([(0, 1024)]));

        // check the written file
        let expect_batch = concat_batches(&schema, vec![&to_write, &to_write_null]).unwrap();
        check_parquet_data_file(&file_io, &data_file, &expect_batch).await;

        let input = file_io
            .new_input(data_file.file_path())
            .unwrap()
            .read()
            .await
            .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(input).unwrap();
        let footer_metadata = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap();
        let footer_schema_json = footer_metadata
            .iter()
            .find(|entry| entry.key == ICEBERG_SCHEMA_KEY)
            .and_then(|entry| entry.value.as_deref())
            .expect("Parquet footer should contain iceberg.schema");
        assert_eq!(
            serde_json::from_str::<Schema>(footer_schema_json).unwrap(),
            *iceberg_schema
        );

        let arrow_schema = reader.schema();
        let arrow_schema_json = arrow_schema
            .metadata()
            .get(ICEBERG_SCHEMA_KEY)
            .expect("Arrow schema metadata should contain iceberg.schema");
        assert_eq!(
            serde_json::from_str::<Schema>(arrow_schema_json).unwrap(),
            *iceberg_schema
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_writer_encrypted_roundtrip() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));
        let iceberg_schema: SchemaRef = Arc::new(arrow_schema.as_ref().try_into().unwrap());
        let batch = RecordBatch::try_new(arrow_schema, vec![Arc::new(Int32Array::from(vec![
            10, 20, 30,
        ])) as ArrayRef])
        .unwrap();

        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;
        let raw_properties = HashMap::from([(
            TableProperties::PROPERTY_ENCRYPTION_KEY_ID.to_string(),
            "test-key".to_string(),
        )]);
        let table_properties = TableProperties::new(&raw_properties);
        let mut parquet_writer =
            ParquetWriterBuilder::from_table_properties(&table_properties, iceberg_schema.clone())?
                .with_encryption_manager(make_encryption_manager("test-key"))
                .build(output_file)
                .await?;
        parquet_writer.write(&batch).await?;
        let data_file = parquet_writer
            .close()
            .await?
            .into_iter()
            .next()
            .unwrap()
            .content(DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        let raw = file_io
            .new_input(data_file.file_path.clone())?
            .read()
            .await?;
        assert!(
            ParquetRecordBatchReaderBuilder::try_new(raw).is_err(),
            "an encrypted parquet file must not be readable without decryption"
        );

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(data_file.file_size_in_bytes())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(data_file.file_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(iceberg_schema)
            .with_project_field_ids(vec![1])
            .with_case_sensitive(false)
            .with_key_metadata(data_file.key_metadata().map(Box::from))
            .build()
            .unwrap();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[10, 20, 30]);

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_writer_with_complex_schema() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        let schema = nested_schema_for_test();
        let arrow_schema: ArrowSchemaRef = Arc::new((&schema).try_into().unwrap());
        let col0 = Arc::new(Int64Array::from_iter_values(0..1024)) as ArrayRef;
        let col1 = Arc::new(StructArray::new(
            {
                if let DataType::Struct(fields) = arrow_schema.field(1).data_type() {
                    fields.clone()
                } else {
                    unreachable!()
                }
            },
            vec![
                Arc::new(Int64Array::from_iter_values(0..1024)),
                Arc::new(Int64Array::from_iter_values(0..1024)),
            ],
            None,
        ));
        let col2 = Arc::new(arrow_array::StringArray::from_iter_values(
            (0..1024).map(|n| n.to_string()),
        )) as ArrayRef;
        let col3 = Arc::new({
            let list_parts = ListArray::from_iter_primitive::<Int64Type, _, _>(
                (0..1024).map(|n| Some(vec![Some(n)])),
            )
            .into_parts();
            ListArray::new(
                {
                    if let DataType::List(field) = arrow_schema.field(3).data_type() {
                        field.clone()
                    } else {
                        unreachable!()
                    }
                },
                list_parts.1,
                list_parts.2,
                list_parts.3,
            )
        }) as ArrayRef;
        let col4 = Arc::new(StructArray::new(
            {
                if let DataType::Struct(fields) = arrow_schema.field(4).data_type() {
                    fields.clone()
                } else {
                    unreachable!()
                }
            },
            vec![Arc::new(StructArray::new(
                {
                    if let DataType::Struct(fields) = arrow_schema.field(4).data_type() {
                        if let DataType::Struct(fields) = fields[0].data_type() {
                            fields.clone()
                        } else {
                            unreachable!()
                        }
                    } else {
                        unreachable!()
                    }
                },
                vec![Arc::new(Int64Array::from_iter_values(0..1024))],
                None,
            ))],
            None,
        ));
        let col5 = Arc::new({
            let mut map_array_builder = MapBuilder::new(
                None,
                arrow_array::builder::StringBuilder::new(),
                arrow_array::builder::ListBuilder::new(arrow_array::builder::PrimitiveBuilder::<
                    Int64Type,
                >::new()),
            );
            for i in 0..1024 {
                map_array_builder.keys().append_value(i.to_string());
                map_array_builder
                    .values()
                    .append_value(vec![Some(i as i64); i + 1]);
                map_array_builder.append(true)?;
            }
            let (_, offset_buffer, struct_array, null_buffer, ordered) =
                map_array_builder.finish().into_parts();
            let struct_array = {
                let (_, mut arrays, nulls) = struct_array.into_parts();
                let list_array = {
                    let list_array = arrays[1]
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap()
                        .clone();
                    let (_, offsets, array, nulls) = list_array.into_parts();
                    let list_field = {
                        if let DataType::Map(map_field, _) = arrow_schema.field(5).data_type() {
                            if let DataType::Struct(fields) = map_field.data_type() {
                                if let DataType::List(list_field) = fields[1].data_type() {
                                    list_field.clone()
                                } else {
                                    unreachable!()
                                }
                            } else {
                                unreachable!()
                            }
                        } else {
                            unreachable!()
                        }
                    };
                    ListArray::new(list_field, offsets, array, nulls)
                };
                arrays[1] = Arc::new(list_array) as ArrayRef;
                StructArray::new(
                    {
                        if let DataType::Map(map_field, _) = arrow_schema.field(5).data_type() {
                            if let DataType::Struct(fields) = map_field.data_type() {
                                fields.clone()
                            } else {
                                unreachable!()
                            }
                        } else {
                            unreachable!()
                        }
                    },
                    arrays,
                    nulls,
                )
            };
            MapArray::new(
                {
                    if let DataType::Map(map_field, _) = arrow_schema.field(5).data_type() {
                        map_field.clone()
                    } else {
                        unreachable!()
                    }
                },
                offset_buffer,
                struct_array,
                null_buffer,
                ordered,
            )
        }) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![
            col0, col1, col2, col3, col4, col5,
        ])
        .unwrap();
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;

        // write data
        let mut pw =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), Arc::new(schema))
                .build(output_file)
                .await?;
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 1024);
        assert_eq!(
            *data_file.value_counts(),
            HashMap::from([
                (0, 1024),
                (5, 1024),
                (6, 1024),
                (2, 1024),
                (7, 1024),
                (9, 1024),
                (11, 1024),
                (13, (1..1025).sum()),
            ])
        );
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([
                (0, Datum::long(0)),
                (5, Datum::long(0)),
                (6, Datum::long(0)),
                (2, Datum::string("0")),
                (7, Datum::long(0)),
                (9, Datum::long(0)),
                (11, Datum::string("0")),
                (13, Datum::long(0))
            ])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([
                (0, Datum::long(1023)),
                (5, Datum::long(1023)),
                (6, Datum::long(1023)),
                (2, Datum::string("999")),
                (7, Datum::long(1023)),
                (9, Datum::long(1023)),
                (11, Datum::string("999")),
                (13, Datum::long(1023))
            ])
        );

        // check the written file
        check_parquet_data_file(&file_io, &data_file, &to_write).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_all_type_for_write() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        // generate iceberg schema for all type
        let schema = schema_for_all_type();
        let arrow_schema: ArrowSchemaRef = Arc::new((&schema).try_into().unwrap());
        let col0 = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
        ])) as ArrayRef;
        let col1 = Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)])) as ArrayRef;
        let col2 = Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(4)])) as ArrayRef;
        let col3 = Arc::new(Float32Array::from(vec![
            Some(0.5),
            Some(2.0),
            None,
            Some(3.5),
        ])) as ArrayRef;
        let col4 = Arc::new(Float64Array::from(vec![
            Some(0.5),
            Some(2.0),
            None,
            Some(3.5),
        ])) as ArrayRef;
        let col5 = Arc::new(arrow_array::StringArray::from(vec![
            Some("a"),
            Some("b"),
            None,
            Some("d"),
        ])) as ArrayRef;
        let col6 = Arc::new(arrow_array::LargeBinaryArray::from_opt_vec(vec![
            Some(b"one"),
            None,
            Some(b""),
            Some(b"zzzz"),
        ])) as ArrayRef;
        let col7 = Arc::new(arrow_array::Date32Array::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
        ])) as ArrayRef;
        let col8 = Arc::new(arrow_array::Time64MicrosecondArray::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
        ])) as ArrayRef;
        let col9 = Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
        ])) as ArrayRef;
        let col10 = Arc::new(
            arrow_array::TimestampMicrosecondArray::from(vec![Some(0), Some(1), None, Some(3)])
                .with_timezone_utc(),
        ) as ArrayRef;
        let col11 = Arc::new(arrow_array::TimestampNanosecondArray::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
        ])) as ArrayRef;
        let col12 = Arc::new(
            arrow_array::TimestampNanosecondArray::from(vec![Some(0), Some(1), None, Some(3)])
                .with_timezone_utc(),
        ) as ArrayRef;
        let col13 = Arc::new(
            Decimal128Array::from(vec![Some(1), Some(2), None, Some(100)])
                .with_precision_and_scale(10, 5)
                .unwrap(),
        ) as ArrayRef;
        let col14 = Arc::new(
            arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![
                    Some(Uuid::from_u128(0).as_bytes().to_vec()),
                    Some(Uuid::from_u128(1).as_bytes().to_vec()),
                    None,
                    Some(Uuid::from_u128(3).as_bytes().to_vec()),
                ]
                .into_iter(),
                16,
            )
            .unwrap(),
        ) as ArrayRef;
        let col15 = Arc::new(
            arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![
                    Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
                    Some(vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20]),
                    None,
                    Some(vec![21, 22, 23, 24, 25, 26, 27, 28, 29, 30]),
                ]
                .into_iter(),
                10,
            )
            .unwrap(),
        ) as ArrayRef;
        let col16 = Arc::new(
            Decimal128Array::from(vec![Some(1), Some(2), None, Some(100)])
                .with_precision_and_scale(38, 5)
                .unwrap(),
        ) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![
            col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13,
            col14, col15, col16,
        ])
        .unwrap();
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;

        // write data
        let mut pw =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), Arc::new(schema))
                .build(output_file)
                .await?;
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 4);
        assert!(data_file.value_counts().iter().all(|(_, &v)| { v == 4 }));
        assert!(
            data_file
                .null_value_counts()
                .iter()
                .all(|(_, &v)| { v == 1 })
        );
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([
                (0, Datum::bool(false)),
                (1, Datum::int(1)),
                (2, Datum::long(1)),
                (3, Datum::float(0.5_f32)),
                (4, Datum::double(0.5)),
                (5, Datum::string("a")),
                (6, Datum::binary(vec![])),
                (7, Datum::date(0)),
                (8, Datum::time_micros(0).unwrap()),
                (9, Datum::timestamp_micros(0)),
                (10, Datum::timestamptz_micros(0)),
                (11, Datum::timestamp_nanos(0)),
                (12, Datum::timestamptz_nanos(0)),
                (
                    13,
                    Datum::new(
                        PrimitiveType::Decimal {
                            precision: 10,
                            scale: 5
                        },
                        PrimitiveLiteral::Int128(1)
                    )
                ),
                (14, Datum::uuid(Uuid::from_u128(0))),
                (15, Datum::fixed(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
                (
                    16,
                    Datum::new(
                        PrimitiveType::Decimal {
                            precision: 38,
                            scale: 5
                        },
                        PrimitiveLiteral::Int128(1)
                    )
                ),
            ])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([
                (0, Datum::bool(true)),
                (1, Datum::int(4)),
                (2, Datum::long(4)),
                (3, Datum::float(3.5_f32)),
                (4, Datum::double(3.5)),
                (5, Datum::string("d")),
                (6, Datum::binary(vec![122, 122, 122, 122])),
                (7, Datum::date(3)),
                (8, Datum::time_micros(3).unwrap()),
                (9, Datum::timestamp_micros(3)),
                (10, Datum::timestamptz_micros(3)),
                (11, Datum::timestamp_nanos(3)),
                (12, Datum::timestamptz_nanos(3)),
                (
                    13,
                    Datum::new(
                        PrimitiveType::Decimal {
                            precision: 10,
                            scale: 5
                        },
                        PrimitiveLiteral::Int128(100)
                    )
                ),
                (14, Datum::uuid(Uuid::from_u128(3))),
                (
                    15,
                    Datum::fixed(vec![21, 22, 23, 24, 25, 26, 27, 28, 29, 30])
                ),
                (
                    16,
                    Datum::new(
                        PrimitiveType::Decimal {
                            precision: 38,
                            scale: 5
                        },
                        PrimitiveLiteral::Int128(100)
                    )
                ),
            ])
        );

        // check the written file
        check_parquet_data_file(&file_io, &data_file, &to_write).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_decimal_bound() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // test 1.1 and 2.2
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        0,
                        "decimal",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 28,
                            scale: 10,
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;
        let mut pw = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone())
            .build(output_file)
            .await?;
        let col0 = Arc::new(
            Decimal128Array::from(vec![Some(22000000000), Some(11000000000)])
                .with_data_type(DataType::Decimal128(28, 10)),
        ) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col0]).unwrap();
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            .content(DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();
        assert_eq!(
            data_file.upper_bounds().get(&0),
            Some(Datum::decimal_with_precision(decimal_new(22000000000_i64, 10), 28).unwrap())
                .as_ref()
        );
        assert_eq!(
            data_file.lower_bounds().get(&0),
            Some(Datum::decimal_with_precision(decimal_new(11000000000_i64, 10), 28).unwrap())
                .as_ref()
        );

        // test -1.1 and -2.2
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        0,
                        "decimal",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 28,
                            scale: 10,
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;
        let mut pw = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone())
            .build(output_file)
            .await?;
        let col0 = Arc::new(
            Decimal128Array::from(vec![Some(-22000000000), Some(-11000000000)])
                .with_data_type(DataType::Decimal128(28, 10)),
        ) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col0]).unwrap();
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            .content(DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();
        assert_eq!(
            data_file.upper_bounds().get(&0),
            Some(Datum::decimal_with_precision(decimal_new(-11000000000_i64, 10), 28).unwrap())
                .as_ref()
        );
        assert_eq!(
            data_file.lower_bounds().get(&0),
            Some(Datum::decimal_with_precision(decimal_new(-22000000000_i64, 10), 28).unwrap())
                .as_ref()
        );

        // test 38-digit precision decimal values (Iceberg spec max)
        // Note: fastnum D128::MAX/MIN have impractical exponents, so we use meaningful values
        use crate::spec::decimal_utils::decimal_from_str_exact;
        let decimal_max = decimal_from_str_exact("99999999999999999999999999999999999999").unwrap();
        let decimal_min =
            decimal_from_str_exact("-99999999999999999999999999999999999999").unwrap();
        assert_eq!(decimal_scale(&decimal_max), decimal_scale(&decimal_min));
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        0,
                        "decimal",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 38,
                            scale: decimal_scale(&decimal_max),
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;
        let mut pw = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema)
            .build(output_file)
            .await?;
        let col0 = Arc::new(
            Decimal128Array::from(vec![
                Some(decimal_mantissa(&decimal_max)),
                Some(decimal_mantissa(&decimal_min)),
            ])
            .with_data_type(DataType::Decimal128(38, 0)),
        ) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col0]).unwrap();
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            .content(DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();
        assert_eq!(
            data_file.upper_bounds().get(&0),
            Some(Datum::decimal(decimal_max).unwrap()).as_ref()
        );
        assert_eq!(
            data_file.lower_bounds().get(&0),
            Some(Datum::decimal(decimal_min).unwrap()).as_ref()
        );

        // test max and min for scale 38
        // # TODO
        // Readd this case after resolve https://github.com/apache/iceberg-rust/issues/669
        // let schema = Arc::new(
        //     Schema::builder()
        //         .with_fields(vec![NestedField::optional(
        //             0,
        //             "decimal",
        //             Type::Primitive(PrimitiveType::Decimal {
        //                 precision: 38,
        //                 scale: 0,
        //             }),
        //         )
        //         .into()])
        //         .build()
        //         .unwrap(),
        // );
        // let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        // let mut pw = ParquetWriterBuilder::new(
        //     WriterProperties::builder().build(),
        //     schema,
        //     file_io.clone(),
        //     loccation_gen,
        //     file_name_gen,
        // )
        // .build()
        // .await?;
        // let col0 = Arc::new(
        //     Decimal128Array::from(vec![
        //         Some(99999999999999999999999999999999999999_i128),
        //         Some(-99999999999999999999999999999999999999_i128),
        //     ])
        //     .with_data_type(DataType::Decimal128(38, 0)),
        // ) as ArrayRef;
        // let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col0]).unwrap();
        // pw.write(&to_write).await?;
        // let res = pw.close().await?;
        // assert_eq!(res.len(), 1);
        // let data_file = res
        //     .into_iter()
        //     .next()
        //     .unwrap()
        //     .content(crate::spec::DataContentType::Data)
        //     .partition(Struct::empty())
        //     .build()
        //     .unwrap();
        // assert_eq!(
        //     data_file.upper_bounds().get(&0),
        //     Some(Datum::new(
        //         PrimitiveType::Decimal {
        //             precision: 38,
        //             scale: 0
        //         },
        //         PrimitiveLiteral::Int128(99999999999999999999999999999999999999_i128)
        //     ))
        //     .as_ref()
        // );
        // assert_eq!(
        //     data_file.lower_bounds().get(&0),
        //     Some(Datum::new(
        //         PrimitiveType::Decimal {
        //             precision: 38,
        //             scale: 0
        //         },
        //         PrimitiveLiteral::Int128(-99999999999999999999999999999999999999_i128)
        //     ))
        //     .as_ref()
        // );

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_write() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Test that file will create if data to write
        let schema = {
            let fields =
                vec![
                    Field::new("col", DataType::Int64, true).with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "0".to_string(),
                    )])),
                ];
            Arc::new(arrow_schema::Schema::new(fields))
        };
        let col = Arc::new(Int64Array::from_iter_values(0..1024)) as ArrayRef;
        let to_write = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();
        let file_path = location_gen.generate_location(None, &file_name_gen.generate_file_name());
        let output_file = file_io.new_output(&file_path)?;
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(to_write.schema().as_ref().try_into().unwrap()),
        )
        .build(output_file)
        .await?;
        pw.write(&to_write).await?;
        pw.close().await.unwrap();
        assert!(file_io.exists(&file_path).await.unwrap());

        // Test that file will not create if no data to write
        let file_name_gen =
            DefaultFileNameGenerator::new("test_empty".to_string(), None, DataFileFormat::Parquet);
        let file_path = location_gen.generate_location(None, &file_name_gen.generate_file_name());
        let output_file = file_io.new_output(&file_path)?;
        let pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(to_write.schema().as_ref().try_into().unwrap()),
        )
        .build(output_file)
        .await?;
        pw.close().await.unwrap();
        assert!(!file_io.exists(&file_path).await.unwrap());

        Ok(())
    }

    #[tokio::test]
    async fn test_nan_val_cnts_primitive_type() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        let arrow_schema = {
            let fields = vec![
                Field::new("col", DataType::Float32, false).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "0".to_string(),
                )])),
                Field::new("col2", DataType::Float64, false).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "1".to_string(),
                )])),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let float_32_col = Arc::new(Float32Array::from_iter_values_with_nulls(
            [1.0_f32, f32::NAN, 2.0, 2.0].into_iter(),
            None,
        )) as ArrayRef;

        let float_64_col = Arc::new(Float64Array::from_iter_values_with_nulls(
            [1.0_f64, f64::NAN, 2.0, 2.0].into_iter(),
            None,
        )) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![float_32_col, float_64_col]).unwrap();
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(to_write.schema().as_ref().try_into().unwrap()),
        )
        .build(output_file)
        .await?;

        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 4);
        assert_eq!(*data_file.value_counts(), HashMap::from([(0, 4), (1, 4)]));
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([(0, Datum::float(1.0_f32)), (1, Datum::double(1.0)),])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([(0, Datum::float(2.0_f32)), (1, Datum::double(2.0)),])
        );
        assert_eq!(
            *data_file.null_value_counts(),
            HashMap::from([(0, 0), (1, 0)])
        );
        assert_eq!(
            *data_file.nan_value_counts(),
            HashMap::from([(0, 1), (1, 1)])
        );

        // check the written file
        let expect_batch = concat_batches(&arrow_schema, vec![&to_write]).unwrap();
        check_parquet_data_file(&file_io, &data_file, &expect_batch).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_nan_val_cnts_struct_type() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let schema_struct_float_fields = Fields::from(vec![
            Field::new("col4", DataType::Float32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "4".to_string(),
            )])),
        ]);

        let schema_struct_nested_float_fields = Fields::from(vec![
            Field::new("col7", DataType::Float32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "7".to_string(),
            )])),
        ]);

        let schema_struct_nested_fields = Fields::from(vec![
            Field::new(
                "col6",
                DataType::Struct(schema_struct_nested_float_fields.clone()),
                false,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "6".to_string(),
            )])),
        ]);

        // prepare data
        let arrow_schema = {
            let fields = vec![
                Field::new(
                    "col3",
                    DataType::Struct(schema_struct_float_fields.clone()),
                    false,
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "3".to_string(),
                )])),
                Field::new(
                    "col5",
                    DataType::Struct(schema_struct_nested_fields.clone()),
                    false,
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "5".to_string(),
                )])),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let float_32_col = Arc::new(Float32Array::from_iter_values_with_nulls(
            [1.0_f32, f32::NAN, 2.0, 2.0].into_iter(),
            None,
        )) as ArrayRef;

        let struct_float_field_col = Arc::new(StructArray::new(
            schema_struct_float_fields,
            vec![float_32_col.clone()],
            None,
        )) as ArrayRef;

        let struct_nested_float_field_col = Arc::new(StructArray::new(
            schema_struct_nested_fields,
            vec![Arc::new(StructArray::new(
                schema_struct_nested_float_fields,
                vec![float_32_col.clone()],
                None,
            )) as ArrayRef],
            None,
        )) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![
            struct_float_field_col,
            struct_nested_float_field_col,
        ])
        .unwrap();
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(to_write.schema().as_ref().try_into().unwrap()),
        )
        .build(output_file)
        .await?;

        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 4);
        assert_eq!(*data_file.value_counts(), HashMap::from([(4, 4), (7, 4)]));
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([(4, Datum::float(1.0_f32)), (7, Datum::float(1.0_f32)),])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([(4, Datum::float(2.0_f32)), (7, Datum::float(2.0_f32)),])
        );
        assert_eq!(
            *data_file.null_value_counts(),
            HashMap::from([(4, 0), (7, 0)])
        );
        assert_eq!(
            *data_file.nan_value_counts(),
            HashMap::from([(4, 1), (7, 1)])
        );

        // check the written file
        let expect_batch = concat_batches(&arrow_schema, vec![&to_write]).unwrap();
        check_parquet_data_file(&file_io, &data_file, &expect_batch).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_nan_val_cnts_list_type() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let schema_list_float_field = Field::new("element", DataType::Float32, true).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
        );

        let schema_struct_list_float_field = Field::new("element", DataType::Float32, true)
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "4".to_string(),
            )]));

        let schema_struct_list_field = Fields::from(vec![
            Field::new_list("col2", schema_struct_list_float_field.clone(), true).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())]),
            ),
        ]);

        let arrow_schema = {
            let fields = vec![
                Field::new_list("col0", schema_list_float_field.clone(), true).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "0".to_string())]),
                ),
                Field::new_struct("col1", schema_struct_list_field.clone(), true)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "2".to_string(),
                    )]))
                    .clone(),
                // Field::new_large_list("col3", schema_large_list_float_field.clone(), true).with_metadata(
                //     HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "5".to_string())]),
                // ).clone(),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let list_parts = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
            Some(1.0_f32),
            Some(f32::NAN),
            Some(2.0),
            Some(2.0),
        ])])
        .into_parts();

        let list_float_field_col = Arc::new({
            let list_parts = list_parts.clone();
            ListArray::new(
                {
                    if let DataType::List(field) = arrow_schema.field(0).data_type() {
                        field.clone()
                    } else {
                        unreachable!()
                    }
                },
                list_parts.1,
                list_parts.2,
                list_parts.3,
            )
        }) as ArrayRef;

        let struct_list_fields_schema =
            if let DataType::Struct(fields) = arrow_schema.field(1).data_type() {
                fields.clone()
            } else {
                unreachable!()
            };

        let struct_list_float_field_col = Arc::new({
            ListArray::new(
                {
                    if let DataType::List(field) = struct_list_fields_schema
                        .first()
                        .expect("could not find first list field")
                        .data_type()
                    {
                        field.clone()
                    } else {
                        unreachable!()
                    }
                },
                list_parts.1,
                list_parts.2,
                list_parts.3,
            )
        }) as ArrayRef;

        let struct_list_float_field_col = Arc::new(StructArray::new(
            struct_list_fields_schema,
            vec![struct_list_float_field_col.clone()],
            None,
        )) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![
            list_float_field_col,
            struct_list_float_field_col,
            // large_list_float_field_col,
        ])
        .expect("Could not form record batch");
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(
                to_write
                    .schema()
                    .as_ref()
                    .try_into()
                    .expect("Could not convert iceberg schema"),
            ),
        )
        .build(output_file)
        .await?;

        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            .content(DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 1);
        assert_eq!(*data_file.value_counts(), HashMap::from([(1, 4), (4, 4)]));
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([(1, Datum::float(1.0_f32)), (4, Datum::float(1.0_f32))])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([(1, Datum::float(2.0_f32)), (4, Datum::float(2.0_f32))])
        );
        assert_eq!(
            *data_file.null_value_counts(),
            HashMap::from([(1, 0), (4, 0)])
        );
        assert_eq!(
            *data_file.nan_value_counts(),
            HashMap::from([(1, 1), (4, 1)])
        );

        // check the written file
        let expect_batch = concat_batches(&arrow_schema, vec![&to_write]).unwrap();
        check_parquet_data_file(&file_io, &data_file, &expect_batch).await;

        Ok(())
    }

    macro_rules! construct_map_arr {
        ($map_key_field_schema:ident, $map_value_field_schema:ident) => {{
            let int_builder = Int32Builder::new();
            let float_builder = Float32Builder::with_capacity(4);
            let mut builder = MapBuilder::new(None, int_builder, float_builder);
            builder.keys().append_value(1);
            builder.values().append_value(1.0_f32);
            builder.append(true).unwrap();
            builder.keys().append_value(2);
            builder.values().append_value(f32::NAN);
            builder.append(true).unwrap();
            builder.keys().append_value(3);
            builder.values().append_value(2.0);
            builder.append(true).unwrap();
            builder.keys().append_value(4);
            builder.values().append_value(2.0);
            builder.append(true).unwrap();
            let array = builder.finish();

            let (_field, offsets, entries, nulls, ordered) = array.into_parts();
            let new_struct_fields_schema =
                Fields::from(vec![$map_key_field_schema, $map_value_field_schema]);

            let entries = {
                let (_, arrays, nulls) = entries.into_parts();
                StructArray::new(new_struct_fields_schema.clone(), arrays, nulls)
            };

            let field = Arc::new(Field::new(
                DEFAULT_MAP_FIELD_NAME,
                DataType::Struct(new_struct_fields_schema),
                false,
            ));

            Arc::new(MapArray::new(field, offsets, entries, nulls, ordered))
        }};
    }

    #[tokio::test]
    async fn test_nan_val_cnts_map_type() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let map_key_field_schema =
            Field::new(MAP_KEY_FIELD_NAME, DataType::Int32, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
            ]));

        let map_value_field_schema =
            Field::new(MAP_VALUE_FIELD_NAME, DataType::Float32, true).with_metadata(HashMap::from(
                [(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())],
            ));

        let struct_map_key_field_schema =
            Field::new(MAP_KEY_FIELD_NAME, DataType::Int32, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "6".to_string()),
            ]));

        let struct_map_value_field_schema =
            Field::new(MAP_VALUE_FIELD_NAME, DataType::Float32, true).with_metadata(HashMap::from(
                [(PARQUET_FIELD_ID_META_KEY.to_string(), "7".to_string())],
            ));

        let schema_struct_map_field = Fields::from(vec![
            Field::new_map(
                "col3",
                DEFAULT_MAP_FIELD_NAME,
                struct_map_key_field_schema.clone(),
                struct_map_value_field_schema.clone(),
                false,
                false,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "5".to_string(),
            )])),
        ]);

        let arrow_schema = {
            let fields = vec![
                Field::new_map(
                    "col0",
                    DEFAULT_MAP_FIELD_NAME,
                    map_key_field_schema.clone(),
                    map_value_field_schema.clone(),
                    false,
                    false,
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "0".to_string(),
                )])),
                Field::new_struct("col1", schema_struct_map_field.clone(), true)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "3".to_string(),
                    )]))
                    .clone(),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let map_array = construct_map_arr!(map_key_field_schema, map_value_field_schema);

        let struct_map_arr =
            construct_map_arr!(struct_map_key_field_schema, struct_map_value_field_schema);

        let struct_list_float_field_col = Arc::new(StructArray::new(
            schema_struct_map_field,
            vec![struct_map_arr],
            None,
        )) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![
            map_array,
            struct_list_float_field_col,
        ])
        .expect("Could not form record batch");
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(
                to_write
                    .schema()
                    .as_ref()
                    .try_into()
                    .expect("Could not convert iceberg schema"),
            ),
        )
        .build(output_file)
        .await?;

        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            .content(DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 4);
        assert_eq!(
            *data_file.value_counts(),
            HashMap::from([(1, 4), (2, 4), (6, 4), (7, 4)])
        );
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([
                (1, Datum::int(1)),
                (2, Datum::float(1.0_f32)),
                (6, Datum::int(1)),
                (7, Datum::float(1.0_f32))
            ])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([
                (1, Datum::int(4)),
                (2, Datum::float(2.0_f32)),
                (6, Datum::int(4)),
                (7, Datum::float(2.0_f32))
            ])
        );
        assert_eq!(
            *data_file.null_value_counts(),
            HashMap::from([(1, 0), (2, 0), (6, 0), (7, 0)])
        );
        assert_eq!(
            *data_file.nan_value_counts(),
            HashMap::from([(2, 1), (7, 1)])
        );

        // check the written file
        let expect_batch = concat_batches(&arrow_schema, vec![&to_write]).unwrap();
        check_parquet_data_file(&file_io, &data_file, &expect_batch).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_write_empty_parquet_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);
        let output_file = file_io
            .new_output(location_gen.generate_location(None, &file_name_gen.generate_file_name()))
            .unwrap();

        // write data
        let pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(
                Schema::builder()
                    .with_schema_id(1)
                    .with_fields(vec![
                        NestedField::required(0, "col", Type::Primitive(PrimitiveType::Long))
                            .with_id(0)
                            .into(),
                    ])
                    .build()
                    .expect("Failed to create schema"),
            ),
        )
        .build(output_file)
        .await
        .unwrap();

        let res = pw.close().await.unwrap();
        assert_eq!(res.len(), 0);

        // Check that file should have been deleted.
        assert_eq!(std::fs::read_dir(temp_dir.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn test_parquet_writer_uuid_logical_type() {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);
        let output_file = file_io
            .new_output(location_gen.generate_location(None, &file_name_gen.generate_file_name()))
            .unwrap();

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(0, "uuid", Type::Primitive(PrimitiveType::Uuid)).into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let col = Arc::new(
            arrow_array::FixedSizeBinaryArray::try_from_iter(
                [Uuid::from_u128(0), Uuid::from_u128(1)]
                    .into_iter()
                    .map(|uuid| uuid.into_bytes()),
            )
            .unwrap(),
        ) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema, vec![col]).unwrap();

        let mut pw = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema)
            .build(output_file)
            .await
            .unwrap();
        pw.write(&to_write).await.unwrap();
        let data_file = pw
            .close()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .content(DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        let raw = file_io
            .new_input(data_file.file_path.clone())
            .unwrap()
            .read()
            .await
            .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(raw).unwrap();
        let column = reader.metadata().file_metadata().schema_descr().column(0);
        assert_eq!(
            (
                column.physical_type(),
                column.type_length(),
                column.logical_type_ref()
            ),
            (
                parquet::basic::Type::FIXED_LEN_BYTE_ARRAY,
                16,
                Some(&parquet::basic::LogicalType::Uuid)
            )
        );
    }

    #[test]
    fn test_min_max_aggregator() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(0, "col", Type::Primitive(PrimitiveType::Int))
                        .with_id(0)
                        .into(),
                ])
                .build()
                .expect("Failed to create schema"),
        );
        let mut min_max_agg = MinMaxColAggregator::new(schema);
        let create_statistics =
            |min, max| Statistics::Int32(ValueStatistics::new(min, max, None, None, false));
        min_max_agg
            .update(0, &create_statistics(None, Some(42)))
            .unwrap();
        min_max_agg
            .update(0, &create_statistics(Some(0), Some(i32::MAX)))
            .unwrap();
        min_max_agg
            .update(0, &create_statistics(Some(i32::MIN), None))
            .unwrap();
        min_max_agg
            .update(0, &create_statistics(None, None))
            .unwrap();

        let (lower_bounds, upper_bounds) = min_max_agg.produce();

        assert_eq!(lower_bounds, HashMap::from([(0, Datum::int(i32::MIN))]));
        assert_eq!(upper_bounds, HashMap::from([(0, Datum::int(i32::MAX))]));
    }

    fn cdc_test_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::required(2, "payload", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_iceberg_schema_metadata_preserves_custom_entries() {
        let schema = cdc_test_schema();
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![
                KeyValue::new("custom.key".to_string(), "custom-value".to_string()),
                KeyValue::new(ICEBERG_SCHEMA_KEY.to_string(), "stale-schema".to_string()),
            ]))
            .build();

        let builder = ParquetWriterBuilder::new(props, schema.clone());
        let metadata = builder.props.key_value_metadata().unwrap();
        assert_eq!(
            metadata
                .iter()
                .find(|entry| entry.key == "custom.key")
                .and_then(|entry| entry.value.as_deref()),
            Some("custom-value")
        );
        let iceberg_entries = metadata
            .iter()
            .filter(|entry| entry.key == ICEBERG_SCHEMA_KEY)
            .collect::<Vec<_>>();
        assert_eq!(iceberg_entries.len(), 1);
        assert_eq!(
            serde_json::from_str::<Schema>(
                iceberg_entries[0].value.as_deref().expect("schema value")
            )
            .unwrap(),
            *schema
        );
    }

    #[test]
    fn test_from_table_properties_no_cdc_by_default() {
        let raw_properties = HashMap::new();
        let tp = TableProperties::new(&raw_properties);
        let builder = ParquetWriterBuilder::from_table_properties(&tp, cdc_test_schema()).unwrap();
        assert!(builder.props.content_defined_chunking().is_none());
    }

    #[tokio::test]
    async fn test_from_table_properties_without_encryption_writes_plaintext() {
        let raw_properties = HashMap::new();
        let tp = TableProperties::new(&raw_properties);
        let tmp = TempDir::new().unwrap();
        let output = FileIO::new_with_fs()
            .new_output(format!("{}/plain.parquet", tmp.path().to_str().unwrap()))
            .unwrap();
        let writer = ParquetWriterBuilder::from_table_properties(&tp, cdc_test_schema())
            .unwrap()
            .build(output)
            .await
            .unwrap();

        assert!(writer.key_metadata.is_none());
        assert!(
            writer
                .writer_properties
                .file_encryption_properties()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_from_table_properties_propagate_to_writer() {
        // `build()` must carry the translated `WriterProperties` through to the
        // `ParquetWriter` unchanged — otherwise the `write.parquet.*` settings
        // derived in `from_table_properties` would never reach parquet-rs.
        //
        // Asserting on the writer's `WriterProperties` (rather than re-reading a
        // written file) keeps this a direct propagation check: every future
        // `write.parquet.*` option just adds an assertion on its corresponding
        // `WriterProperties` getter here.
        let raw_properties = HashMap::from([
            (
                TableProperties::PROPERTY_PARQUET_CDC_ENABLED.to_string(),
                "true".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE.to_string(),
                "4096".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE.to_string(),
                "8192".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_NORM_LEVEL.to_string(),
                "2".to_string(),
            ),
        ]);
        let tp = TableProperties::new(&raw_properties);

        let tmp = TempDir::new().unwrap();
        let output = FileIO::new_with_fs()
            .new_output(format!("{}/cdc.parquet", tmp.path().to_str().unwrap()))
            .unwrap();
        let writer = ParquetWriterBuilder::from_table_properties(&tp, cdc_test_schema())
            .unwrap()
            .build(output)
            .await
            .unwrap();

        let cdc = writer
            .writer_properties
            .content_defined_chunking()
            .copied()
            .expect("CDC should be enabled on the built writer");
        assert_eq!(cdc.min_chunk_size, 4096);
        assert_eq!(cdc.max_chunk_size, 8192);
        assert_eq!(cdc.norm_level, 2);
    }

    #[test]
    fn test_from_table_properties_sizing_defaults() {
        // With no properties set, the writer must use Iceberg's defaults (which
        // differ from parquet-rs's own defaults), not parquet-rs's.
        let raw_properties = HashMap::new();
        let tp = TableProperties::new(&raw_properties);
        let props = ParquetWriterBuilder::from_table_properties(&tp, cdc_test_schema())
            .unwrap()
            .props;

        assert_eq!(
            props.max_row_group_bytes(),
            Some(TableProperties::PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT)
        );
        assert_eq!(
            props.data_page_size_limit(),
            TableProperties::PROPERTY_PARQUET_PAGE_SIZE_BYTES_DEFAULT
        );
        assert_eq!(
            props.data_page_row_count_limit(),
            TableProperties::PROPERTY_PARQUET_PAGE_ROW_LIMIT_DEFAULT
        );
        assert_eq!(
            props.dictionary_page_size_limit(),
            TableProperties::PROPERTY_PARQUET_DICT_SIZE_BYTES_DEFAULT
        );
        // Default codec is zstd at the Java-aligned default level (3).
        assert_eq!(
            props.compression(&ColumnPath::from("id")),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap())
        );
    }

    #[test]
    fn test_from_table_properties_sizing_and_compression_overrides() {
        let raw_properties = HashMap::from([
            (
                TableProperties::PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES.to_string(),
                "1048576".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_PAGE_SIZE_BYTES.to_string(),
                "65536".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_PAGE_ROW_LIMIT.to_string(),
                "5000".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_DICT_SIZE_BYTES.to_string(),
                "131072".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC.to_string(),
                "gzip".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_COMPRESSION_LEVEL.to_string(),
                "9".to_string(),
            ),
        ]);
        let tp = TableProperties::new(&raw_properties);
        let props = ParquetWriterBuilder::from_table_properties(&tp, cdc_test_schema())
            .unwrap()
            .props;

        assert_eq!(props.max_row_group_bytes(), Some(1048576));
        assert_eq!(props.data_page_size_limit(), 65536);
        assert_eq!(props.data_page_row_count_limit(), 5000);
        assert_eq!(props.dictionary_page_size_limit(), 131072);
        assert_eq!(
            props.compression(&ColumnPath::from("id")),
            Compression::GZIP(GzipLevel::try_new(9).unwrap())
        );
    }

    #[test]
    fn test_from_table_properties_invalid_codec_errors() {
        let entries = HashMap::from([(
            TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC.to_string(),
            "bogus".to_string(),
        )]);
        let err = ParquetWriterBuilder::from_table_properties(
            &TableProperties::new(&entries),
            cdc_test_schema(),
        )
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("bogus"));
    }

    #[test]
    fn test_parquet_compression_mapping() {
        // Codecs without a level.
        assert_eq!(
            parquet_compression(CompressionCodec::None).unwrap(),
            Compression::UNCOMPRESSED
        );
        assert_eq!(
            parquet_compression(CompressionCodec::Snappy).unwrap(),
            Compression::SNAPPY
        );
        assert_eq!(
            parquet_compression(CompressionCodec::Lz4).unwrap(),
            Compression::LZ4
        );
        assert_eq!(
            parquet_compression(CompressionCodec::Lz4Raw).unwrap(),
            Compression::LZ4_RAW
        );
        assert_eq!(
            parquet_compression(CompressionCodec::Lzo).unwrap(),
            Compression::LZO
        );

        // Level-carrying codecs at their defaults.
        assert_eq!(
            parquet_compression(CompressionCodec::zstd_default()).unwrap(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap())
        );
        assert_eq!(
            parquet_compression(CompressionCodec::gzip_default()).unwrap(),
            Compression::GZIP(GzipLevel::try_new(6).unwrap())
        );
        assert_eq!(
            parquet_compression(CompressionCodec::brotli_default()).unwrap(),
            Compression::BROTLI(BrotliLevel::try_new(1).unwrap())
        );

        // Explicit levels are honored.
        assert_eq!(
            parquet_compression(CompressionCodec::Zstd(10)).unwrap(),
            Compression::ZSTD(ZstdLevel::try_new(10).unwrap())
        );
    }

    #[test]
    fn test_parquet_compression_invalid_level() {
        // zstd valid range is 1..=22; 99 is out of range.
        let err = parquet_compression(CompressionCodec::Zstd(99)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("zstd"));
    }

    /// SQE PATCH (v3-unknown): a V3 `unknown` column is never written to the
    /// data file; the other columns are.
    #[tokio::test]
    async fn test_parquet_writer_skips_unknown_column() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "u", Type::Primitive(PrimitiveType::Unknown)).into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let to_write = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int64Array::from_iter_values(0..3)) as ArrayRef,
            Arc::new(arrow_array::NullArray::new(3)) as ArrayRef,
        ])?;

        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;
        let file_path = output_file.location().to_string();
        let mut pw = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone())
            .build(output_file)
            .await?;
        pw.write(&to_write).await?;
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);

        let bytes = file_io.new_input(&file_path)?.read().await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap().build().unwrap();
        let batches = reader.map(|b| b.unwrap()).collect::<Vec<_>>();
        let read = concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(read.num_rows(), 6);
        let names: Vec<_> = read.schema().fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["id".to_string()]);
        Ok(())
    }

    /// SQE PATCH (v3-unknown): a nested `unknown` struct member is left out
    /// of the file, inside a struct column and inside a list of structs.
    #[tokio::test]
    async fn test_parquet_writer_skips_nested_unknown_field() -> Result<()> {
        use arrow_array::cast::AsArray;
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let member = |a: i32, u: i32| {
            Type::Struct(StructType::new(vec![
                NestedField::optional(a, "a", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(u, "u", Type::Primitive(PrimitiveType::Unknown)).into(),
            ]))
        };
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "s", member(4, 5)).into(),
                    NestedField::optional(
                        3,
                        "l",
                        Type::List(ListType::new(
                            NestedField::list_element(6, member(7, 8), false).into(),
                        )),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let DataType::Struct(s_fields) = arrow_schema.field(1).data_type().clone() else {
            unreachable!()
        };
        let DataType::List(l_field) = arrow_schema.field(2).data_type().clone() else {
            unreachable!()
        };
        let DataType::Struct(l_fields) = l_field.data_type().clone() else {
            unreachable!()
        };
        let members = |fields: &Fields, n: usize| {
            StructArray::new(
                fields.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..n as i32)) as ArrayRef,
                    Arc::new(arrow_array::NullArray::new(n)) as ArrayRef,
                ],
                None,
            )
        };
        let to_write = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int64Array::from_iter_values(0..2)) as ArrayRef,
            Arc::new(members(&s_fields, 2)) as ArrayRef,
            Arc::new(ListArray::new(
                l_field,
                arrow_buffer::OffsetBuffer::from_lengths([1, 2]),
                Arc::new(members(&l_fields, 3)),
                None,
            )) as ArrayRef,
        ])?;

        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;
        let file_path = output_file.location().to_string();
        let mut pw = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone())
            .build(output_file)
            .await?;
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);

        let bytes = file_io.new_input(&file_path)?.read().await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap().build().unwrap();
        let batches = reader.map(|b| b.unwrap()).collect::<Vec<_>>();
        let read = concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(read.num_rows(), 2);
        let s = read.column(1).as_struct();
        assert_eq!(s.num_columns(), 1);
        assert_eq!(s.column_names(), vec!["a"]);
        let l = read.column(2).as_list::<i32>();
        assert_eq!(l.values().as_struct().column_names(), vec!["a"]);
        assert_eq!(l.values().len(), 3);
        Ok(())
    }

    /// SQE PATCH (v3-unknown): an unknown field that cannot be left out
    /// without losing its parent is refused, not written as a Null column
    /// other engines cannot read.
    #[test]
    fn test_prune_unknown_type() {
        use arrow_schema::{DataType, Field, Fields};
        let nested = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("u", DataType::Null, true),
        ]));
        let pruned = DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)]));
        assert_eq!(prune_unknown_type("c", &nested).unwrap(), Some(pruned));
        assert_eq!(prune_unknown_type("c", &DataType::Null).unwrap(), None);
        assert_eq!(
            prune_unknown_type("c", &DataType::Int32).unwrap(),
            Some(DataType::Int32)
        );
        let all_unknown =
            DataType::Struct(Fields::from(vec![Field::new("u", DataType::Null, true)]));
        let list_of_unknown =
            DataType::List(Arc::new(Field::new("element", DataType::Null, true)));
        for refused in [all_unknown, list_of_unknown] {
            let err = prune_unknown_type("c", &refused).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
        }
        assert!(contains_null(&nested));
        assert!(!contains_null(&DataType::Int32));
    }
}
