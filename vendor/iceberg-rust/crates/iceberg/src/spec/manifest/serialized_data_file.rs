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

use serde::{Deserialize, Serialize};

use super::_serde::DataFileSerde;
use super::DataFile;
use crate::Result;
use crate::spec::{FormatVersion, Schema, StructType, Type};

/// A serde-compatible representation of an Iceberg [`DataFile`].
///
/// The partition spec id is intentionally not embedded because it belongs to
/// the manifest containing the data file. Callers must persist it alongside
/// this value and supply it when converting back to [`DataFile`].
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SerializedDataFile(DataFileSerde);

impl SerializedDataFile {
    /// Convert a [`DataFile`] into its stable serialized representation.
    pub fn try_from(
        data_file: DataFile,
        partition_type: &StructType,
        format_version: FormatVersion,
    ) -> Result<Self> {
        Ok(Self(DataFileSerde::try_from(
            data_file,
            &Type::Struct(partition_type.clone()),
            format_version,
        )?))
    }

    /// Materialize a [`DataFile`] using its manifest context.
    pub fn try_into(
        self,
        partition_spec_id: i32,
        partition_type: &StructType,
        schema: &Schema,
    ) -> Result<DataFile> {
        self.0
            .try_into(partition_spec_id, &Type::Struct(partition_type.clone()), schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Schema, StructType};

    #[test]
    fn test_json_round_trip_preserves_v3_fields() {
        let schema = Schema::builder().build().unwrap();
        let partition_type = StructType::new(vec![]);
        let data_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("s3://bucket/table/data/deletes.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .record_count(3)
            .file_size_in_bytes(128)
            .partition_spec_id(7)
            .referenced_data_file(Some("s3://bucket/table/data/data.parquet".to_string()))
            .content_offset(Some(16))
            .content_size_in_bytes(Some(48))
            .key_metadata(Some(vec![1, 2, 3]))
            .build()
            .unwrap();

        let serialized =
            SerializedDataFile::try_from(data_file.clone(), &partition_type, FormatVersion::V3)
                .unwrap();
        let json = serde_json::to_string(&serialized).unwrap();
        let decoded: SerializedDataFile = serde_json::from_str(&json).unwrap();
        let actual = decoded.try_into(7, &partition_type, &schema).unwrap();

        assert_eq!(actual, data_file);
    }
}
