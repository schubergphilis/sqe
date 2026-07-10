#![allow(non_snake_case)]
//! OpenLineage 2-0-2 RunEvent types.
//!
//! Field names are camelCase to match the OL wire format exactly.
//! See `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md` §4.1.

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::collections::BTreeMap;

pub const SCHEMA_URL: &str = "https://openlineage.io/spec/2-0-2/OpenLineage.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum EventType { Start, Complete, Fail }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunEvent {
    pub eventType: EventType,
    pub eventTime: String,
    pub producer: String,
    pub schemaURL: String,
    pub run: Run,
    pub job: Job,
    pub inputs: Vec<InputDataset>,
    pub outputs: Vec<OutputDataset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub runId: Uuid,
    #[serde(default)]
    pub facets: RunFacets,
}

impl Run {
    pub fn new(id: Uuid) -> Self { Self { runId: id, facets: Default::default() } }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Job {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub facets: JobFacets,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunFacets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nominalTime: Option<NominalTimeFacet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<Box<ParentRunFacet>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub errorMessage: Option<ErrorMessageFacet>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobFacets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<SqlFacet>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputDataset {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub facets: DatasetFacets,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputDataset {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub facets: DatasetFacets,
    #[serde(default)]
    pub outputFacets: OutputDatasetFacets,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DatasetFacets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaFacet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataSource: Option<DataSourceFacet>,
    // OL 2.0 places ColumnLineageDatasetFacet in DatasetFacets, not
    // OutputDatasetFacets. Marquez-style ingesters (including the
    // data-platform backend) only walk ``facets`` when applying
    // dataset-scoped facets, so emitting columnLineage from
    // outputFacets caused it to be silently dropped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columnLineage: Option<ColumnLineageFacet>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OutputDatasetFacets {
    // Reserved for facets that are genuinely write-only per the OL
    // spec (outputStatistics, etc.). columnLineage moved to
    // ``DatasetFacets`` above to match the spec and the consumer.
}

// Facet types. Full versions stay here; no partial fills.
#[derive(Debug, Clone, Serialize, Deserialize)] pub struct NominalTimeFacet { pub nominalStartTime: String }
#[derive(Debug, Clone, Serialize, Deserialize)] pub struct ParentRunFacet { pub run: Run, pub job: Job }
#[derive(Debug, Clone, Serialize, Deserialize)] pub struct ErrorMessageFacet { pub message: String, pub programmingLanguage: String }
#[derive(Debug, Clone, Serialize, Deserialize)] pub struct SqlFacet { pub query: String, pub dialect: String }
#[derive(Debug, Clone, Serialize, Deserialize)] pub struct SchemaFacet { pub fields: Vec<SchemaField> }
#[derive(Debug, Clone, Serialize, Deserialize)] pub struct SchemaField { pub name: String, #[serde(rename = "type")] pub field_type: String }
#[derive(Debug, Clone, Serialize, Deserialize)] pub struct DataSourceFacet { pub name: String, pub uri: String }

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnLineageFacet {
    pub fields: BTreeMap<String, ColumnLineageEntry>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnLineageEntry {
    pub inputFields: Vec<ColumnLineageInput>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnLineageInput {
    pub namespace: String,
    pub name: String,
    pub field: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transformations: Vec<Transformation>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transformation {
    #[serde(rename = "type")] pub kind: String,
    pub subtype: String,
    pub description: String,
    pub masking: bool,
}
