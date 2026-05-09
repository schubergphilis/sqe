use sqe_lineage::event::*;
use chrono::Utc;
use uuid::Uuid;

#[test]
fn run_event_serialises_with_required_fields() {
    let ev = RunEvent {
        eventType: EventType::Start,
        eventTime: Utc::now().to_rfc3339(),
        producer: "https://github.com/sbp/sqe/v0.1.0".to_string(),
        schemaURL: SCHEMA_URL.to_string(),
        run: Run::new(Uuid::new_v4()),
        job: Job { namespace: "sqe".into(), name: "query:abc".into(), facets: Default::default() },
        inputs: vec![],
        outputs: vec![],
    };
    let json = serde_json::to_value(&ev).unwrap();
    assert_eq!(json["eventType"], "START");
    assert_eq!(json["schemaURL"], SCHEMA_URL);
    assert!(json["run"]["runId"].is_string());
    assert_eq!(json["job"]["namespace"], "sqe");
}

fn fixed_uuid() -> uuid::Uuid {
    uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
}

fn sample_run() -> Run {
    Run {
        runId: fixed_uuid(),
        facets: RunFacets {
            nominalTime: Some(NominalTimeFacet {
                nominalStartTime: "2026-05-08T10:00:00Z".into(),
            }),
            parent: None,
            errorMessage: None,
        },
    }
}

fn sample_job(name: &str) -> Job {
    Job {
        namespace: "sqe".into(),
        name: name.into(),
        facets: JobFacets {
            sql: Some(SqlFacet {
                query: "SELECT 1".into(),
                dialect: "sqe".into(),
            }),
        },
    }
}

#[test]
fn snapshot_select_complete() {
    let ev = RunEvent {
        eventType: EventType::Complete,
        eventTime: "2026-05-08T10:00:01Z".into(),
        producer: "https://github.com/sbp/sqe/v0.1.0".into(),
        schemaURL: SCHEMA_URL.into(),
        run: sample_run(),
        job: sample_job("query:abc"),
        inputs: vec![InputDataset {
            namespace: "https://polaris.example/api/catalog".into(),
            name: "sales.orders".into(),
            facets: DatasetFacets {
                schema: Some(SchemaFacet {
                    fields: vec![SchemaField {
                        name: "id".into(),
                        field_type: "long".into(),
                    }],
                }),
                dataSource: Some(DataSourceFacet {
                    name: "polaris".into(),
                    uri: "https://polaris.example/api/catalog".into(),
                }),
            },
        }],
        outputs: vec![],
    };
    insta::assert_json_snapshot!(ev);
}

#[test]
fn snapshot_ctas_complete_with_column_lineage() {
    use std::collections::BTreeMap;

    let mut col_fields: BTreeMap<String, ColumnLineageEntry> = BTreeMap::new();
    col_fields.insert(
        "doubled".into(),
        ColumnLineageEntry {
            inputFields: vec![ColumnLineageInput {
                namespace: "https://polaris.example/api/catalog".into(),
                name: "sales.orders".into(),
                field: "amount".into(),
                transformations: vec![Transformation {
                    kind: "DIRECT".into(),
                    subtype: "TRANSFORMATION".into(),
                    description: "amount * 2".into(),
                    masking: false,
                }],
            }],
        },
    );

    let ev = RunEvent {
        eventType: EventType::Complete,
        eventTime: "2026-05-08T10:00:01Z".into(),
        producer: "https://github.com/sbp/sqe/v0.1.0".into(),
        schemaURL: SCHEMA_URL.into(),
        run: sample_run(),
        job: sample_job("ctas:def"),
        inputs: vec![InputDataset {
            namespace: "https://polaris.example/api/catalog".into(),
            name: "sales.orders".into(),
            facets: DatasetFacets::default(),
        }],
        outputs: vec![OutputDataset {
            namespace: "https://polaris.example/api/catalog".into(),
            name: "sales.archive".into(),
            facets: DatasetFacets::default(),
            outputFacets: OutputDatasetFacets {
                columnLineage: Some(ColumnLineageFacet { fields: col_fields }),
            },
        }],
    };
    insta::assert_json_snapshot!(ev);
}

#[test]
fn snapshot_query_fail() {
    let ev = RunEvent {
        eventType: EventType::Fail,
        eventTime: "2026-05-08T10:00:01Z".into(),
        producer: "https://github.com/sbp/sqe/v0.1.0".into(),
        schemaURL: SCHEMA_URL.into(),
        run: Run {
            runId: fixed_uuid(),
            facets: RunFacets {
                nominalTime: Some(NominalTimeFacet {
                    nominalStartTime: "2026-05-08T10:00:00Z".into(),
                }),
                parent: None,
                errorMessage: Some(ErrorMessageFacet {
                    message: "table not found: missing_table".into(),
                    programmingLanguage: "sql".into(),
                }),
            },
        },
        job: sample_job("query:err"),
        inputs: vec![],
        outputs: vec![],
    };
    insta::assert_json_snapshot!(ev);
}
