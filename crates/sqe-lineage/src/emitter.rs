//! Background task that drains the LineageMsg channel and emits OL RunEvents
//! to all configured sinks.
//!
//! See `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md` §3.4.

use crate::event::*;
use crate::extract::{self, CatalogLookup};
use crate::observer::*;
use crate::sink::MultiSink;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct EmitterConfig {
    pub job_namespace: String,
    pub producer: String,
    pub catalog_lookup: CatalogLookup,
}

/// Spawn the emitter background task.
///
/// The returned `JoinHandle` aborts when dropped, which mirrors the lifecycle
/// of the SpoolSink replay task. In `bin/sqe_server.rs` the handle is kept
/// alive for the duration of the process.
pub fn spawn_emitter(
    mut rx: mpsc::Receiver<LineageMsg>,
    sinks: Arc<MultiSink>,
    cfg: Arc<EmitterConfig>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let event = match msg {
                LineageMsg::Start(c) => build_start(&c, &cfg),
                LineageMsg::Complete(c) => build_complete(&c, &cfg),
                LineageMsg::Fail(c) => build_fail(&c, &cfg),
            };
            sinks.send(&event).await;
        }
    })
}

fn build_start(c: &QueryStartCtx, cfg: &EmitterConfig) -> RunEvent {
    RunEvent {
        eventType: EventType::Start,
        eventTime: c.started_at.to_rfc3339(),
        producer: cfg.producer.clone(),
        schemaURL: SCHEMA_URL.into(),
        run: build_run(c.run_id, c.started_at, &c.session_id, None),
        job: build_job(&cfg.job_namespace, &c.statement_kind, &c.sql),
        inputs: vec![],
        outputs: vec![],
    }
}

fn build_complete(c: &QueryCompleteCtx, cfg: &EmitterConfig) -> RunEvent {
    let (inputs, outputs) = match &c.plan {
        Some(PlanOrHint::Plan(p)) => extract::extract_lineage(p.as_ref(), &cfg.catalog_lookup),
        Some(PlanOrHint::Hint(h)) => extract::extract_from_hint(h, &cfg.catalog_lookup),
        None => (vec![], vec![]),
    };
    RunEvent {
        eventType: EventType::Complete,
        eventTime: c.ended_at.to_rfc3339(),
        producer: cfg.producer.clone(),
        schemaURL: SCHEMA_URL.into(),
        run: build_run(c.run_id, c.started_at, &c.session_id, None),
        job: build_job(&cfg.job_namespace, &c.statement_kind, &c.sql),
        inputs,
        outputs,
    }
}

fn build_fail(c: &QueryFailCtx, cfg: &EmitterConfig) -> RunEvent {
    let (inputs, outputs) = match &c.plan {
        Some(PlanOrHint::Plan(p)) => extract::extract_lineage(p.as_ref(), &cfg.catalog_lookup),
        Some(PlanOrHint::Hint(h)) => extract::extract_from_hint(h, &cfg.catalog_lookup),
        None => (vec![], vec![]),
    };
    RunEvent {
        eventType: EventType::Fail,
        eventTime: c.ended_at.to_rfc3339(),
        producer: cfg.producer.clone(),
        schemaURL: SCHEMA_URL.into(),
        run: build_run(
            c.run_id,
            c.started_at,
            &c.session_id,
            Some(&c.error_message),
        ),
        job: build_job(&cfg.job_namespace, &c.statement_kind, &c.sql),
        inputs,
        outputs,
    }
}

fn build_run(
    run_id: uuid::Uuid,
    started: chrono::DateTime<chrono::Utc>,
    session_id: &str,
    error: Option<&str>,
) -> Run {
    let parent = parse_session_uuid(session_id).map(|sess_uuid| {
        Box::new(ParentRunFacet {
            run: Run::new(sess_uuid),
            job: Job {
                namespace: "sqe".into(),
                name: format!("session:{session_id}"),
                facets: Default::default(),
            },
        })
    });

    Run {
        runId: run_id,
        facets: RunFacets {
            nominalTime: Some(NominalTimeFacet {
                nominalStartTime: started.to_rfc3339(),
            }),
            parent,
            errorMessage: error.map(|m| ErrorMessageFacet {
                message: m.to_string(),
                programmingLanguage: "sql".into(),
            }),
        },
    }
}

fn build_job(namespace: &str, kind: &str, sql: &str) -> Job {
    let hash = sqe_metrics::audit::query_hash(sql);
    Job {
        namespace: namespace.into(),
        name: format!("{kind}:{hash}"),
        facets: JobFacets {
            sql: Some(SqlFacet {
                // SQL-07: lineage sinks (JSONL/HTTP/Marquez) are a different
                // trust boundary than the SQL client, and `redact_pii` is
                // pattern-only (misses free-form literals like
                // `WHERE patient_id = 'P-998877'`). Strip ALL literals to
                // placeholders so query SHAPE is recorded but no literal value
                // reaches the sink; the exact text correlates via the job-name
                // hash above. `redact_pii` first keeps secret-keyword literals
                // out even from the placeholder pass.
                query: sqe_metrics::audit::strip_sql_literals(&sqe_metrics::audit::redact_pii(sql)),
                dialect: "sqe".into(),
            }),
        },
    }
}

/// Parse a session_id as a UUID for the parent.run.runId field. If the session
/// id is not UUID-shaped (e.g. a flight-sql token), fall back to None. OL spec
/// allows omitting the parent facet.
fn parse_session_uuid(session_id: &str) -> Option<uuid::Uuid> {
    uuid::Uuid::parse_str(session_id).ok()
}
