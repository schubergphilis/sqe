//! A minimal Iceberg table reader, written against the spec rather than
//! against `iceberg-rust`.
//!
//! `iceberg-rust` cannot compile for `wasm32-unknown-unknown`: its FileIO goes
//! through opendal, whose `Access` trait uses boxed futures declared without a
//! `Send` bound. So the read path is re-implemented here over the two formats
//! Iceberg actually uses on disk: `metadata.json` is JSON, and the manifest
//! list and manifests are Avro. Both are reachable from a browser.
//!
//! Scope is deliberately narrow: copy-on-write tables with data manifests
//! only. A table carrying delete files is refused rather than read, because
//! quietly ignoring position deletes returns rows the table says are gone,
//! which looks like success.

use apache_avro::types::Value as AvroValue;
use apache_avro::Reader as AvroReader;
use datafusion::prelude::SessionContext;
use object_store::ObjectStoreExt;
use url::Url;

/// What a snapshot resolves to: the parquet files to scan, plus the schema the
/// catalog says the table has.
pub struct IcebergTable {
    pub data_files: Vec<String>,
    pub fields: Vec<(String, String)>,
    pub snapshot_id: i64,
    pub location: String,
}

type R<T> = Result<T, String>;

/// Read `url` through whichever object store the session has registered for
/// its scheme and authority, so http, https and s3 all work unchanged.
async fn fetch(ctx: &SessionContext, url: &Url) -> R<Vec<u8>> {
    let store = ctx
        .runtime_env()
        .object_store(
            datafusion::datasource::object_store::ObjectStoreUrl::parse(format!(
                "{}://{}",
                url.scheme(),
                url.authority()
            ))
            .map_err(|e| format!("object store url: {e}"))?,
        )
        .map_err(|e| format!("no object store registered for {url}: {e}"))?;

    let path = object_store::path::Path::from_url_path(url.path())
        .map_err(|e| format!("path {}: {e}", url.path()))?;

    let got = store
        .get(&path)
        .await
        .map_err(|e| format!("GET {url}: {e}"))?;
    got.bytes()
        .await
        .map(|b| b.to_vec())
        .map_err(|e| format!("body {url}: {e}"))
}

/// Unwrap the Avro union wrapper that every nullable Iceberg field carries.
fn unwrap_union(v: &AvroValue) -> &AvroValue {
    match v {
        AvroValue::Union(_, inner) => unwrap_union(inner),
        other => other,
    }
}

fn field<'a>(rec: &'a AvroValue, name: &str) -> Option<&'a AvroValue> {
    match rec {
        AvroValue::Record(fields) => fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| unwrap_union(v)),
        _ => None,
    }
}

fn as_str(v: Option<&AvroValue>) -> Option<String> {
    match v? {
        AvroValue::String(s) => Some(s.clone()),
        AvroValue::Bytes(b) => String::from_utf8(b.clone()).ok(),
        _ => None,
    }
}

fn as_i64(v: Option<&AvroValue>) -> Option<i64> {
    match v? {
        AvroValue::Int(i) => Some(*i as i64),
        AvroValue::Long(l) => Some(*l),
        _ => None,
    }
}

fn avro_records(bytes: &[u8], what: &str) -> R<Vec<AvroValue>> {
    let reader = AvroReader::new(bytes).map_err(|e| format!("{what}: not avro: {e}"))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("{what}: decode: {e}"))
}

/// Rewrite an absolute path recorded in the metadata onto the base URL the
/// browser can actually reach.
///
/// Iceberg records fully-qualified locations, and they are almost never the
/// URL you fetched from: a table written locally records `file:///...`, and an
/// S3 table records `s3://...` even when the page reaches it over https. So
/// every path is rebased by stripping the table's own `location` prefix and
/// re-rooting it on the base.
fn rebase(path: &str, location: &str, base: &str) -> String {
    let loc = location.trim_end_matches('/');
    let base = base.trim_end_matches('/');
    // Compare on the path portion so file:/// vs file:// spelling differences
    // and scheme changes do not defeat the prefix match.
    let strip = |s: &str| -> String {
        match s.find("://") {
            Some(i) => s[i + 3..].to_string(),
            None => s.to_string(),
        }
    };
    let (sp, sl) = (strip(path), strip(loc));
    // A local `file:///a/b` strips to `/a/b`; an `s3://bucket/a` to `bucket/a`.
    match sp.strip_prefix(&sl) {
        Some(rest) => format!("{base}/{}", rest.trim_start_matches('/')),
        // Not under the table location. Leave it: it may already be reachable.
        None => path.to_string(),
    }
}

/// Resolve `metadata_url` to the data files of its current snapshot.
pub async fn load(ctx: &SessionContext, metadata_url: &Url) -> R<IcebergTable> {
    let raw = fetch(ctx, metadata_url).await?;
    let md: serde_json::Value =
        serde_json::from_slice(&raw).map_err(|e| format!("metadata.json: {e}"))?;

    let location = md["location"]
        .as_str()
        .ok_or("metadata.json has no `location`")?
        .to_string();

    // The table base is the metadata URL with `/metadata/<file>` removed, which
    // is where every rebased path is re-rooted.
    let base = {
        let s = metadata_url.as_str();
        let cut = s
            .rfind("/metadata/")
            .ok_or("metadata URL is not under a /metadata/ directory")?;
        s[..cut].to_string()
    };

    let fields = md["schemas"]
        .as_array()
        .and_then(|schemas| {
            let want = md["current-schema-id"].as_i64().unwrap_or(0);
            schemas
                .iter()
                .find(|s| s["schema-id"].as_i64() == Some(want))
                .or_else(|| schemas.first())
        })
        .or(Some(&md["schema"]))
        .and_then(|s| s["fields"].as_array())
        .map(|fs| {
            fs.iter()
                .map(|f| {
                    (
                        f["name"].as_str().unwrap_or("?").to_string(),
                        // A nested type is an object; render it as its `type`.
                        match &f["type"] {
                            serde_json::Value::String(t) => t.clone(),
                            other => other["type"].as_str().unwrap_or("struct").to_string(),
                        },
                    )
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    // Everything hangs off the CURRENT snapshot. Older manifest lists are still
    // on disk and reading the wrong one silently returns a previous version of
    // the table.
    let snapshot_id = md["current-snapshot-id"]
        .as_i64()
        .filter(|id| *id != -1)
        .ok_or("table has no current snapshot (it is empty)")?;

    let snapshot = md["snapshots"]
        .as_array()
        .and_then(|s| {
            s.iter()
                .find(|s| s["snapshot-id"].as_i64() == Some(snapshot_id))
        })
        .ok_or_else(|| format!("snapshot {snapshot_id} is not in `snapshots`"))?;

    let manifest_list = snapshot["manifest-list"]
        .as_str()
        .ok_or("snapshot has no `manifest-list`")?;
    let list_url = rebase(manifest_list, &location, &base);

    let list_bytes = fetch(
        ctx,
        &Url::parse(&list_url).map_err(|e| format!("manifest list url {list_url}: {e}"))?,
    )
    .await?;

    let mut manifests = Vec::new();
    for entry in avro_records(&list_bytes, "manifest list")? {
        // content: 0 = data, 1 = deletes. Absent means v1, which is data only.
        let content = as_i64(field(&entry, "content")).unwrap_or(0);
        if content != 0 {
            return Err(
                "this table has delete manifests (merge-on-read). The playground reads \
                 copy-on-write tables only; reading it while ignoring deletes would \
                 return rows the table says are deleted."
                    .to_string(),
            );
        }
        if let Some(p) = as_str(field(&entry, "manifest_path")) {
            manifests.push(rebase(&p, &location, &base));
        }
    }

    let mut data_files = Vec::new();
    for m in &manifests {
        let bytes = fetch(
            ctx,
            &Url::parse(m).map_err(|e| format!("manifest url {m}: {e}"))?,
        )
        .await?;
        for entry in avro_records(&bytes, "manifest")? {
            // status: 0 EXISTING, 1 ADDED, 2 DELETED.
            if as_i64(field(&entry, "status")) == Some(2) {
                continue;
            }
            let Some(df) = field(&entry, "data_file") else {
                continue;
            };
            // data_file.content: 0 = data, 1 = position deletes, 2 = equality.
            if as_i64(field(df, "content")).unwrap_or(0) != 0 {
                return Err(
                    "this table carries delete files (merge-on-read), which the \
                     playground does not apply. Refusing rather than returning \
                     deleted rows."
                        .to_string(),
                );
            }
            if let Some(p) = as_str(field(df, "file_path")) {
                data_files.push(rebase(&p, &location, &base));
            }
        }
    }

    if data_files.is_empty() {
        return Err(format!(
            "snapshot {snapshot_id} resolved to no data files"
        ));
    }

    Ok(IcebergTable {
        data_files,
        fields,
        snapshot_id,
        location,
    })
}
