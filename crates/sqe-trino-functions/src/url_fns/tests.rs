use arrow::datatypes::DataType;
use datafusion::arrow::util::display::array_value_to_string;

async fn run(sql: &str) -> (DataType, Vec<String>) {
    let ctx = crate::duckdb_test_ctx();
    crate::register_trino_functions(&ctx);
    crate::register_extended_trino_functions(&ctx);
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let dt = batches[0].column(0).data_type().clone();
    let mut rows = Vec::new();
    for b in &batches {
        let col = b.column(0);
        for i in 0..col.len() {
            rows.push(if col.is_null(i) {
                "NULL".to_string()
            } else {
                array_value_to_string(col, i).unwrap()
            });
        }
    }
    (dt, rows)
}

async fn one(sql: &str) -> String {
    run(sql).await.1.remove(0)
}

#[tokio::test]
async fn components_of_an_absolute_url() {
    let u = "https://User@Example.com:8443/a%20b/c?x=1&y=two%20words#frag%21";
    let (dt, rows) = run(&format!("SELECT url_extract_protocol('{u}')")).await;
    assert_eq!(dt, DataType::Utf8);
    assert_eq!(rows, ["https"]);
    // java.net.URI keeps the host's case.
    assert_eq!(
        one(&format!("SELECT url_extract_host('{u}')")).await,
        "Example.com"
    );
    let (dt, rows) = run(&format!("SELECT url_extract_port('{u}')")).await;
    assert_eq!(dt, DataType::Int64);
    assert_eq!(rows, ["8443"]);
    assert_eq!(
        one(&format!("SELECT url_extract_path('{u}')")).await,
        "/a b/c"
    );
    assert_eq!(
        one(&format!("SELECT url_extract_query('{u}')")).await,
        "x=1&y=two words"
    );
    let (dt, rows) = run(&format!("SELECT url_extract_fragment('{u}')")).await;
    assert_eq!(dt, DataType::Utf8);
    assert_eq!(rows, ["frag!"]);
}

#[tokio::test]
async fn absent_components_are_empty_and_port_is_null() {
    let u = "https://example.com";
    assert_eq!(one(&format!("SELECT url_extract_path('{u}')")).await, "");
    assert_eq!(one(&format!("SELECT url_extract_query('{u}')")).await, "");
    assert_eq!(
        one(&format!("SELECT url_extract_fragment('{u}')")).await,
        ""
    );
    assert_eq!(
        one(&format!("SELECT url_extract_port('{u}')")).await,
        "NULL"
    );
    assert_eq!(
        one(&format!("SELECT url_extract_host('{u}')")).await,
        "example.com"
    );
    let u = "http://h:80/";
    assert_eq!(one(&format!("SELECT url_extract_port('{u}')")).await, "80");
    assert_eq!(one(&format!("SELECT url_extract_path('{u}')")).await, "/");
}

#[tokio::test]
async fn relative_urls_keep_path_and_query() {
    assert_eq!(one("SELECT url_extract_path('/a/b?x=1')").await, "/a/b");
    assert_eq!(one("SELECT url_extract_query('/a/b?x=1')").await, "x=1");
    assert_eq!(one("SELECT url_extract_host('/a/b?x=1')").await, "");
    assert_eq!(one("SELECT url_extract_protocol('/a/b?x=1')").await, "");
    assert_eq!(one("SELECT url_extract_port('/a/b?x=1')").await, "NULL");
    assert_eq!(
        one("SELECT url_extract_parameter('/a/b?x=1', 'x')").await,
        "1"
    );
}

#[tokio::test]
async fn unparseable_urls_are_null() {
    for u in [
        "http://exa mple.com/",
        "http://a/b|c",
        "ht!tp://a",
        "http://a/%zz",
        ":foo",
    ] {
        for f in [
            "url_extract_host",
            "url_extract_path",
            "url_extract_port",
            "url_extract_protocol",
            "url_extract_query",
            "url_extract_fragment",
        ] {
            assert_eq!(one(&format!("SELECT {f}('{u}')")).await, "NULL", "{f}({u})");
        }
        assert_eq!(
            one(&format!("SELECT url_extract_parameter('{u}', 'x')")).await,
            "NULL"
        );
    }
}

#[tokio::test]
async fn ipv6_and_opaque_uris() {
    assert_eq!(
        one("SELECT url_extract_host('http://[::1]:8080/x')").await,
        "[::1]"
    );
    assert_eq!(
        one("SELECT url_extract_port('http://[::1]:8080/x')").await,
        "8080"
    );
    assert_eq!(
        one("SELECT url_extract_protocol('mailto:a@b.com')").await,
        "mailto"
    );
    assert_eq!(one("SELECT url_extract_path('mailto:a@b.com')").await, "");
}

#[tokio::test]
async fn parameter_is_the_first_value() {
    assert_eq!(
        one("SELECT url_extract_parameter('http://a/?x=1&x=2&y=3', 'x')").await,
        "1"
    );
    assert_eq!(
        one("SELECT url_extract_parameter('http://a/?x=1&y=a+b%21', 'y')").await,
        "a b!"
    );
    assert_eq!(
        one("SELECT url_extract_parameter('http://a/?x=1', 'z')").await,
        "NULL"
    );
    assert_eq!(
        one("SELECT url_extract_parameter('http://a/', 'z')").await,
        "NULL"
    );
    // Present without a value.
    assert_eq!(
        one("SELECT url_extract_parameter('http://a/?flag&x=1', 'flag')").await,
        ""
    );
    assert_eq!(
        one("SELECT url_extract_parameter('http://a/?a=b=c', 'a')").await,
        "b=c"
    );
    let (dt, _) = run("SELECT url_extract_parameter('http://a/?x=1', 'x')").await;
    assert_eq!(dt, DataType::Utf8);
}

#[tokio::test]
async fn column_input_with_nulls() {
    let (dt, rows) = run(
        "SELECT url_extract_port(u) FROM (VALUES ('http://a:81/'), ('http://b/'), (NULL), ('bad url')) t(u)",
    )
    .await;
    assert_eq!(dt, DataType::Int64);
    assert_eq!(rows, ["81", "NULL", "NULL", "NULL"]);
    let (_, rows) = run(
        "SELECT url_extract_host(u) FROM (VALUES ('http://a:81/'), ('http://b/'), (NULL)) t(u)",
    )
    .await;
    assert_eq!(rows, ["a", "b", "NULL"]);
    let (_, rows) = run(
        "SELECT url_extract_parameter(u, 'k') FROM (VALUES ('http://a/?k=1'), ('http://a/'), (NULL)) t(u)",
    )
    .await;
    assert_eq!(rows, ["1", "NULL", "NULL"]);
}
