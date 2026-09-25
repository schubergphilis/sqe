use sqe_lineage::event::*;
use sqe_lineage::sinks::http::{AuthMode, HttpConfig, HttpSink};
use sqe_lineage::*;
use wiremock::{matchers, Mock, MockServer, ResponseTemplate};

fn dummy_event() -> RunEvent {
    RunEvent {
        eventType: EventType::Start,
        eventTime: "2026-05-08T10:00:00Z".into(),
        producer: "test".into(),
        schemaURL: SCHEMA_URL.into(),
        run: Run::new(uuid::Uuid::nil()),
        job: Job {
            namespace: "sqe".into(),
            name: "query:test".into(),
            facets: Default::default(),
        },
        inputs: vec![],
        outputs: vec![],
    }
}

#[tokio::test]
async fn http_sink_posts_with_no_auth() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/api/v1/lineage"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let sink = HttpSink::new(HttpConfig {
        endpoint: format!("{}/api/v1/lineage", server.uri()),
        auth: AuthMode::None,
        timeout_ms: 5000,
        retry_attempts: 1,
    })
    .unwrap();

    sink.send(&dummy_event()).await.unwrap();
}

#[tokio::test]
async fn http_sink_posts_with_static_bearer() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/api/v1/lineage"))
        .and(matchers::header("Authorization", "Bearer secret"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let sink = HttpSink::new(HttpConfig {
        endpoint: format!("{}/api/v1/lineage", server.uri()),
        auth: AuthMode::Bearer("secret".into()),
        timeout_ms: 5000,
        retry_attempts: 1,
    })
    .unwrap();

    sink.send(&dummy_event()).await.unwrap();
}

#[tokio::test]
async fn http_sink_posts_with_user_token() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/api/v1/lineage"))
        .and(matchers::header("Authorization", "Bearer user-jwt"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let sink = HttpSink::new(HttpConfig {
        endpoint: format!("{}/api/v1/lineage", server.uri()),
        auth: AuthMode::UserToken("user-jwt".into()),
        timeout_ms: 5000,
        retry_attempts: 1,
    })
    .unwrap();

    sink.send(&dummy_event()).await.unwrap();
}

#[tokio::test]
async fn http_sink_retries_once_on_503_then_succeeds() {
    let server = MockServer::start().await;
    // First call returns 503, then 200
    Mock::given(matchers::method("POST"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(matchers::method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let sink = HttpSink::new(HttpConfig {
        endpoint: format!("{}/api/v1/lineage", server.uri()),
        auth: AuthMode::None,
        timeout_ms: 5000,
        retry_attempts: 1,
    })
    .unwrap();

    sink.send(&dummy_event()).await.unwrap();
}

#[tokio::test]
async fn http_sink_returns_error_after_exhausting_retries() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let sink = HttpSink::new(HttpConfig {
        endpoint: format!("{}/api/v1/lineage", server.uri()),
        auth: AuthMode::None,
        timeout_ms: 5000,
        retry_attempts: 0, // no retries; first 503 fails immediately
    })
    .unwrap();

    let err = sink.send(&dummy_event()).await.unwrap_err();
    assert!(matches!(err, SinkError::Http(_)));
}
