### Changed

- Dependency refresh: DataFusion 55.2, arrow 59.3.1, AWS SDK and other patch releases

    DataFusion 55.1 enabled serde_json's `preserve_order` feature for the whole build and 55.2 no longer does. The workspace now enables it itself, so JSON objects keep their insertion order as before.
