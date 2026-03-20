## ADDED Requirements

### Requirement: IcebergRestBackend — generalised REST catalog
The system SHALL connect to any Iceberg REST-compliant catalog (Polaris, Snowflake Open Catalog, Unity Catalog REST endpoint).

#### Scenario: List tables via REST catalog
- **GIVEN** an Iceberg REST catalog at the configured URL
- **WHEN** `SHOW TABLES` is executed
- **THEN** tables from the catalog's namespaces are returned

#### Scenario: Passthrough catalog auth
- **GIVEN** `catalog.auth.type = "passthrough"` and a user with a valid OIDC token
- **WHEN** a catalog call is made
- **THEN** the user's bearer token is forwarded to the catalog
- **AND** the catalog enforces its own ACLs against the user

### Requirement: AwsGlueBackend
The system SHALL discover and read Iceberg tables registered in AWS Glue.

#### Scenario: List Glue tables
- **GIVEN** `catalog.type = "aws_glue"` with valid IAM credentials
- **WHEN** `SHOW TABLES` is executed
- **THEN** Iceberg tables registered in Glue are listed

#### Scenario: IAM from instance profile
- **GIVEN** `catalog.auth.type = "aws_iam"` with no explicit keys
- **WHEN** running on an EC2 instance / EKS pod with an IAM role attached
- **THEN** credentials are obtained from the instance metadata service
- **AND** catalog calls are signed with SigV4

### Requirement: NessieBackend
The system SHALL discover and read Iceberg tables managed by Project Nessie.

#### Scenario: List tables on a Nessie branch
- **GIVEN** `catalog.type = "nessie"` with `ref = "main"`
- **WHEN** `SHOW TABLES` is executed
- **THEN** Iceberg tables on the `main` branch are listed

### Requirement: StorageOnlyBackend — auto-discovery
The system SHALL discover Iceberg tables by scanning a base path without a catalog server.

#### Scenario: Auto-discover tables
- **GIVEN** `catalog.type = "storage_only"` with `base_path = "s3://my-lake/"`
- **AND** the bucket contains `s3://my-lake/sales/orders/metadata/v1.metadata.json`
- **WHEN** `SHOW TABLES` is executed
- **THEN** a table `sales.orders` is listed

#### Scenario: Namespace derived from directory structure
- **GIVEN** `base_path = "s3://my-lake/"` and a table at `s3://my-lake/analytics/sessions/metadata/`
- **WHEN** tables are discovered
- **THEN** the table is listed as namespace `analytics`, table `sessions`

#### Scenario: Explicit table registration
- **GIVEN** a `[[catalog.tables]]` entry `name = "raw.events"`, `path = "s3://other-bucket/events/"`
- **WHEN** `SELECT * FROM raw.events` is executed
- **THEN** the table is found and read

#### Scenario: iceberg_scan TVF
- **GIVEN** no catalog configured
- **WHEN** `SELECT * FROM iceberg_scan('s3://my-bucket/path/to/table/')` is executed
- **THEN** the table at that path is read and results returned

### Requirement: Delta Lake support (feature flag)
The system SHALL read Delta Lake tables when the `delta` feature flag is enabled.

#### Scenario: Read Delta table via Unity Catalog
- **GIVEN** `features.delta = true` and a Unity Catalog REST catalog
- **WHEN** a table is identified as Delta format
- **THEN** it is opened via delta-rs DeltaTableProvider
- **AND** `SELECT` returns correct results

#### Scenario: Delta tables are read-only
- **GIVEN** a Delta table
- **WHEN** `INSERT INTO` or `CREATE OR REPLACE TABLE` targeting a Delta table is attempted
- **THEN** an error is returned: `"Delta write path not yet supported"`

### Requirement: Multi-cloud storage backends
The system SHALL read Iceberg data files from S3-compatible, Azure, GCS, and local storage.

#### Scenario: S3-compatible storage with endpoint override
- **GIVEN** `storage.type = "s3"` with `endpoint = "https://<account>.r2.cloudflarestorage.com"`
- **WHEN** a query reads Iceberg parquet files
- **THEN** files are fetched from Cloudflare R2 successfully

#### Scenario: Azure ADLS Gen2 storage
- **GIVEN** `storage.type = "azure"` with a valid storage account + access key
- **WHEN** a query reads Iceberg parquet files
- **THEN** files are fetched from Azure successfully

#### Scenario: GCS storage
- **GIVEN** `storage.type = "gcs"` with a service account key file
- **WHEN** a query reads Iceberg parquet files
- **THEN** files are fetched from GCS successfully
