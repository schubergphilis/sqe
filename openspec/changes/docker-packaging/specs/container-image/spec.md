## ADDED Requirements

### Requirement: Single image with both binaries
The Docker image SHALL contain both `sqe-server` and `sqe-cli` binaries in `/usr/local/bin/`.

#### Scenario: Image contains sqe-server
- **WHEN** the image is inspected
- **THEN** `/usr/local/bin/sqe-server` SHALL exist and be executable

#### Scenario: Image contains sqe-cli
- **WHEN** the image is inspected
- **THEN** `/usr/local/bin/sqe-cli` SHALL exist and be executable

### Requirement: Entrypoint is sqe-server
The Docker image entrypoint SHALL be `sqe-server`. CMD SHALL be empty so that all arguments are passed through.

#### Scenario: Default container start
- **WHEN** the container is started with `docker run sqe SQE_MODE=coordinator`
- **THEN** `sqe-server` runs as the main process

#### Scenario: CLI via exec
- **WHEN** `docker exec -it <container> sqe-cli` is run against a running container
- **THEN** the `sqe-cli` REPL starts and can connect to the local sqe-server

### Requirement: Multi-stage build
The Dockerfile SHALL use a multi-stage build with a Rust build stage and a minimal runtime stage.

#### Scenario: Build stage compiles binaries
- **WHEN** `docker build` is run
- **THEN** both `sqe-server` and `sqe-cli` are compiled in the build stage using `cargo build --release`

#### Scenario: Runtime stage is minimal
- **WHEN** the final image is built
- **THEN** it SHALL be based on `debian:bookworm-slim` and SHALL NOT contain the Rust toolchain, source code, or build artifacts

### Requirement: Non-root execution
The container SHALL run as a non-root user by default.

#### Scenario: Default user
- **WHEN** the container starts without an explicit `--user` flag
- **THEN** the process SHALL run as a non-root user (e.g., `sqe` with UID 1000)

### Requirement: Configurable via environment variables
The container SHALL support all configuration via environment variables for Kubernetes compatibility.

#### Scenario: K8s coordinator deployment
- **WHEN** the container is started with env `SQE_MODE=coordinator`, `SQE_LISTEN_ADDR=0.0.0.0:8080`
- **THEN** `sqe-server` starts as coordinator listening on all interfaces at port 8080

#### Scenario: K8s worker deployment
- **WHEN** the container is started with env `SQE_MODE=worker`, `SQE_COORDINATOR_ADDR=sqe-coordinator:8080`
- **THEN** `sqe-server` starts as worker and registers with the coordinator at the given address

### Requirement: Image metadata
The Docker image SHALL include OCI labels for version, build date, source commit, and description.

#### Scenario: Labels present
- **WHEN** the image is inspected with `docker inspect`
- **THEN** labels `org.opencontainers.image.version`, `org.opencontainers.image.created`, `org.opencontainers.image.revision`, and `org.opencontainers.image.description` SHALL be present
