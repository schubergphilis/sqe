#!/usr/bin/env bash
set -euo pipefail

# Run comprehensive Trino compatibility tests
# This script requires:
# 1. SQE server running
# 2. Trino CLI installed
# 3. Trino server running

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Start SQE server in background
start_sqe() {
    echo "Starting SQE server..."
    
    # Create test config
    cat > "$ROOT_DIR/sqe-test.toml" << 'EOF'
[coordinator]
flight_sql_port = 60051
trino_http_port = 18080

[auth]
token_endpoint = "http://localhost:18181/api/catalog/v1/oauth/tokens"
client_id = "root"
client_secret = "s3cr3t"

[catalog]
polaris_url = "http://localhost:18181/api/catalog"
warehouse = "test_warehouse"

[storage]
s3_endpoint = "http://localhost:19000"
s3_access_key = "s3admin"
s3_secret_key = "s3admin"
s3_region = "us-east-1"
s3_path_style = true

[query]
max_concurrent_queries = 100
EOF

    # Start SQE server
    cargo run --bin sqe -- --config "$ROOT_DIR/sqe-test.toml" > /tmp/sqe.log 2>&1 &
    SQE_PID=$!
    
    # Wait for server to start
    for i in {1..30}; do
        if curl -s http://localhost:18080/v1/info > /dev/null 2>&1; then
            echo "SQE server started successfully"
            return 0
        fi
        echo "Waiting for SQE server... ($i/30)"
        sleep 2
    done
    
    echo "Failed to start SQE server"
    kill $SQE_PID 2>/dev/null || true
    exit 1
}

# Run CLI compatibility test
run_cli_test() {
    echo "Running CLI compatibility test..."
    
    # Make executable
    chmod +x "$ROOT_DIR/crates/sqe-trino-compat/tests/trino_cli_test.rs"
    
    # Run the test
    cargo test --package sqe-trino-compat --test trino_cli_test -- --ignored --nocapture
}

# Run shell script compatibility test
run_shell_test() {
    echo "Running shell compatibility test..."
    
    # Make executable
    chmod +x "$ROOT_DIR/scripts/verify_trino_compatibility.sh"
    
    # Run the test
    "$ROOT_DIR/scripts/verify_trino_compatibility.sh"
}

# Run integration test
run_integration_test() {
    echo "Running integration test..."
    
    # Make executable
    chmod +x "$ROOT_DIR/scripts/integration-test.sh"
    
    # Run the test
    "$ROOT_DIR/scripts/integration-test.sh"
}

# Cleanup
cleanup() {
    echo "Cleaning up..."
    kill $SQE_PID 2>/dev/null || true
    rm -f "$ROOT_DIR/sqe-test.toml"
    rm -f /tmp/sqe.log
}

# Main execution
main() {
    # Start SQE server
    start_sqe
    
    # Run tests
    run_cli_test
    run_shell_test
    run_integration_test
    
    # Cleanup
    cleanup
    
    echo "All Trino compatibility tests completed successfully!"
}

# Run main
main
