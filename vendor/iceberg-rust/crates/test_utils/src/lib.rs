// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

//! Minimal stub of apache/iceberg-rust `iceberg_test_utils`.
//!
//! SQE's vendored iceberg-rust fork doesn't ship the Docker Compose
//! harness layer. The real upstream crate provides `set_up()` logging
//! initialisation and container fixtures; the tests we run from this
//! workspace don't need them. The stub lets `cargo check` find the
//! crate name so the dev-dependency resolves.

/// No-op test setup. Matches the signature upstream exposes but does
/// nothing (no tracing subscriber, no docker bootstrap).
pub fn set_up() {}
