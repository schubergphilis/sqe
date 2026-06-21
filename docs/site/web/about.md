---
title: About SQE - Sovereign Query Engine
description: SQE started as a fork of Trino with end-to-end authentication instead of service accounts. Built at Schuberg Philis by Jacob Verhoeks & Rafael Herrero, it's the open-source engine at the heart of a sovereign data platform.
---

# Make the query the identity.

SQE started as a fork of **Trino** with one stubborn idea: end-to-end authentication instead of shared service accounts. Every query should run as the person who issued it, not as `trino-coordinator`, not as a service principal.

## Origin: From a Trino fork to a Rust engine

The audit question, "who read the customer table last Tuesday?", was unanswerable when every query ran under the same service account. So we forked Trino to pass the user's identity all the way through to the catalog and storage. That single constraint reshaped everything, and SQE grew into a purpose-built engine in **Rust**, on **DataFusion** and **Apache Iceberg**, one binary that runs embedded on a laptop or distributed across a cluster, with per-query OIDC pass-through and policy enforced at the plan layer.

## Where it's going: A sovereign data platform

SQE is the **open-source** query engine (Apache-2.0, on [GitHub](https://github.com/schubergphilis/sqe)) at the heart of a larger **sovereign data platform** we're building, data infrastructure you actually own, with no shared root and no vendor lock-in. The wider platform isn't open source *yet*.

Built at **Schuberg Philis**

by Jacob Verhoeks & Rafael Herrero
