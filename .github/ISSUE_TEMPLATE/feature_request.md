---
name: Feature request
about: Suggest a capability or improvement for SQE
title: "[feat] "
labels: ["enhancement"]
---

## What you want

A short description of the capability. State it as the user-visible
behaviour, not the implementation. "I want SQE to support Avro file
reads" is better than "add an avro module."

## Why you want it

The use case. What are you trying to do that SQE does not let you do
today? A concrete workflow ("I run dbt against an Iceberg lake on
Glue and need ...") helps a lot.

## What you have considered

If you have looked at workarounds or alternatives, list them. Prior
art from other engines (DuckDB, Trino, Spark, ClickHouse) is welcome
and often informs the design.

## Scope

If you have an opinion on how big the change should be, say so:

- [ ] Small and contained (e.g. a new SQL function, a config option)
- [ ] Medium (a new TVF, a new optimizer rule, a Trino-compat shim)
- [ ] Large (new catalog backend, new write path, new wire protocol)

This helps us route the issue and decide whether it is a starter
contribution candidate or needs a design proposal in `openspec/`
first.

## Additional context

Links to specs, RFCs, upstream issues, or sample queries that
illustrate the feature.
