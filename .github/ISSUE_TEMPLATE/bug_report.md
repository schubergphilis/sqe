---
name: Bug report
about: Something is wrong in SQE that you would like fixed
title: "[bug] "
labels: ["bug"]
---

## What happened

A short description of the bug. Be specific about what you ran and
what went wrong.

## What you expected to happen

What output or behaviour you were expecting.

## How to reproduce

Step-by-step instructions, ideally with the exact SQL and config that
triggered the bug. A `cargo run` command or a Flight SQL trace is
ideal.

```sql
-- paste the failing query here
```

## Environment

- **SQE version**: (output of `cargo run --bin sqe-coordinator -- --version`)
- **Rust version**: (output of `rustc --version`)
- **OS / architecture**: (output of `uname -a`)
- **Catalog backend**: (Polaris / Nessie / HMS / Glue / S3 Tables / JDBC / Hadoop)
- **Storage**: (S3 / S3-compatible / local filesystem; provider if relevant)
- **Auth provider**: (OIDC / bearer / API key / mTLS / anonymous / other)

## Logs

If the coordinator or worker logged anything useful, paste the
relevant lines here. Wrap them in a fenced block. Redact bearer
tokens, account IDs, and other secrets.

```
<paste logs here>
```

## Additional context

Anything else that might help: catalog version, table format
version (V2 / V3), partition spec, table properties, dataset size,
whether it reproduces on a fresh table, whether it reproduces with
the docker-compose test stack.
