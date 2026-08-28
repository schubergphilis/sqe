---
slug: mcp
title: "MCP with Codex and Claude"
description: "Run SQE MCP with Keycloak user identity, Polaris source authorization, protocol tests, and Codex or Claude Code client setup."
---

# MCP with Codex and Claude

This runnable quickstart layers SQE's Streamable HTTP MCP endpoint over the
existing Polaris + Keycloak user-token stack. It includes raw protocol tests,
read-only defaults, an optional three-gate write test, and reusable Codex and
Claude Code configuration.

```bash
cd quickstart/mcp
cp .env.example .env
./test-static.sh
./run.sh --check
```

For the complete configuration, scripts, client templates and troubleshooting,
see [`quickstart/mcp/`](https://github.com/schubergphilis/sqe/tree/main/quickstart/mcp/).
