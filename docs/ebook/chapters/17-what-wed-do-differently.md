# What We'd Do Differently {#sec:retrospective}

> The honest chapter. The one most technical books skip.

This is the chapter where I stop saying "we" and start saying "I" more than usual. The architectural decisions were collaborative. The reflections are mine. And the hardest thing to write in a technical book isn't the code walkthrough or the failure post-mortem. It's the part where you sit with what you built and ask: was this the right thing to do?

I don't mean "does it work." It works. Sixteen chapters demonstrate that. I mean the deeper question: given everything we know now, having built a distributed SQL engine from scratch in fifteen days with an AI coding agent, would we do it again? And if so, what would we change?

The answer to the first question is yes. The answer to the second fills this chapter.


## Why Build a SQL Engine at All?

People asked this. A lot.

There's a version of this answer in the Preface, but this is the retrospective, and the retrospective gets the honest version. Not the polished version you give in a conference talk. The one you give at midnight after the load test finally passes.

**Reason one: because we can.** This is the engineer's answer, and I won't apologise for it. I've been taking things apart since childhood. Not to break them -- to understand them. A team that can build a query engine from scratch has a depth of understanding that a team running a managed service never develops. You learn how query planning actually works. You learn where the bottlenecks really are. You learn that the thing you blamed on "Trino being slow" was actually your Parquet file layout. Building gives you X-ray vision into every query engine you'll ever operate again.

**Reason two: to challenge ourselves.** Not in the "growth mindset" motivational poster sense. In the practical sense: could we build the right tool for our specific problem? Not the most general tool. Not the most feature-complete tool. The one that matches our security model, our catalog, our operational constraints. Building SQE forced us to articulate what we actually needed, which turned out to be far less than what Trino provides and far more than what any managed service offers.

**Reason three: because the tools don't match.** Trino's auth model is fundamentally incompatible with zero-trust -- every query runs as a service account, and no amount of plugin configuration changes that. Spark is a cluster framework cosplaying as a query engine. DuckDB is brilliant but single-node. DataFusion is a library, not a product. The gap between "library" and "product" is exactly the gap this book fills.

The honest addendum to reason three: we didn't fully understand that gap when we started. We thought it was smaller than it is. Bridging "DataFusion SessionContext" to "production query engine with distributed execution, auth, observability, and a benchmark suite" is a lot of bridge. The fifteen-day timeline makes it sound easy. It wasn't easy. It was fast. Those are different things.


## 316 Commits (Across All Branches) in 15 Days

The git log tells the real story:

```
Mar 14  -- Initial commit: architecture docs + core engine spec
Mar 14  -- All 6 crates scaffolded: core, auth, policy, sql, catalog, coordinator
Mar 14  -- First Flight SQL integration test passes
Mar 15  -- Write path: CTAS, INSERT INTO, DROP TABLE tested
Mar 15  -- Distributed execution: ScanTask protocol, worker, DistributedScanExec
Mar 16  -- Prometheus metrics, audit logging, Trino HTTP compat
Mar 17  -- Docker, Helm chart, mdBook docs, CLI
Mar 18  -- Lightweight test stack: Polaris in-memory + RustFS
Mar 19  -- 37 integration tests: views, joins, aggregations, EXPLAIN
Mar 21  -- Benchmark suite: TPC-H, SSB, TPC-DS, ClickBench queries
Mar 22  -- TPC-E, TPC-BB, TPC-C generators + benchmark runner
Mar 24  -- Weighted fragment scheduler, OTel trace propagation, heartbeats
Mar 25  -- Query history, result cache, system.runtime.* virtual tables
Mar 27  -- Distributed docker-compose: coordinator + 2 workers
Mar 28  -- Distributed execution wired into query pipeline
Mar 29  -- Concurrent client load test, schema projection fix
```

That's a functioning distributed SQL engine with auth, observability, six benchmark suites, and a load testing harness -- in fifteen days. The pace is real. But the pace needs context, because the pace is the thing people focus on, and it's the wrong thing to focus on.

The pace was possible because of three factors, in order of importance:

1. **Spec-driven design.** Every feature started as a written specification before a line of Rust was generated. Architecture decisions, trait definitions, data flows, failure modes -- all documented in OpenSpec format before implementation began. The AI never started from a blank page. It started from a design that a human had already thought through.

2. **AI-assisted implementation.** Claude Code wrote the vast majority of the Rust code. It scaffolded crates. It implemented traits. It wrote integration tests. It debugged gRPC stream issues by generating tracing instrumentation and reading DataFusion internals. The implementation speed was extraordinary.

3. **Rust and DataFusion.** The language and the library did enormous heavy lifting. Rust's type system catches entire categories of bugs at compile time. DataFusion provides a complete query engine in a single `SessionContext`. We weren't building a query engine from zero. We were building the product layer on top of one.

Factor three is the one that gets underestimated. DataFusion is not scaffolding. It's a query engine. We built an engine *around* an engine -- adding auth, catalog integration, distributed execution, observability, and a wire protocol. That's still a lot. But it's not "building a query engine from scratch" in the way that DataFusion itself was built from scratch.


## The AI-Assisted Build: Honest Assessment

I want to be specific here, because the discourse around AI-assisted development oscillates between "AI writes all the code now" and "AI is just autocomplete." Neither is accurate. The truth is more interesting and more nuanced.

### What the AI did well

**Rust implementation.** Given a trait definition and a description of the desired behaviour, the AI could produce correct, idiomatic Rust implementations consistently. The `PolicyEnforcer` trait in `sqe-policy` is twenty-six lines of code. The AI produced it in one pass from a spec paragraph. The `FlightSqlService` implementation in `sqe-coordinator` is hundreds of lines with async streams, error handling, and token extraction -- and the AI got the borrow checker happy on the first try about 70% of the time.

**Cross-crate refactoring.** When we renamed from vendor-specific identifiers to generic ones (the OSS security hardening pass), the AI held all twelve crates in context and made consistent changes across every file. The `keycloak_url` to `oidc_url` rename touched config structs, TOML parsing, environment variable names, integration tests, documentation, and error messages. The AI found every instance. A human doing find-and-replace would have missed the test fixtures.

**Test generation.** Integration tests are tedious to write and critical to have. The AI generated 37 integration tests covering views, joins, aggregations, window functions, EXPLAIN output, and error cases. It knew what edge cases to test because it had just written the implementation. The tests weren't afterthoughts -- they were generated as part of the implementation cycle.

**Debugging.** The gRPC stream accumulation bug in Chapter 14 is the clearest example. Fifty concurrent clients hung after about 30 queries. No error, no timeout, just silence. The AI generated step-by-step tracing instrumentation, wrapped every Flight call with timing logs, and identified that HTTP/2 stream IDs were accumulating on a reused connection. The fix was one line. The diagnosis that led to it was the AI's, working from symptoms I described.

**Boilerplate elimination.** Protobuf codec implementations, Prometheus metric registrations, TOML config struct definitions, CLI argument parsing -- the kind of code that's necessary, correct, and soul-destroying to write by hand. The AI handled all of it without complaint, and without the subtle bugs that creep in when a human writes their fortieth `impl TryFrom<proto::Thing> for Thing` of the day.

### What the AI did poorly

**Architecture.** The AI never once said "this is the wrong approach, let's step back." It implemented whatever was specified, even when the specification had a structural problem. The distributed execution design went through three iterations -- not because the AI couldn't implement any of them, but because the first two had fundamental issues that the AI couldn't see.

The first design had the coordinator shipping entire `LogicalPlan` trees to workers. The AI implemented it. It worked. But it meant workers needed access to the full catalog to resolve table references in the plan. That broke the security model -- workers shouldn't need catalog access, they should receive pre-resolved physical plan fragments with concrete file paths.

A human saw that problem. The AI didn't.

**Security trade-offs.** The AI suggested a service account model twice. Not because it was wrong in general -- service accounts are the standard pattern for query engines. But for SQE, where bearer token passthrough is the entire point, a service account model is architecturally incompatible. The AI couldn't weigh "this is the standard pattern" against "this violates our core design constraint" because it didn't understand the constraint the way a human who'd sat through the twelve-minute security review did.

**"Should we?" questions.** The AI answers "how?" questions brilliantly. It does not answer "should we?" questions at all. Should we add Trino HTTP compatibility? Should we build six benchmark suites or just TPC-H? Should the policy engine support both OPA and Cedar, or pick one? These are product decisions, strategy decisions, resource allocation decisions. The AI will implement whichever one you choose. It won't help you choose.

::: {.fieldreport}
**Field report:** During the distributed execution design, the AI produced three complete implementations in three days. Each one compiled, passed tests, and was architecturally wrong for different reasons. The third iteration worked because I spent a full day writing a design document that explicitly stated the trust boundary between coordinator and worker. The AI couldn't derive that boundary from the code. It needed a human to draw the line.
:::

### What surprised us

The AI's ability to hold twelve crates in context and reason about cross-cutting concerns. When we added OpenTelemetry trace propagation, the change touched `sqe-metrics` (the OTel setup), `sqe-coordinator` (injecting trace context into gRPC metadata), `sqe-worker` (extracting trace context from incoming requests), and `sqe-core` (the shared trace context type). The AI made all four changes in a single pass, and the traces connected end-to-end on the first run.

### What didn't surprise us

The AI's inability to evaluate its own output critically. It generates code that compiles and passes the tests you asked for. It doesn't generate code that handles the test you *didn't* ask for. Every edge case we caught was caught by a human reading the code or by a test that a human specified. The AI is a brilliant implementer with no taste. Taste is the human's job.

### The speed multiplier

I've been asked for a number. What's the multiplier? 5x? 10x? 50x?

The honest answer: it depends on what you're measuring. For raw implementation -- turning a spec into compiled, tested Rust code -- the multiplier is probably 10-20x. A human writing the `sqe-bench` benchmark framework from scratch would take weeks. The AI did it in hours.

But implementation is maybe 30% of building software. The rest is design, debugging integration issues, rethinking approaches that don't work, writing specs, reviewing generated code, and making the product decisions that determine whether the code solves the right problem.

For the total project, including all of that, the multiplier is more like 3-5x. Still remarkable. Still enough to build a distributed SQL engine in fifteen days. But not the "AI does everything" story that the timeline might suggest.


## One Complete Cycle

The previous section describes the AI workflow in general terms. A reviewer rightly pointed out that the outputs are described but not the mechanics. So here's one full cycle -- spec to prompt to AI output to review to revision to final code -- for the `PolicyPlanRewriter` in `sqe-policy`.

**The spec.** An OpenSpec-style requirement, written by a human before any Rust existed:

> Row filters must be injected above TableScan nodes before the optimizer runs, so user predicates can push through row filters but not bypass them. Column masks must use expression wrapping (e.g., sha256 or redaction) that creates an expression boundary blocking predicate pushdown on raw values. Restricted columns must be removed from projections entirely -- invisible, not errors.

Three sentences. Every design constraint that matters is in there. The ordering, the injection point, the predicate pushdown semantics, the PostgreSQL RLS-style invisibility model.

**The prompt.** What we actually sent to Claude Code:

> Implement the PolicyPlanRewriter in sqe-policy. It should implement the PolicyEnforcer trait. Use LogicalPlan::transform_down to walk the plan tree. When it encounters a TableScan, resolve the policy from the PolicyStore, then: (1) inject Filter nodes above the scan for row-level filters, (2) create a Projection that wraps masked columns in their mask expressions, (3) remove restricted columns from the projection. On policy resolution failure, inject a FALSE filter to deny all rows.

**The AI output.** The first implementation compiled. It passed the basic tests -- row filters were injected, masks were applied, restricted columns vanished. But it applied masks and restrictions as independent steps, both building their projection from the schema of the node *after* filtering. When a table had both masks and restrictions, the restriction step rebuilt the projection from scratch, discarding the mask expressions the previous step had just created. Masked columns reverted to their raw values.

**The review.** A human reading the generated code spotted the problem in under a minute. The two projection steps were sequential but independent -- each one read the schema and built a fresh expression list. The second one didn't know the first one had wrapped columns in mask expressions. The structure was correct for tables with only masks or only restrictions, but broke when both applied to the same table.

**The revision.** We restructured the prompt to be explicit about ordering and mutual exclusivity: masks and restrictions should be handled in a single projection pass. When masks exist, the projection should apply masks *and* filter out restricted columns in one step. When only restrictions exist (no masks), a simpler projection removes the columns. The key insight: masks must be applied while all columns still exist in the schema, and the restriction filter happens inside the same projection, not after it.

**The final code.** The revised implementation lives in `crates/sqe-policy/src/plan_rewriter.rs`. The mask projection at lines 131-158 iterates over all fields, filters out restricted columns, and applies mask expressions to the remaining ones -- a single pass that handles both concerns. The `else if` branch at lines 160-181 handles the restrictions-only case. The ordering is: row filters first (above the TableScan), then masks-plus-restrictions in one projection, then done. One traversal, three security layers, correct by construction.

Total elapsed time for the full cycle: about forty minutes. The spec took fifteen. The first prompt and review took ten. The revision prompt and verification took fifteen. Forty minutes from written requirement to reviewed, tested, committed code.

That's the real mechanic. Not "AI writes code." Spec, prompt, output, catch the bug, revise the prompt, verify the fix. The AI is fast. The human is precise. The cycle is what produces code you can trust.


## Decisions We'd Keep

Five decisions held up under pressure. They're the ones I'd make again without hesitation.

**DataFusion as the foundation.** The extensibility model is right. Custom `TableProvider`, custom `ExecutionPlan` nodes, custom optimizer rules -- DataFusion gives you the hooks without forcing you to fork. We extended it for Iceberg catalog integration, distributed scan execution, and policy-based plan rewriting, and none of those extensions required modifying DataFusion's source. That's the mark of a well-designed library.

**Rust.** The compile-time guarantees saved us months of runtime debugging. The `Send + Sync` bounds on the `PolicyEnforcer` trait meant we couldn't accidentally create a policy enforcer that held non-thread-safe state -- the compiler simply wouldn't let us. In a distributed system where the coordinator dispatches plan fragments to workers concurrently, this kind of compile-time safety isn't a nice-to-have. It's the difference between "it works" and "it works except under concurrent load on Tuesdays."

**Bearer token passthrough.** Chapter 4 covers this in depth. The security model that makes everything else possible. Every query runs as the authenticated user. No service account. No ambient credentials. When the security team asks who accessed the customer table, the answer is a name, not an application. We considered three approaches and picked the hardest one. Fifteen chapters later, it's the decision I'm most confident about.

**Iceberg + Polaris.** Open table format, open catalog protocol, open storage. No vendor lock-in at any layer. When we wanted to add benchmark suites, we generated Parquet files and loaded them as Iceberg tables via `CTAS`. The format didn't fight us. The catalog didn't fight us. The storage didn't fight us. That's sovereignty in practice, not in marketing.

**Single-node first, distributed later.** The first working query ran on a single `SessionContext` with no coordinator, no workers, no fragment scheduler. We had correct results before we had distributed execution. That meant every distributed bug was a distribution bug, not a query bug. The debugging was orders of magnitude easier because we could always check: does this query return the right answer on a single node? If yes, the bug is in the distribution layer. If no, the bug is deeper. This binary diagnostic saved us days.


## Decisions We'd Change

These are harder to write about. Not because they're embarrassing -- they're not. They're the decisions that were reasonable at the time and turned out to cost more than expected.

**The custom SQL parser wrapper.** We wrapped `sqlparser-rs` to handle SQE-specific SQL extensions -- `GRANT ... MASKED WITH`, `ROWS WHERE`, `SHOW EFFECTIVE POLICY`. The wrapper intercepts the parse output, detects these patterns, and converts them to custom AST nodes.

The problem: DataFusion already has extension points for custom SQL. The `Statement` enum has a `Statement::Extension` variant. The `UserDefinedLogicalNodeUnparser` trait exists for exactly this purpose. We could have used DataFusion's built-in extension mechanism instead of wrapping the parser.

We didn't, because at the time of writing the spec, we hadn't fully explored DataFusion's SQL extension surface. The spec was written before the implementation, and the spec assumed we'd need to intercept at the parser level. The AI implemented what was specified. By the time we realised DataFusion's native extensions would have been cleaner, the wrapper was working and tested.

I'd evaluate DataFusion's built-in extension points more thoroughly in a first pass. The wrapper works, but it's a maintenance surface we could have avoided.

**Configuration as an afterthought.** The first version of SQE had connection strings and port numbers as constants in the source code. "Works on my machine" was literally the design constraint. The TOML config came in version 0.3. The environment variable overlay came later. The full configuration surface with validation, defaults, and documentation came much later.

This is backwards. Configuration should be a first-class concern from day one, because configuration is the API you expose to operations teams. The people deploying your engine are not the people who built it. They need config keys that are self-explanatory, defaults that work, and validation that tells them what's wrong before the engine crashes at startup.

We got there eventually. Chapter 10 shows the finished config surface. But the retrofit was painful -- every new config key required touching the struct definition, the TOML parser, the environment variable mapping, the example config, and the documentation. If we'd set up that pipeline from the start, each new config key would have been a one-line addition to a derive macro.

**Test infrastructure timing.** Integration tests against a real Polaris + S3 stack should have been in CI from the first week. We didn't add them until after the first round of debugging distributed execution issues, when we realised that unit tests with mocked catalogs were passing while the real system was broken.

The lightweight test stack -- Polaris in-memory mode plus RustFS (a Rust-native S3-compatible server) -- was the fix. No AWS account needed. No Docker pulls from third-party registries. The entire test dependency runs in-process or in local containers. We should have built this on day two.

::: {.deadend}
**Dead end: mocking the Iceberg catalog for integration tests.** We tried. We built mock implementations of the catalog traits that returned canned responses. The unit tests passed. Then we connected to a real Polaris instance and discovered that our mock didn't simulate Polaris's credential vending flow, which meant our S3 client configuration was wrong in production even though tests were green. Mock what you must, but test against the real thing as early as you can afford to.
:::


## The Abstraction That Has a Ceiling

The `ScanTask` protocol.

The `ScanTask` model works well for distributed scans. Each task carries file paths, projected columns, and filter predicates. The worker executes them independently -- build a local `ExecutionPlan` to scan those specific files, apply the predicate, project the columns, and stream results back. It is clean, simple, and correct for the workload it was designed for.

But `ScanTask` only describes scans -- it cannot represent local aggregation, sort, or join nodes above the scan. When we attempted distributed aggregation, we hit this wall: the coordinator could not express "scan these files, then group by region" as a single `ScanTask`. It needs a plan subtree. The coordinator ends up reassembling intermediate results and performing all the post-scan operations itself. That works for scan-only queries. It falls apart the moment you have a `GROUP BY` that should be partially evaluated on the worker.

The future architecture ships `PlanFragment` objects -- serialised subtrees of the physical plan that include scan, filter, projection, and partial aggregation. Workers would execute the full subtree locally and return partial results to the coordinator for final aggregation. The `ScanTask` model was not premature -- it was the right starting point. But it is a subset of what full distributed execution requires.

The AI implemented the `ScanTask` protocol without complaint. It didn't flag that the approach would limit worker-side computation. That was a human insight, born from staring at EXPLAIN output and realising that the coordinator was doing all the aggregation work while two workers sat idle after their scans completed.

::: {.deadend}
**Dead end: per-file ScanTask dispatch.** Each worker got one file at a time. Clean separation of concerns. Terrible performance for aggregation queries, because all the compute-heavy work happened on the coordinator after the scans returned. The future fix is shipping plan fragments (subtrees) instead of individual scan tasks -- turning workers from "file readers" into "local query engines."
:::


## The Abstraction We Needed Earlier

A proper `QueryLifecycle` state machine.

For the first several iterations, query execution was a linear function: parse, plan, optimise, execute, return. Each step called the next. Error handling was scattered -- some errors were caught in the planner, some in the executor, some in the Flight SQL handler. Cancellation was bolted on after the fact with `CancellationToken`. Timeouts were added separately. Query history logging was added separately again.

Each addition made the linear function more complex. By the time we had parse, plan, optimise, check-policy, maybe-distribute, execute-locally-or-dispatch-to-workers, stream-results, log-to-history, update-metrics, handle-cancellation, handle-timeout -- the function was unreadable.

What we needed from the start: a state machine that models query lifecycle explicitly. States like `Parsed`, `Planned`, `Optimised`, `PolicyChecked`, `Dispatched`, `Executing`, `Streaming`, `Complete`, `Failed`, `Cancelled`. Transitions between states are explicit. Side effects (logging, metrics, history) attach to transitions, not to the middle of a function.

We didn't build this as a formal state machine. We refactored toward it -- extracting the lifecycle into a struct with methods for each transition -- but the linear-function heritage is still visible in the code. A state machine from day one would have made the cancellation, timeout, and history features trivial additions instead of careful retrofits.


## What Rust Taught Us

Rust is not just a language choice. It's a design philosophy that infects your architecture.

**The borrow checker is a distributed systems design tool.** When you try to send a plan fragment to a worker, the compiler forces you to either clone the data or prove that nothing else is using it. This sounds like a nuisance. It's actually forcing you to think about data ownership across network boundaries. If you can't express the ownership model in Rust's type system, your distributed protocol probably has a race condition.

**`Send + Sync` constraints catch concurrency bugs at compile time.** The `PolicyEnforcer` trait requires `Send + Sync`. This means any implementation must be safe to share across threads and safe to send between threads. When we wrote the passthrough enforcer, this was trivially satisfied. When we eventually write the OPA enforcer with an HTTP client and a policy cache, the compiler will verify that the cache is thread-safe and the HTTP client is `Send`. In Go or Java, these bugs show up under load. In Rust, they show up at compile time.

**The trait system enables genuine extensibility.** DataFusion's `TableProvider`, `ExecutionPlan`, `OptimizerRule` -- these traits are the extension surface. You implement them, register them, and DataFusion calls them. No dependency injection framework. No runtime reflection. No classpath scanning. Just traits and their implementations. This is the right model for a pluggable system that needs to be fast.

**Token efficiency for AI-assisted development.** This is an underappreciated property. Rust code is dense. A Rust struct with derive macros, a trait implementation, and a few methods conveys more semantic information per token than the equivalent Java or Python code. When your coding agent has a context window and you're working across twelve crates, density matters. The AI could hold more of the codebase in context because Rust doesn't waste tokens on boilerplate.

**The compile times are real.** I mentioned this in Chapter 3, and I'm going to be more specific here because the retrospective earns specificity.

A clean build of all twelve SQE crates from scratch takes about eight minutes on an M3 MacBook Pro. Incremental builds after a single-file change in a leaf crate take 15-30 seconds. Incremental builds after changing a type in `sqe-core` -- which everything depends on -- take 2-4 minutes because half the dependency tree recompiles.

Multiply by fifty builds a day. That's between 12 minutes and 3 hours of waiting, depending on what you're changing. Over fifteen days, the cumulative wait time is measured in hours.

Strategies that helped:
- `cargo check` instead of `cargo build` for type-checking without codegen -- about 3x faster
- Workspace splitting so crate boundaries limit recompilation blast radius
- `split-debuginfo = "unpacked"` in the dev profile to skip the macOS dsymutil step
- `debug = 1` (line tables only) instead of `debug = 2` (full debug info)
- Never running `cargo build --release` during development

Strategies we should have adopted earlier:
- Feature-gating optional subsystems (`distributed`, `trino-compat`, `bench`) so you only compile what you're working on
- `sccache` for shared compilation cache across branches

Worth it? Yes. Every time. The hours spent waiting for the compiler are hours you don't spend debugging null pointer exceptions, data races, use-after-free, or the hundred other runtime bugs that Rust prevents. But budget for it. New Rust projects underestimate compile-time impact by 3-5x.


## The Open-Source Goal

SQE is built to be open-sourced. That sentence appears in the Preface and it's repeated here because the retrospective is where I can explain what it actually cost.

Designing for open source is more expensive than designing for internal use. Every decision has a second audience. The config section that works for our Keycloak instance needs to work for someone else's Auth0 instance. The catalog integration that assumes Polaris needs to be pluggable for someone running Nessie or AWS Glue. The naming that references our internal infrastructure needs to be generic enough for strangers.

The OSS security hardening pass -- Step 3 in our roadmap, fifty-one tasks -- was entirely about this. Renaming `keycloak_url` to `oidc_url`. Replacing MinIO-specific language with generic S3. Removing internal hostnames from example configs. Adding TLS support that we didn't need internally but that any production deployment would require. Adding rate limiting that our five-person team didn't need but that a public-facing deployment would.

Fifty-one tasks. Days of work. Zero new features. All of it was necessary if we wanted strangers to run this thing.

The pluggable architecture -- `PolicyEnforcer` trait, `AuthProvider` trait (designed, not yet fully implemented), `CatalogBackend` trait (designed, not yet implemented) -- exists because of the open-source goal. Our internal deployment uses Polaris, Keycloak, and a passthrough policy enforcer. But the traits are there so that someone else can plug in AWS Glue, Okta, and OPA without forking the codebase.

This design-for-pluggability approach is directly connected to the spec-driven development model. You can't design a good trait boundary if you don't know what implementations it needs to support. The specs forced us to think about multiple implementations before writing the first one. The `PolicyEnforcer` trait is twenty-six lines long. It took longer to specify than to implement. And it will support OPA, Cedar, and whatever policy engine comes next, without changing a single line.

::: {.artofagents}
**Art of Agents:** This is *Use of Spies* (Chapter 13) -- the feedback loop. The retrospective closes the build cycle: specs, design, build, measure, learn. The learnings feed the next spec. The open-source goal means those learnings are public too. The feedback loop extends beyond the team to every engineer who reads this code or this book.
:::


## Where This Goes Next

Four trajectories, in order of impact.

**Pluggable auth and catalogs.** Steps 4 and 5 in the roadmap. The traits are defined. The config sections are stubbed. The implementation is next. Bearer token passthrough via OIDC remains the primary path, but API key auth, mTLS, and anonymous access are all in the design. Catalog backends for AWS Glue, Nessie, and storage-only (scan a directory for Iceberg metadata, no catalog server needed) are specified.

This is the work that makes SQE usable beyond our specific infrastructure. Right now, you need Polaris and an OIDC provider. After these steps, you need whatever you already have.

**Upstream DataFusion improvements.** DataFusion's release cadence is fast -- we're on version 52. Each release brings optimizer improvements, new SQL functions, better memory management. SQE benefits from all of it for free, because we built on the library rather than forking it. The features we're watching: improved recursive CTE performance (for graph queries), better spill-to-disk under memory pressure, and native Iceberg predicate pushdown in DataFusion's optimizer.

**Iceberg v3 features.** Row-level deletes (Merge-on-Read) are blocked on iceberg-rust, with an estimated landing of Q3 2026. When that lands, `DELETE FROM` and `MERGE INTO` become possible. Branching and tagging -- Iceberg's version control for tables -- are also in the roadmap. These features turn SQE from a read-heavy analytical engine into a full read-write engine suitable for dbt workloads with incremental models.

**The semantic AI layer.** This is the ambitious one. RDF triple stores on Iceberg. Property graph queries compiled to DataFusion logical plans. Vector search via Lance datasets. AI agent interfaces via CLI, REST, and MCP. Each of these is a separate crate, fully additive, breaking nothing in the existing engine.

The semantic layer is designed but unbuilt. It represents the thesis that a query engine should be agent-native -- discoverable, self-describing, and composable by AI agents that need to explore and query data without human mediation. Whether that thesis is right is a question for the next version of this book.


## The Build-vs-Buy Honest Accounting

This is the section that technical leaders will skip to, and it's the section I most want to get right.

**Total engineering time.** Fifteen days of intense development. That's one person (me) plus an AI coding agent, working full days. Call it fifteen person-days of human effort, multiplied by whatever factor you want to assign to AI assistance. The AI amplified output by roughly 10x for implementation tasks and roughly zero for design tasks. A reasonable estimate of equivalent human-only effort: two to three months for a senior Rust engineer, six months or more for a team less familiar with DataFusion and Iceberg.

**What you get.** A distributed SQL engine with OIDC auth, bearer token passthrough, Iceberg catalog integration, Arrow Flight SQL and Trino HTTP wire protocols, observability (OpenTelemetry + Prometheus), a CLI, a benchmark suite covering six industry-standard benchmarks, a Docker multi-stage build, and 37 integration tests. Twelve crates, clear boundaries, documented architecture.

**What you don't get.** Enterprise-grade maturity. Battle-tested failure recovery under real production load. A community of contributors fixing bugs you haven't hit yet. The ten years of optimisation that Trino's join algorithms represent. Support contracts. Compliance certifications.

**Operational cost.** One Helm chart. One coordinator pod. One or more worker pods. The coordinator uses about 512MB of memory at idle and scales with concurrent query count. Compare this to Trino's deployment: coordinator, multiple workers, a discovery service, possibly a separate metadata store, Ranger or Sentry for security, and the operational expertise to tune all of it.

**The capability gap.** Trino handles complex multi-way joins better. Trino's exchange operators are more mature. Trino's ecosystem of connectors (Postgres, MySQL, Kafka, Elasticsearch) is vast and battle-tested. SQE connects to Iceberg via Polaris and that's it. If your workload is "analytical queries over Iceberg tables with strict auth," SQE matches or exceeds Trino. If your workload is "query everything from everywhere," Trino wins.

**The capability gain.** SQE does things that nothing else can. Every query runs as the authenticated user -- not a service account. Bearer tokens pass through to storage. The audit trail shows humans, not applications. The policy engine rewrites query plans before optimisation, making row filters and column masks invisible to the user and impossible to bypass. This isn't a feature gap that Trino could close with a plugin. It's an architectural property that requires building the auth model into the engine's foundation.

**When to build.** When you have a non-negotiable architectural constraint that existing tools cannot satisfy. For us, that constraint was bearer token passthrough -- the security model where every I/O operation traces back to a human identity. No existing query engine supports this because it requires building the auth model into the plan execution path, not bolting it on top.

**When not to build.** When the constraint is negotiable after all. If the security team will accept application-level logging with a service account, use Trino. If the workload fits in a single node, use DuckDB. If you need connectors to fifteen different data sources, use Spark. Building a query engine is the right choice when -- and only when -- the architectural constraint is genuinely non-negotiable and no existing tool satisfies it.

The honest truth: most constraints are more negotiable than engineers believe. We build because we can, and we justify it with constraints. The discipline is knowing when the constraint is real and when it's an excuse to build something interesting.

For SQE, the constraint was real. The security team's twelve-minute rejection of the service account model was the proof. But I'd be lying if I said the engineering drive -- the desire to understand how query engines actually work, to build one, to take it apart and put it back together -- wasn't a factor.

It was. Engineers build things. That's the drive. The trick is building things that matter.


## The Book That Found Bugs

Something unexpected happened while writing this book: it found bugs.

Not in the prose. In the code.

Writing a chapter forces you to explain what the code does, and explaining what code does is the fastest way to discover that the code doesn't do what you think. Chapter 8 described the `PolicyPlanRewriter` injecting row filters and column masks. When we went to reference the sha256 masking function, we discovered DataFusion doesn't ship one. That's not a bug in the usual sense, but it's a gap that would have bitten the first user who tried hash-based column masking. Writing the chapter forced us to implement the UDF.

Chapter 7 described the Iceberg commit mechanism and referenced an `x-iceberg-update-sequence-number` HTTP header for optimistic concurrency. When the reviewers checked the Iceberg REST specification, the actual mechanism is `assert-current-snapshot-id` in the request body, not an HTTP header. The code was correct -- iceberg-rust handles this internally -- but the mental model was wrong. Writing it down exposed the gap between what we thought was happening and what was actually happening.

Chapter 3 described DataFusion's pull model as "nothing is computed until someone reads from the output." A reviewer pointed out that `HashJoinExec` eagerly builds its hash table. The code was fine -- we weren't building incorrect joins. But our understanding of the execution model had a blind spot that would have eventually caused a memory accounting bug.

The `block_in_place` usage in `SchemaProvider::table_names()` had been there since the first week. Nobody flagged it. Writing the chapter, a reviewer identified it as a known anti-pattern that could deadlock under certain executor configurations. We added safety documentation and put an upstream proposal on the backlog.

In total, writing the book produced 12 code fixes and 6 remaining TODO items. Some were genuine bugs. Some were missing features. Some were documentation gaps that would have confused contributors. All were invisible until someone had to explain, in writing, what the code was supposed to do.

::: {.fieldreport}
**Field report:** The book review process caught factual errors about Iceberg's partition evolution ("unique to Iceberg" -- it's not anymore, Delta Lake has liquid clustering), PyIceberg's GIL behavior (I/O releases the GIL, only scan planning is bound), and Polaris's access control model (it does have RBAC, not "no opinions"). Every claim in a technical book is a claim that can be verified. The reviewers verified them, and several were wrong.
:::

This is the strongest argument for writing about your code, not just writing code. A codebase that compiles and passes tests can still contain incorrect assumptions, missing features, and misleading mental models. Writing forces you to make those assumptions explicit. Reviewers force you to defend them. The code improves even though nobody opened a pull request.


## The Hardest Lesson

The hardest lesson from this project isn't about Rust or DataFusion or distributed systems. It's about the relationship between a human and an AI coding agent.

The AI made the pace possible. Without it, this project would have taken months, not weeks. The implementation quality is high -- the code is idiomatic, the tests are thorough, the error handling is consistent across twelve crates.

But the AI didn't make the decisions. Every architectural turn -- bearer passthrough over service accounts, plan fragments over scan tasks, single-node-first development, the trait boundaries for pluggability -- was a human decision. Some of those decisions were wrong the first time and had to be revised. The AI implemented the wrong version just as competently as it implemented the right one. It didn't know the difference.

The model that works is: **human architect, AI builder.** The human decides what to build and why. The AI decides how to build it and does the work. The human reviews every big turn. The AI executes between the turns.

This sounds obvious written down. In practice, the temptation is to let the AI lead. It's fast. It's confident. It produces working code. The code compiles and the tests pass. But "compiles and tests pass" is not the same as "solves the right problem." The AI optimises for the local objective -- make this function work, make this test pass. The human optimises for the global objective -- does this architecture serve the security model, the operational constraints, the open-source goal.

Fifteen days. Three hundred and sixteen commits. Twelve crates. One principal engineer who made the decisions. One AI agent that did the work.

That ratio -- one human, one AI, clear roles -- is the thing I'd keep above all else.

::: {.ailog}
**AI Logbook:** This chapter is the one the AI could not have written alone. The retrospective required evaluating which decisions were right, which were wrong, and why — judgments that depend on context the AI never had. The AI drafted the prose from the human's structural outline and specific commit references. The honest assessment of AI limitations (three wrong distributed execution designs, inability to evaluate its own output, the security trade-off it missed twice) was the human's observation; the AI would have reported its own work as successful at every stage.
:::

::: {.sovereignty}
**Sovereignty principle:** Sovereignty applies to your development process too. The AI is a tool, not a decision-maker. You own the architecture. You own the security model. You own the trade-offs. The AI accelerates the implementation of decisions you've already made. The moment you let it make the decisions, you've outsourced your sovereignty to a model that doesn't understand your constraints.
:::

::: {.fieldreport}
**Field report:** This book was also written with AI assistance. The same model applies: I decided what each chapter needed to say, wrote the structural outline, identified the specific commits and code examples to reference. The AI did the prose drafting. I edited every paragraph. The voice is mine. The pace was the AI's. The ratio holds.
:::
