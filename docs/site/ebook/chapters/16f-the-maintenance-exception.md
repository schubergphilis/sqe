# The Maintenance Exception {#sec:maintenance-exception}

> Every query runs as the authenticated user. That sentence is the spine of this book. This chapter is about the one thing that cannot obey it, and what we did instead of pretending otherwise.

Chapter 4 spent its whole length on one idea: no service account. Every query, every catalog call, every S3 read traces back to a human identity. When the security team asked "who accessed the customer table last Tuesday," the answer was a name.

Then we needed compaction to run itself.

Small files accumulate. Merge-on-Read tables accumulate position deletes and equality deletes on top of that. Nobody logs in at 2 AM to type `CALL system.rewrite_data_files`. If SQE is going to stay healthy without a human watching it, something has to run in the background, on a schedule, with no user behind it.

That something needs write privilege on a table. It needs to authenticate. And by definition, nobody is sitting at a keyboard when it does. Every identity we had was built on the assumption that a person requested this query. Autonomous maintenance breaks that assumption on purpose, because that is the whole point of autonomy.

That tension is not a bug in the design. It is the design meeting a case it was never built for. The question was not "how do we bend the rule." It was "how do we contain the one place it has to bend."

## Why "just add a service account" was the wrong question

The easy answer already existed in every other query engine we'd looked at. Trino runs its coordinator as a service identity. Spark's maintenance jobs run under whatever role submitted the job, usually a shared one. Nobody treats background table maintenance as a special case, because in those engines every query is already running under some form of shared identity. Adding one more shared identity for maintenance is not a compromise. It is consistent with how the rest of the system already works.

SQE doesn't have that luxury, and we didn't want it. Bearer passthrough means the query path has never had a service account to begin with. Adding one now, even scoped to maintenance, would plant the first shared credential in an engine whose entire architecture assumes there isn't one. Once that credential exists, the question stops being "does SQE have a service account" and becomes "does SQE have exactly one service account, used only for maintenance, or does that boundary erode over the next two years." We have watched boundaries erode. Nobody erodes them on purpose. They erode one convenient shortcut at a time.

So the constraint we set for ourselves was specific: the interactive query path has to stay 100% user-identity, and it has to stay that way not because of a policy someone remembers to enforce, but because the code makes the alternative impossible to reach.

## Structurally unreachable, not policy-forbidden

The difference between "we have a rule against this" and "this cannot happen" is the difference between a lock on a door and a door that was never built.

`sqe-auth` already has an `OidcM2mProvider` for client-credentials token flows. Wiring it into the interactive auth chain would have been the fast version: add an `AuthProviderConfig::M2m` variant, let an operator select it in `build_auth_chain`, done in an afternoon. We didn't do that.

Instead, `MaintenancePrincipal` in `crates/sqe-coordinator/src/maintenance_principal.rs` owns a private `OidcM2mProvider` instance that it constructs directly from `[maintenance.principal]` config. There is no `AuthProviderConfig::M2m` variant. `build_auth_chain` was never touched. The doc comment on the struct says it plainly:

```rust
/// This is structurally isolated from the interactive auth chain on purpose.
/// `MaintenancePrincipal` wraps its own `OidcM2mProvider` instance, built
/// directly from `MaintenancePrincipalConfig`. It does not go through
/// `sqe_auth::factory::build_auth_chain`, and there is no
/// `AuthProviderConfig::M2m` variant: the only way to construct one is from
/// inside this module, from the `[maintenance]` config section.
```

An operator cannot misconfigure their way into using the maintenance principal for interactive queries, because there is no config knob that reaches it from that side. A future engineer cannot accidentally plug it into a listener, because the type that would let them do that doesn't exist. That absence is what "structurally unreachable" means in practice: not a check that runs, but a wire that was never soldered.

Three more layers sit on top of that structural one. Sessions the principal mints are ephemeral, minted per job, and never inserted into `SessionManager` at all, so there's no shared session an attacker or a bug could reuse across jobs. The scheduler that holds the principal calls only the maintenance dispatcher, never the general SQL query path, so even a compromised scheduler can't run arbitrary user SQL as the principal. And the whole thing is off by default: `mode = "off"` in `[maintenance]`, and the coordinator refuses to start in any other mode without a principal block configured. Four layers, each one closing a different door, stacked because any single layer failing shouldn't be the only thing standing between the maintenance identity and a query it was never meant to run.

## Deny-by-default, opted in one table at a time

Structural isolation answers "can the principal reach interactive queries." It doesn't answer the second question: which tables can it touch at all.

The answer is per-table, and it requires two things to agree before a single byte gets rewritten. The table owner sets `sqe.maintenance.enabled = 'true'` as a table property, visible in table history like any other property change, no hidden flag. Separately, the Polaris principal role backing the maintenance identity gets exactly `TABLE_READ_DATA` and `TABLE_WRITE_DATA` on the opted-in namespaces. No `CREATE`, no `DROP`, no admin scope. Polaris enforces that server-side, so even if something upstream of Polaris got the authorization logic wrong, the catalog itself refuses anything outside that grant.

Both conditions have to hold. A table with the property but no grant fails loud, an audit event and a metric, never a silent skip, because silent skips are how operators discover gaps the hard way, six months later, during an incident. A table with a grant but no property is simply never selected. Opt-in means opt-in twice, from two different directions that don't trust each other.

That double-key pattern is deliberate. A single switch, even a well-intentioned one, is one bug away from touching a table nobody meant to hand over. Two switches that both have to be right, one set by the table owner and one set by the platform operator, means a single mistake in either place fails safe.

## Advisory before autonomous

We didn't ship straight to autonomous rewriting. The rollout has three modes, and the first two touch nothing.

`mode = "off"` is the default and mints no principal at all. `mode = "advisory"` reads snapshot summaries, `total-data-files`, `total-delete-files`, `total-position-deletes`, and turns them into Prometheus gauges and a `CALL system.table_health('t')` surface that tells an operator exactly which `CALL` to run and why, without ever writing to the table. Only `mode = "active"` lets the scheduler actually dispatch a rewrite, and even then only against tables that cleared both opt-in checks above.

An operator can run advisory mode forever and get fleet-wide small-file and delete-debt visibility with zero credentials configured. Nothing mutates until someone deliberately flips one more switch. That staging wasn't caution for its own sake. It meant we could prove the detection logic against real snapshot data before the code that writes anything existed, and it gave operators a genuine off-ramp: advisory-only is a legitimate, permanent choice for anyone who wants the dashboard without the autonomy.

## The compaction the scheduler is allowed to run

None of this would be worth building if the compaction underneath it wasn't already trustworthy, and it hadn't always been.

`rewrite_data_files` existed on the coordinator well before this project started. It read raw Parquet, bin-packed small files, and swapped them in. It worked, right up until a table had live delete files. On a Merge-on-Read table, the naive rewrite read the raw data without applying the position or equality deletes sitting on top of it, wrote fresh files under new paths, and left the surviving deletes pointing at file paths that no longer existed. The rows those deletes were supposed to hide came back. Silently. No error, no warning, just rows that should have been gone showing up again after a maintenance call that looked completely routine.

We would never point an autonomous scheduler at that code. So before any of the scheduling or the service-principal work started, the rewrite path itself had to become delete-aware. The fix routes the rewrite through the table's own scan planner instead of a raw file read, so position and equality deletes get applied the same way an interactive query would apply them, and the invariant changes from "rows written equals rows removed" to "rows written equals rows the scan actually produced," with a cross-check that the position-delete count still reconciles.

The subtler correctness question was concurrency. Compaction takes time. A rewrite of a large partition can run for minutes. If another writer commits new equality deletes against that partition while the rewrite is in flight, do those deletes still apply to the freshly rewritten files, or do they silently stop mattering because the rewrite captured an older sequence number?

The fix is a sequence-number pin: `set_new_data_file_sequence_number(seq_at_start)`, called at the moment the rewrite plans its input, not at commit time. The rewritten files carry the sequence number the table had when the read began, not a fresh one from the eventual commit. A concurrent equality delete committed at a higher sequence number still applies against the rewritten files, because Iceberg's delete-application rule compares sequence numbers, not commit order. Get the pin wrong and compaction quietly reintroduces the exact resurrection bug it was built to fix, just through a different door.

Sort compaction added a third property worth stating plainly, because it's easy to miss and expensive to get wrong. Bin-pack groups files within a partition and repacks them; sort takes the whole partition as one stream, sorts it, and lets a rolling writer cut new files at the target size. That last detail matters more than it sounds. Sorting per group instead of per partition would produce output files that each span the full key domain, because every group saw the same range of keys. Sorting the partition as a single stream produces output files with disjoint, non-overlapping key ranges, which is the property that actually lets query-time pruning skip whole files instead of opening every one of them and checking. The difference between those two isn't a performance nuance. It's the difference between a sort that helps and a sort that only looks like it does.

## Making it scale: from one coordinator's memory to a fleet

Delete-aware, sequence-pinned, correctly grouped compaction still ran entirely on one machine: the coordinator, the same node serving interactive queries. That caps compaction at one node's CPU and NIC, and it competes for both with the queries SQE exists to answer.

The fix follows the same shape as the rest of SQE's distributed execution: workers do the expensive work, the coordinator keeps the one decision that has to stay singular. A worker receives one file group over a signed Arrow Flight `do_action`, HMAC over the exact wire bytes, the same signing scheme the scan path already uses. It pins itself to the exact snapshot the coordinator read, using `StaticTable::from_metadata_file` to build a table view straight from a `metadata.json` path and S3 credentials, no catalog token at all. It re-plans the delete-aware read for just its group, applies deletes, sorts if asked, writes fresh Parquet directly to the table's storage location, and reports back a small Avro-encoded description of what it wrote.

The coordinator never receives raw data back from a worker. It collects those small descriptions, re-checks the row-count invariant across the whole job, and issues exactly one `RewriteFilesAction` commit that swaps every old file for every new file at once. If any group fails, the whole job fails before anything commits. Commit authority never leaves the coordinator, which is the same principle Chapter 13 spent a whole chapter establishing for query execution: workers act, the coordinator decides.

We considered the cheaper-looking alternative first, streaming decoded batches back to the coordinator and letting it handle the encode and write. It parallelizes decode and sort, nothing else. Every byte still crosses the network twice, and every encode and upload still funnels through one NIC. Compaction is I/O-symmetric: read, decode, maybe sort, encode, write. Only distributing the write side actually distributes the bottleneck.

## The lock that wasn't there, and the one that actually works

Multiple coordinators can run behind SQE for high availability. Multiple coordinators running an autonomous maintenance scheduler creates an obvious question: what stops two of them from compacting the same table at the same time, wasting a fleet's worth of compute rewriting terabytes that only one commit can keep?

The instinct is to reach for a distributed lock. We reached for one too, and the first shape we tried was wrong in a way worth explaining, because the wrongness taught us something about Iceberg we hadn't internalized.

The plan was: a claim row in a state table, appended via a plain Iceberg `fast_append`, naming the coordinator that holds the lease. Two coordinators race to append a claim; only one should win. We wrote a spike test to prove it before building anything on top of it, two racing coordinators, one commit action, check who lands.

Both landed.

`Transaction::commit()` reloads the table to the freshest metadata and rebuilds the pending action against it on every attempt, and appends are commutative by construction: an append doesn't care what else is in the table, it just adds a row. There is no base state for a fast-append to conflict against, so there's nothing for a second claimant's commit to fail on. A fast-append claim isn't a lock. It's two people writing their name on two different pieces of paper and calling it a queue.

The primitive that actually gives exclusion looks less like an append and more like a swap. `Transaction::rewrite_files().delete_files([sentinel]).add_data_files([claim]).set_check_file_existence(true)` deletes a known sentinel file and replaces it with the claimant's own file in the same commit. Both racers target the same sentinel. Whoever commits first removes it. The second commit reloads to the freshest table, which no longer has that sentinel, `check_file_existence` runs its validation against the current manifest, finds the delete target gone, and hard-fails. Deterministically, not as a timing-dependent race: once the sentinel is gone, every later attempt by the loser rediscovers the same fact, so there's no retry count that heals it.

There was a second surprise sitting inside the first one. That failure surfaces as `ErrorKind::DataInvalid` with `retryable() == false`, which is not what our existing conflict-retry machinery was built to recognize. `commit_with_retry`'s heuristics look for `CatalogCommitConflicts` and phrases like "stale snapshot" or "rowdelta conflict." A file-existence failure reads as "Cannot delete files that are not in the current snapshot," which matches none of that. Routed through the existing retry path unmodified, a losing claimant would get a single hard failure with no automatic retry, which is actually the correct behavior for a lease, losing the race means back off and check again next tick, not spin, but only because we noticed and classified it deliberately. An unclassified `DataInvalid` looks exactly like a bug report, and it would have generated one the first time two coordinators actually raced.

## The lease is an efficiency layer. It is not where correctness lives.

Here is the part worth being blunt about, because it's the kind of distinction that's easy to state and easy to forget under pressure: the lease we built from that primitive does not make distributed maintenance correct. It makes distributed maintenance cheap.

Correctness was already established, by the same commit machinery every rewrite already used. `RewriteFilesAction` with `check_file_existence(true)` means that if two coordinators ever do plan and dispatch a rewrite for the same table at the same time, whether the lease was never configured, a lease operation failed and the scheduler proceeded anyway, or a lease was legitimately stolen mid-job, exactly one commit wins. The loser's commit hits a file-existence check against files the winner already removed, fails as a non-retryable conflict, and that coordinator's job ends having changed nothing on the table. Nothing about the lease's presence, absence, or failure changes that outcome.

What the lease buys is not having to pay for the loser's redundant work. Without a lease, two coordinators could both read a hundred-gigabyte partition, apply deletes, sort, and write fresh Parquet, and only then discover at commit time that one of those runs was wasted. With the lease, the second coordinator sees the claim before it starts scanning and skips the tick for that table. Waste avoided, not corruption prevented, because corruption was never possible in the first place.

That framing matters for how you reason about failure. A coordinator that crashes mid-job has committed nothing, because the rewrite is one atomic commit; the table is untouched, and the lease it held simply expires on its TTL. The one honest gap left in the design is the very first claim on a brand-new lease row, which has no existing row to compare-and-swap against and uses an unprotected append, so two coordinators racing that specific first-ever claim can both succeed. We documented that as an accepted, narrow exception rather than pretending it doesn't exist, because the table-level commit still decides the actual outcome regardless of which coordinator thinks it holds the lease.

## What we shipped verified, and what we're honest about not having run yet

The distributed rewrite path, the wire protocol, the dispatch and retry logic, the atomic commit assembly, all of it is covered by unit tests and integration tests against the coordinator's own test harness. The lease primitive is covered by a spike test that deliberately races two claimants against a real catalog and checks who wins, which is how we found the fast-append problem in the first place instead of shipping it.

What we have not run is the thing that actually proves the performance case: a real multi-worker fleet, compacting a real large table, with the wall-clock comparison against the coordinator-local path. The plan calls for that specific gate, distributed rewrite beating coordinator-local by a meaningful margin at scale, before we'd call the performance story proven rather than architecturally sound.

We shipped anyway, and I want to be clear about why that's not the same as shipping untested. Every correctness-bearing decision in this design, commit authority staying on the coordinator, the sequence-number pin surviving distribution, the lease's failure modes, is verified by construction and confirmed under review: the code cannot commit from a worker, the pin is captured and applied coordinator-side regardless of where the read happened, and the spike test proves the exclusion primitive against a real race. What's gated on a live multi-worker run is a number, not a correctness property. We know the design can't corrupt a table. We don't yet know exactly how much faster four workers are than one coordinator, and we said so in the plan instead of writing down a number we hadn't measured.

That's the honest version. Not every claim in this chapter has a benchmark attached yet. The ones that need a benchmark say so.

## What this cost, and what it bought

Adding autonomous maintenance to an engine built around "no service account" is not free, and it shouldn't look free in the retelling. It cost a config section, a principal type, a scheduler, a lease, a worker-side compaction action, and a spike test that had to fail once before we understood why our first design was wrong. It cost the discipline of shipping advisory mode before autonomous mode, even though autonomous mode was the interesting part.

What it bought is an exception with edges. The maintenance principal exists, but it cannot reach a listener, cannot touch a table without two independent opt-ins, and cannot run anything but the maintenance dispatcher. The scheduler exists, but it defaults to off and its most permissive mode still commits through the exact same conflict-safe path every manual `CALL` already used. The lease exists, but the table was never depending on it for correctness, only for not wasting a fleet's compute.

Sovereignty for user queries never bent. The one place we needed something to run without a person watching, we built a narrow, audited, deny-by-default exception, and we made sure the exception couldn't grow into the rule by accident.
