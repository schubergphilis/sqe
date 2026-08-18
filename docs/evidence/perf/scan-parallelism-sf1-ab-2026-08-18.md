# Scan-parallelism flags at SF1: A/B/C/D (2026-08-18, issue #414)

What `query.parallel_probe_scan` and `query.parallel_scan` actually cost and buy at
SF1, measured as a four-arm A/B on one box in one session. Issue #414 says the flags
are opt-in and that `parallel_probe_scan` costs TPC-DS about +26% at SF1. Both claims
predate the 2026-07-08 default flip and neither survives measurement.

## Rig

Native coordinator (release profile, no LTO changes), local Polaris + RustFS test
stack brought up fresh (the previous stack had 20h of uptime and was torn down with
`down -v`), SF1 data generated and loaded once, then reused by every arm. Each arm is
its own coordinator process started from a config that differs only in the two flags;
queries run through `sqe-bench test <suite>` against the already-loaded tables, so no
arm pays a load.

Arms run interleaved A, B, C, D, A, B, C, D so machine drift spreads across arms
rather than loading one of them. Two reps per arm, three suites per rep.

| Arm | `parallel_probe_scan` | `parallel_scan` |
|---|---|---|
| A | true (shipped default) | true (shipped default) |
| B | false | false |
| C | true | false |
| D | false | true |

One number is deliberately excluded: the very first defaults run went through
`benchmark-test.sh`, so it followed the generate + load of three suites on a stack
that had just written gigabytes to RustFS. It reported TPC-DS 42.8s against 34-36s
for every settled-stack arm. That is the load envelope, not the flag. Only the eight
interleaved runs below are comparable.

## Suite totals (ms, mean of 2 reps)

| Suite | A both-on | B both-off | C probe-only | D scan-only | A-B |
|---|---|---|---|---|---|
| tpch (22/22) | 14207 | 13630 | 13665 | 13518 | +4.2% |
| ssb (13/13) | 4720 | 4690 | 4839 | 4661 | +0.6% |
| tpcds (99/99) | 35905 | 34534 | 35889 | 33833 | +4.0% |

Rep-to-rep spread inside a single arm reaches 5% (arm B TPC-DS: 35380 then 33687).
Every cross-arm total delta is at or below that. Summed over queries the effect and
the noise are the same size: sum of per-query |A-B| on TPC-DS is 2664ms against
2675ms of within-arm B rep-to-rep spread.

**On suite totals, SF1 cannot qualify these flags.** Correctness is unaffected:
22/22, 13/13, 99/99 in all eight runs.

## The rules do fire, so the null is not "nothing happened"

Same four arms, one debug-logged run each, counting the rules' own log lines:

| Arm | `ParallelProbeScanRule` fired | `ParallelScanRule` fired |
|---|---|---|
| A both-on | 123 | **2** |
| B both-off | 0 | 0 |
| C probe-only | 123 | 0 |
| D scan-only | 0 | **26** |

Two things fall out.

`ParallelProbeScanRule` fires on 123 of the 134 queries in the three suites, so the
SF1 null is a real measurement of a rule that ran, not of a gate that never opened.

`ParallelScanRule` fires 26 times alone and twice when the probe rule is also on. The
probe rule runs first and bumps the same scans; `is_bumpable_scan` then rejects them
because it only takes scans still at `partition_count() == 1`. The config comment
calling the two rules complementary is wrong on the shipped default: with both flags
on, `parallel_scan` is nearly inert, and its 128MB `distribution_threshold` gate is
not what makes it inert.

## Per query, the cost is real and concentrated

Suite totals hide it; per-query numbers with each query's own rep-to-rep spread as the
yardstick do not. TPC-DS, mean of 2 reps, ms:

| Query | B off | A on | A-B | D-B | B rep noise |
|---|---|---|---|---|---|
| q67 | 428 | 620 | **+192** | -10 | 19 |
| q72 | 1108 | 1293 | +186 | +68 | 85 |
| q30 | 229 | 408 | **+180** | +4 | 2 |
| q22 | 305 | 434 | +129 | +15 | 32 |
| q31 | 513 | 640 | **+128** | -6 | 18 |
| q84 | 248 | 349 | +101 | -22 | 78 |
| q47 | 706 | 794 | **+88** | +9 | 40 |
| q29 | 532 | 618 | **+86** | -26 | 21 |
| q64 | 1690 | 1622 | -68 | -56 | 27 |

Bolded rows move by 4x to 90x their own rep-to-rep spread. q30 is +79% on 2ms of
noise. Arm D sits at zero for all of them, and arm C tracks arm A (35889 vs 35905 on
the suite), so the cost is `parallel_probe_scan`, not `parallel_scan`.

q67 and q47 are two of the three queries the config comment already blames for the
SF10 residual ("the real cost of parallel window/rollup plans on q67/q47/q57"). Same
queries at SF1, so same cause at smaller magnitude. q30, q22, q31 and q29 are not
named anywhere and are the same size or larger.

There are wins, and they are smaller: TPC-H q12 -106ms (-19%) on 7ms of noise, TPC-DS
q64 -68ms on 27ms. SSB moves by at most 30ms on any query, which at ~360ms per query
leaves parallel decode nothing to bite on.

## What this licenses, and what it does not

Corrected by this run:

- The flags are **not opt-in**. `parallel_probe_scan` and `parallel_scan` both default
  to `true` and have since 2026-07-08. `limitations.md` telling operators to leave
  `parallel_probe_scan` off, and `production-guide.md` listing "enable
  `parallel_probe_scan`" as tuning step 3, describe a build that does not ship.
- **+26% on TPC-DS at SF1 does not reproduce.** The suite total moves +4.0%, inside
  the noise floor. The cost is real but lives in about eight queries.
- The two rules **overlap**; they do not complement each other.

Not licensed by this run:

- No default change. A fast laptop at SF1 does not overturn the clean-rig SF10 numbers
  the default was set on, and SF1 cannot resolve a 4% total either way.
- No claim about SF10+. Whether the same eight queries carry the SF10 residual needs
  the separate-host qualification that issue #416 exists for.

## Companion result: issue #415 on the same rig

The distributed half of the same session, since the two issues share the code
path. Native two-worker rig (coordinator plus two `sqe-worker` processes on
loopback, no docker image), distribution forced at SF1 with
`distribution_threshold = 1MB` and `distribution_file_threshold = 1`, and left
at the shipped gates for SF10 SSB, which distributes on its own.

| Rig | InList cap | Distributed | Filters pushed | Conjuncts dropped | Oversized refusals | Result |
|---|---|---|---|---|---|---|
| SSB SF1 | 65536 (shipped) | 13/13 | 26 | 0 | 0 | 13/13, 7.36s |
| SSB SF1 | 64 (forced) | 13/13 | 26 | 62 | 0 | 13/13, 10.01s |
| SSB SF10 | 65536 (shipped) | 13/13 | 221 | **34** | 0 | 13/13, 39.5s |
| SSB SF10 | 1000000 + 64MB | 13/13 | 204-221 | 0 | **17-18** | one query errored in 2 of 3 runs |

Every SF10 drop is on `lo_partkey`, and every one keeps its bounds
(`kept: true`). That is the SSB partkey class: at SF10 the part-side key set
exceeds the cap, so the worker's fact scan receives `lo_partkey >= min AND
lo_partkey <= max` on a uniformly distributed foreign key, which prunes almost
nothing. At SF1 the same builds fit under the cap, membership rides as an
`InList`, and nothing is lost. So #415 is real, it is scale-dependent, and SF1
cannot see it.

**Raising the cap is not the workaround.** With `runtime_filter_inlist_max_values
= 1000000` the drops go to zero and the fragments start failing instead:
workers refuse 9.1 MB messages with "decoded message length too large: the limit
is 4194304 bytes". SQE sets no `max_decoding_message_size` anywhere, so tonic's
4 MiB default is the ceiling on anything that crosses in one message. The
single-ticket predicate guard added on this branch (`MAX_PUSHED_PREDICATE_BYTES`)
never fired during those runs, so the oversized message is aggregated elsewhere
on the wire; the plan codec JSON-encodes an entire `scan_tasks` vector, which
would multiply a per-task predicate. That is a lead, not a diagnosis, and it is
tracked in issue #432.

The shipped configuration does not reach any of this: same rig, same data, cap
at the default, 13/13 with zero refusals.
