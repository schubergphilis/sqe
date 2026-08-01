---
title: "The mask that was never a mask"
description: "Access control was the last subsystem whose end-to-end behaviour we checked by grepping CLI output. The denial test matched the string 'not found', which is also what a typo'd table name prints. We replaced that harness with twenty assertions on decoded Arrow values against a live Apache Ranger, and the exercise found a feature that had never once worked: every tag-based column mask had been restricting the column instead of masking it since the day we shipped it. Two of the new tests then passed for the wrong reason, and only mutation caught them. Here is what fine-grained access control in SQE actually does, and how we know."
pubDate: "2026-08-01"
author: "Jacob Verhoeks"
tags:
  - "security"
  - "ranger"
  - "iceberg"
  - "testing"
---

*August 1, 2026*

Access control in SQE has two axes. Apache Polaris gates the catalog: who may
see a table at all, driven by `GRANT` statements that SQE translates into
Apache Ranger policies. SQE gates the data: row filters and column masks
applied by rewriting the logical plan before DataFusion optimizes it, so the
optimizer cannot push a user predicate through a mask to probe raw values.

Both halves were shipped. Both had tests. The catalog half was checked by a
shell script that grepped CLI output.

That script's denial check matched `not found`. A typo'd table name prints
`not found` too. Its mask check asserted the absence of digits, and a column
the engine refuses to return has no digits either. A test that cannot tell
"correctly hidden" from "wrong identifier" is not a security test. It is a
smoke alarm wired to the light switch.

## What the rewrite is for

We replaced it with twenty cases in the Rust integration tier, driving an
in-process query handler wired to the real Ranger enforcer and the real Ranger
grant backend, authenticating four users through Keycloak, asserting decoded
Arrow values.

One helper does the load-bearing work. Every denial is proven by first running
the identical SQL as an admin who is allowed to see the rows. If the admin's
query fails, the test fails as a broken control rather than reporting a
successful deny. Denied and invalid stop being the same observation.

## The feature that had never worked

Tag-based masking is two systems wearing one name. A rule in Ranger says what
a tag means. An association in the Iceberg table property says which columns
carry it. We wrote about that split a month ago, and the tests behind it were
green.

They were green against a fake tag source.

Against a live Ranger, every tag mask fell through to the unsupported arm.
Ranger's `tag` service definition does not define bare mask names. It
aggregates the mask types of every component it can decorate, so the entries
read `hive:MASK_SHOW_LAST_4`, `trino:MASK_NULL`, and so on. Our mapper matched
bare names. Nothing matched, ever.

The saving grace is that the unmapped path is fail-closed: the tagged column
was restricted instead of masked. No SSN ever leaked. But a column that
returns nothing has no digits in it, which is exactly what the old harness
checked for, so the alarm stayed quiet from the day the feature shipped
through every release since.

The fix is nine lines. Finding it took a test that looked at the value.

## Tag row filters, and a flag that reads like a syntax error

A tag policy can carry a row filter too, so one rule filters every table with
a column tagged `PII`. Ours was rejected:

```
tag policy can specify values for one of the following resource sets:
 does not have any resource hierarchies
```

That message names resources, so we spent a while looking at the resource
block. The resource block was fine. Ranger copies each component's
`dataMaskDef` into the tag service definition unconditionally, but copies its
`rowFilterDef` only when Ranger Admin runs with
`ranger.servicedef.autopropagate.rowfilterdef.to.tag=true`. The default is
false. No upgrade changes it.

Set the property and tag row filters work end to end. The test now asserts
that a tagged table returns exactly the two EU rows for one user and all three
for another.

## Two tests that passed for the wrong reason

The interesting failures were ours.

The first was a policy-breaker test: kill Ranger, prove the engine denies
instead of serving unfiltered data. It passed immediately, which should have
been suspicious. A handler pointed at a dead Ranger returned zero rows. So did
a handler pointed at a healthy one. The deny was real but it came from
somewhere else entirely: the second handler's metadata cache had never seen the
table, tag state read as unknown, and unknown denies by contract. The outage
was not being tested at all.

The second was a cache-TTL test, and it took three attempts. SQE caches the
resolved policy, so a mask authored in the Ranger console is not honored until
that entry expires. Draft one took its measurement half a second after seeding
the cache, so it held for any TTL of a second or more. Draft two started its
clock when a warm-up loop succeeded rather than when the cache entry was
written, and since a cold handler can fail closed for a minute, that loop could
succeed on a cached entry already most of a TTL old. It presented as a flake:
green once, then red twice on identical configuration.

Mutation testing caught both. Break the thing the test claims to check, and
watch whether it goes red. Neither draft did.

What actually diagnosed the second one was a log timeline. Six cache misses,
two of them exactly thirty seconds apart, which located the insert the clock
should have been anchored to. The mutation only ever said "something here
depends on this value". The log said which entry was cached when.

## What works, and what to know before deploying

Resource column masks and row filters work, share one Ranger `hive` service
with Spark, and produce byte-identical output in both engines. The hash mask is
a keyed HMAC, asserted against a digest computed outside the engine so the
implementation is not checking itself. Grants, revokes, role and user grants,
deny precedence, and write privileges separate from read are all covered.

Tag masks and tag row filters work, with the Ranger property above.

Three things to know. Console-authored edits are honored on a delay bounded by
`cache-ttl-secs`, an over-permissive window that the TTL test now pins at both
edges; grants issued through SQE flush the cache and skip it. Every lookup
failure denies rather than degrading: Ranger unreachable, tag state unknown,
mask type unmappable. And tag associations do not reach Spark, which reads them
from the Ranger or Atlas tag store rather than from Iceberg metadata.

The wider lesson is not about Ranger. Three of the four defects in this batch
were invisible to a passing test suite, and two of those tests were ones we had
just written to look for exactly that class of bug. Fail-closed design is what
kept a green suite from becoming a leak. It is not what should have caught it.

Run it with `make test-access-control`.
