# Preface: The Sovereignty Thesis {#sec:preface}

> "The general who wins a battle makes many calculations in his temple
> before the battle is fought. The general who loses a battle makes
> but few calculations beforehand."
> — Sun Tzu, *The Art of War*

We had a Trino cluster. It worked. Mostly.

It ran our analytical queries against Iceberg tables in S3, served dashboards for the data team, and powered the nightly dbt pipeline that turned raw event data into something the business could actually read. We had it deployed on Kubernetes with a Helm chart we'd wrestled into shape over six months. We knew its quirks. We knew which config knobs to turn when queries got slow and which ones to leave alone because they'd been set by someone who'd left the company.

Then one morning the security team asked a simple question: "Who accessed the customer table last Tuesday?"

We couldn't answer it.

Not because we hadn't thought about auditing. We had. The problem was more fundamental. Every query that hit S3 came from the same identity: the Trino service account. When Alice queried customer data, it was the service account that read the files. When Bob ran an aggregation over financial records, same service account. When the dbt pipeline transformed everything at 2am, same service account again.

From S3's perspective, from CloudTrail's perspective, from the security team's perspective — it was all one actor. The Trino service account did everything and everyone did nothing.

That was the moment the idea started forming.


## The Road Here

This book didn't start with Rust. It started with frustration.

For years I worked in the AWS data ecosystem — Glue, Snowflake, managed Spark, the full vendor stack. I wrote about it on [dev.to](https://dev.to/jverhoeks), tracking each step of the journey in real time. The early articles are about Glue custom connectors and Snowflake integrations. They work. They're fine. They're also completely dependent on vendor decisions.

Then Apache Iceberg changed the game. In late 2024, I started experimenting with Iceberg REST APIs — first through AWS Glue, then through Databricks Unity Catalog, then through DuckDB connecting to S3 Iceberg tables directly. Each experiment peeled back a layer. The table format was open. The data files were in your S3 bucket. The only thing still proprietary was the catalog — the thing that tells you which tables exist and where.

When Apache Polaris appeared — a pure Iceberg REST catalog with no opinions about your query engine, your governance layer, or your cloud provider — the last proprietary piece fell away. For the first time, you could have a fully open data stack: open table format (Iceberg), open catalog protocol (REST), open storage (S3-compatible), and... what query engine?

DuckDB was single-node and embedded. Spark was a cluster framework, not a query engine. Trino couldn't pass user credentials through to storage. Every option had a gap.

DataFusion had none of these gaps — because it wasn't a product. It was a library. A Rust library that gave you a complete query engine in a single function call: parse SQL, plan it, optimise it, execute it, return Arrow batches. Everything else — auth, catalog, storage, distribution — was your problem.

That's when SQE started.


## Why Build a SQL Engine at All?

People asked. A lot.

The first answer is simple: **because we can.** A team that builds a query engine from scratch develops an understanding of query planning, execution, and distribution that a team running a managed service never acquires. The exercise itself makes you better at operating *any* data infrastructure.

The second answer is more interesting: **to challenge ourselves to build the right tool.** Not the most general tool. Not the most feature-complete tool. The right one — for our security model, our data architecture, our operational constraints. Building SQE forced us to articulate what we actually needed, rather than accepting what was available.

The third answer only becomes clear in hindsight: **because the tools that exist don't match.** Trino's auth model is incompatible with zero-trust. Spark is too heavy for interactive queries. DuckDB is single-node. DataFusion is a library, not a product. The gap between "library" and "product" is exactly the gap this book fills.


## The Open-Source Goal

SQE is built to be open-sourced. This isn't an afterthought or a marketing strategy. It's a design constraint. Every architectural decision in this book was made with the assumption that the code would be public, that strangers would read it, and that organisations with different security models, different catalogs, and different cloud providers would try to run it. That constraint shaped everything: pluggable auth (you shouldn't need our OIDC provider), pluggable catalog (you shouldn't need Polaris), pluggable policy (you shouldn't need OPA or Cedar), configurable everything (if we hardcoded it, you'd have to fork it).

A sovereign engine that only works in one environment isn't sovereign. It's just private. The goal is an engine that works in *any* environment — because the operator controls the configuration, not the developer. Open-sourcing a query engine also means something specific: the code becomes the documentation. Every trait, every config key, every error message is part of the public API. This book exists, in part, to bridge the gap between "here's the code" and "here's why the code is this way."


## What Sovereignty Means

::: {.sovereignty}
Sovereignty, in the context of data infrastructure, is a precise claim: every component in the pipeline runs under your control, with your policies, using credentials that trace back to an individual human. No shared secrets. No ambient authority. No "the engine has access to everything and we trust it to enforce the rules."
:::

The requirements, once we wrote them down, were almost comically simple:

1. Every query runs as the authenticated user
2. The user's identity propagates to every system the query touches
3. Policy enforcement happens inside the engine, not around it
4. The engine has no ambient credentials
5. The audit trail shows which human accessed which data
6. The engine runs on your infrastructure, under your control
7. The source is open, the config is yours, the data never leaves

We looked at every major query engine. None had this model completely. The authentication model is so deeply embedded in query engine architecture that you can't bolt it on. You have to build it in from the first line of code.


## Connection to *The Art of Agents*

This book is a companion to *The Art of Agents: Building Agentic AI Systems That Think Before They Code*. That book presents 13 principles for building agentic systems, structured around Sun Tzu's *Art of War*. Several of those principles shaped how we built SQE — particularly the Five Constants (Contract, Context, Terrain, Model, Protocol), which map directly onto the SQL standard, Iceberg metadata, the catalog landscape, DataFusion, and the OIDC + Flight SQL protocol stack. You don't need to have read *The Art of Agents* to follow this book, but if you have, you'll recognise the patterns.


## How to Read This Book

Every chapter in this book is a problem being solved. Some problems are architectural ("how do we pass user identity through to S3?"). Some are operational ("what happens when a worker dies mid-query?"). Some are existential ("should we build this at all?"). But they're all problems, and the book follows the shape of solving them — what's in the way, what we tried, what worked, what we learned.

If you're the kind of person who reads technical books to see how someone else thought through a hard problem, you're the audience. If you're looking for API documentation, that's in `docs/book`.

**Part I (Chapters 0--2)** is the problem that started everything. The catalog landscape, the Iceberg stack, the question of whether to build at all.

**Part II (Chapters 3--6)** is the first real challenge: a working single-node engine. DataFusion, auth, Flight SQL, catalog integration. Each chapter is a door we had to get through.

**Part III (Chapters 7--10)** is where it gets interesting. Writes, security policy as plan rewriting, observability, configurability. Each chapter is an obstacle we didn't fully anticipate.

**Part IV (Chapters 11--14)** is the hard part. Distributed execution. Ballista. The load test that broke everything. This is where the most dead ends are.

**Part V (Chapters 15--17)** is the honest accounting. Deployment, benchmarks, and what we'd do differently — including the frank assessment of what AI-assisted development actually looks like in practice.


## Writing While Building

This book is being written at the same time as SQE is being built. That's deliberate. Most technical books are written after the fact — the author finishes the project, then reconstructs the decisions from memory. The decisions get cleaner in hindsight. The wrong turns get edited out. The confusion gets smoothed over. This book doesn't do that. Each chapter is written close to when the feature was implemented. The frustration is fresh. The wrong turns are still in the git log. The design decisions haven't been retroactively rationalised into inevitability.

The numbers tell the story: 316 commits (across all branches) in 15 days. From initial commit to distributed execution with a concurrent load test. From zero crates to a benchmark suite running TPC-H, TPC-DS, ClickBench, SSB, TPC-E, TPC-BB, and TPC-C. Writing two books and building an engine simultaneously is either very efficient or very foolish. Ask me again when they're all finished.


## The Running Code

This book's running code is the SQE repository itself. Each chapter references specific crates, modules, and tests. Tagged commits mark the engine at each stage.

You don't need to check out tags to follow the book. The current `main` branch contains everything. But if you want to see the engine in a simpler state, the tags are there.


## Acknowledgements

SQE exists because of the DataFusion community. The quality of DataFusion as a library — its extensibility, its performance, its documentation — made it possible for a small team to build a production query engine in months rather than years.

The iceberg-rust maintainers built the table format library that handles the Iceberg spec so we don't have to. The Polaris team at Snowflake open-sourced the catalog that proved REST-based table discovery works.

Rafael Herrero has been the other half of this project from the start. While I was deep in the query engine internals — DataFusion plans, Arrow batches, Iceberg commits — Rafael was building the deployment and operational layer that makes SQE actually runnable in production. The Kubernetes deployment, the Helm operator, the security hardening, the network policies, the mTLS configuration between coordinator and workers — that's Rafael's work. He's the person who took a binary that runs on a laptop and turned it into something that deploys securely across namespaces with proper RBAC, pod security standards, and automated rollouts. Many of the architectural decisions in this book — pluggable auth, TLS everywhere, the separation between coordinator and worker configs — exist because Rafael was asking the right deployment questions while I was writing Rust. Building a query engine is one thing. Operating it at the security standard Schuberg Philis demands is another, and that's where Rafael's expertise shaped the project.

The VPF Data & AI team at Schuberg Philis ran the queries, filed the bugs, and never once asked "why don't we just use Trino?" after the first week.

And to everyone who looked at this project and asked "why would you build a SQL engine?" — this book is the answer.

---

*Jacob Verhoeks*
*March 2026*
