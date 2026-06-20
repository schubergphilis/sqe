---
title: "Lake Formation gates the catalog, not the rows"
description: "Our own README promised fine-grained Lake Formation in the SQE Glue quickstart. The engine does not do it. Here is what Lake Formation actually enforces when SQE reads S3 directly, and where SQE's real column and row masking lives."
pubDate: "2026-06-07"
author: "Jacob Verhoeks"
tags:
  - "lake-formation"
  - "glue"
  - "security"
  - "iceberg"
  - "aws"
---



*June 7, 2026*

We built a quickstart for AWS Glue with Lake Formation. Our own README, the one for the plain Glue quickstart, promised it: "the dedicated glue-lake-formation quickstart demonstrates explicit fine-grained LF grants."

Then I went to build the thing and checked what the engine actually does.

It does not do fine-grained Lake Formation. Not column masking, not row filtering. I had written a promise the engine could not keep.

This is the post about catching that, and about what Lake Formation actually does when SQE is the engine.

## What I expected to find

Lake Formation's pitch is fine-grained access on top of Glue. You grant a principal SELECT on three of a table's ten columns, or a row filter that hides everything outside their region, and the query engine sees only what the grant allows. Athena does this. Redshift Spectrum does this. The enforcement is real because those engines ask Lake Formation for the data and Lake Formation hands back a filtered view.

The mechanism is credential vending. The engine calls `GetUnfilteredTableMetadata` and a temporary-credentials API, Lake Formation returns scoped credentials plus the column and row rules, and the engine applies them. The engine never sees the raw files. It only gets what LF vends.

I assumed SQE plugged into that.

## What the engine actually does

`grep -ril "GetUnfilteredTable" crates/` returns nothing. So does every variant I tried: credential vending, cell filters, data-cell. The only hit for "lake formation" in the whole crate tree is a comment in a test, noting a federated endpoint we do not support.

SQE's Glue backend reads Iceberg metadata through the Glue API and then reads the data files straight from S3, with the caller's own AWS credentials. It never asks Lake Formation for a filtered view. It cannot, because it never goes through the vending path. It has the bytes the moment it has S3 read access.

That is the whole answer. An engine that reads storage directly cannot enforce a filter that lives in the credential-vending layer it skips.

## What it gates instead

The catalog. Not the data.

Every Glue API call SQE makes (GetDatabase, GetTable, CreateTable) goes through Lake Formation's permission check. So LF still controls a real boundary: whether SQE can see that a table exists, describe it, or create one. It just does not reach inside the rows.

The quickstart shows exactly that boundary. CloudFormation creates the database. In a Lake-Formation-enabled account a database made that way is governed with no grants, so the first thing SQE tries fails:

```
AccessDeniedException: Insufficient Lake Formation permission(s):
Required Create Table on sqe_lf_quickstart
```

Then we grant the principal permissions on the database:

```bash
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::...:user/jacobadmin \
  --resource '{"Database":{"Name":"sqe_lf_quickstart"}}' \
  --permissions CREATE_TABLE ALTER DROP DESCRIBE
```

The same statement now runs. CREATE TABLE, INSERT, SELECT, four rows back. The grant is the entire difference between denied and allowed. That is Lake Formation doing its job at the level it can reach with this engine.

## Where fine-grained actually lives

SQE does have column masking and row filters. They are not Lake Formation's.

The enforcement happens in the engine, by rewriting the logical plan before the optimizer runs. A policy says "mask this column for this role" or "rows where region matches the caller," and SQE injects the mask expression and the filter node above the table scan. The backend for those policies is pluggable: OPA with Rego, or Cedar. The catalog is not involved.

The two systems answer different questions. Lake Formation answers whether a principal can touch a table. The policy engine answers which columns and rows of that table the principal gets to see. Conflating them is how I ended up promising LF fine-grained in a README.

## What changed

I corrected the Glue quickstart's README and the comment in its CDK stack. They now say table and database-level gating, with a line stating plainly that SQE does not enforce LF column or row masking, and that the OPA/Cedar engine is the path for that.

The glue-lake-formation quickstart ships as the honest version: the denied-then-granted arc, captured from a live run against a real account, with a "what this shows and what it does not" section at the top.

## The lesson

A README is a promise. It is easy to write one the engine cannot keep, especially for a managed service whose marketing describes a capability your integration does not use. The fix is boring and it works: before you document a capability, grep the engine for the API that would implement it. If the call is not there, the capability is not there, whatever the upstream service can do on its own.

Lake Formation is a good authorization boundary for SQE. It is not a row filter. Those are two different sentences, and the quickstart now says both.
