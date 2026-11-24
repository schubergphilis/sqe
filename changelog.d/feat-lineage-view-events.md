### Added

- OpenLineage events for CREATE VIEW and DROP VIEW

    CREATE VIEW now names the view as its output, with `lifecycleStateChange` `CREATE` (or `OVERWRITE` for CREATE OR REPLACE VIEW), the tables the view body reads as inputs, and the body's schema and column lineage. DROP VIEW names the view with `lifecycleStateChange` `DROP` and no inputs. Both carry the standard OpenLineage dataset facet `datasetType: {"datasetType": "VIEW"}`. The audit record of a CREATE VIEW now also lists the tables its body reads.
