{# ── Metadata discovery ─────────────────────────────────────────────── #}

{# SQLAdapter.list_relations_without_caching unpacks rows positionally as
   (database, name, schema, type); any other order swaps name and schema and
   no existing relation is ever found. #}
{% macro sqe__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations', fetch_result=True) %}
    SELECT
      table_catalog AS "database",
      table_name AS "name",
      table_schema AS "schema",
      CASE table_type
        WHEN 'BASE TABLE' THEN 'table'
        WHEN 'VIEW' THEN 'view'
        ELSE 'table'
      END AS "type"
    FROM information_schema.tables
    WHERE table_schema = '{{ schema_relation.schema }}'
      AND table_catalog = '{{ schema_relation.database }}'
  {% endcall %}
  {{ return(load_result('list_relations').table) }}
{% endmacro %}

{% macro sqe__get_columns_in_relation(relation) %}
  {% call statement('get_columns', fetch_result=True) %}
    SELECT
      column_name,
      data_type,
      character_maximum_length,
      numeric_precision,
      numeric_scale
    FROM information_schema.columns
    WHERE table_name = '{{ relation.identifier }}'
      AND table_schema = '{{ relation.schema }}'
      AND table_catalog = '{{ relation.database }}'
    ORDER BY ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro sqe__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True) %}
    SELECT DISTINCT schema_name
    FROM information_schema.schemata
    WHERE catalog_name = '{{ database | replace('"', '') }}'
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro sqe__check_schema_exists(information_schema, schema) %}
  {% call statement('check_schema', fetch_result=True) %}
    SELECT COUNT(*) AS num_schemas
    FROM information_schema.schemata
    WHERE catalog_name = '{{ information_schema.database }}'
      AND schema_name = '{{ schema }}'
  {% endcall %}
  {{ return(load_result('check_schema').table) }}
{% endmacro %}

{# ── DDL generation ──────────────────────────────────────────────────── #}

{# `temporary` marks a scratch table (the delete+insert staging table): it
   takes no model properties, so a write mode meant for the target never
   applies to it. #}
{% macro sqe__create_table_as(temporary, relation, compiled_code) %}
  CREATE OR REPLACE TABLE {{ relation }}
  {%- if not temporary %}{{ sqe__tblproperties_clause() }}{% endif %}
  AS (
    {{ compiled_code }}
  )
{% endmacro %}

{# The model's `table_properties` config (alias `tblproperties`, the
   dbt-spark name) as Iceberg table properties, for example
   {'write.merge.mode': 'merge-on-read'}. Applied when the table is created
   or replaced: an incremental table takes new values on --full-refresh. #}
{% macro sqe__tblproperties_clause() %}
  {%- set props = config.get('table_properties') or config.get('tblproperties') -%}
  {%- if props -%}
    {%- if props is not mapping -%}
      {{ exceptions.raise_compiler_error("table_properties must be a mapping of property name to value, got: " ~ props) }}
    {%- endif %}
  TBLPROPERTIES (
    {%- for key, value in props.items() %}
    {#- YAML `true` is a Python bool; Iceberg reads 'true', not 'True'. #}
    {%- set value = (value | string | lower) if value is boolean else (value | string) %}
    '{{ key | replace("'", "''") }}' = '{{ value | replace("'", "''") }}'{% if not loop.last %},{% endif %}
    {%- endfor %}
  )
  {%- endif -%}
{% endmacro %}

{% macro sqe__create_view_as(relation, sql) %}
  CREATE OR REPLACE VIEW {{ relation }}
  AS (
    {{ sql }}
  )
{% endmacro %}

{# The adapter calls these to run the DDL, not to render it: each must run its
   own statement, as the default__ macros do. A bare SQL body is discarded. #}
{% macro sqe__drop_relation(relation) %}
  {% call statement('drop_relation', auto_begin=False) %}
    {% if relation.type == 'view' %}
      DROP VIEW IF EXISTS {{ relation }}
    {% else %}
      DROP TABLE IF EXISTS {{ relation }}
    {% endif %}
  {% endcall %}
{% endmacro %}

{# A snapshot stages its changes in `<name>__dbt_tmp`, a real table here:
   SQE has no temporary tables, so it is dropped after the MERGE (#891). #}
{% macro sqe__post_snapshot(staging_relation) %}
  {{ adapter.drop_relation(staging_relation) }}
{% endmacro %}

{% macro sqe__rename_relation(from_relation, to_relation) %}
  {% call statement('rename_relation') %}
    ALTER TABLE {{ from_relation }} RENAME TO {{ to_relation }}
  {% endcall %}
{% endmacro %}

{% macro sqe__create_schema(relation) %}
  {% call statement('create_schema') %}
    CREATE SCHEMA IF NOT EXISTS {{ relation.without_identifier() }}
  {% endcall %}
{% endmacro %}

{% macro sqe__drop_schema(relation) %}
  {% call statement('drop_schema') %}
    DROP SCHEMA IF EXISTS {{ relation.without_identifier() }}
  {% endcall %}
{% endmacro %}

{% macro sqe__current_timestamp() %}
  now()
{% endmacro %}

{% macro sqe__make_temp_relation(base_relation, suffix) %}
  {% set tmp_identifier = base_relation.identifier ~ suffix %}
  {# Typed: the materializations create it as a table, and drop_relation
     refuses a relation whose type is null (`this` has none). #}
  {% do return(base_relation.incorporate(path={"identifier": tmp_identifier}, type='table')) %}
{% endmacro %}
