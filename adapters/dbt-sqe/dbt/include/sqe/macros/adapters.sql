{# ── Metadata discovery ─────────────────────────────────────────────── #}

{% macro sqe__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations', fetch_result=True) %}
    SELECT
      table_catalog AS "database",
      table_schema AS "schema",
      table_name AS "name",
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
    WHERE catalog_name = '{{ database }}'
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

{% macro sqe__create_table_as(temporary, relation, compiled_code) %}
  CREATE OR REPLACE TABLE {{ relation }}
  AS (
    {{ compiled_code }}
  )
{% endmacro %}

{% macro sqe__create_view_as(relation, sql) %}
  CREATE OR REPLACE VIEW {{ relation }}
  AS (
    {{ sql }}
  )
{% endmacro %}

{% macro sqe__drop_relation(relation) %}
  {% if relation.type == 'view' %}
    DROP VIEW IF EXISTS {{ relation }}
  {% else %}
    DROP TABLE IF EXISTS {{ relation }}
  {% endif %}
{% endmacro %}

{% macro sqe__rename_relation(from_relation, to_relation) %}
  ALTER TABLE {{ from_relation }} RENAME TO {{ to_relation }}
{% endmacro %}

{% macro sqe__create_schema(relation) %}
  CREATE SCHEMA IF NOT EXISTS {{ relation.without_identifier() }}
{% endmacro %}

{% macro sqe__drop_schema(relation) %}
  DROP SCHEMA IF EXISTS {{ relation.without_identifier() }}
{% endmacro %}

{% macro sqe__current_timestamp() %}
  now()
{% endmacro %}

{% macro sqe__make_temp_relation(base_relation, suffix) %}
  {% set tmp_identifier = base_relation.identifier ~ suffix %}
  {% do return(base_relation.incorporate(path={"identifier": tmp_identifier})) %}
{% endmacro %}
