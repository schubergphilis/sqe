{% macro sqe__get_catalog(information_schema, schemas) %}
  {% call statement('catalog', fetch_result=True) %}
    SELECT
      table_catalog AS "table_database",
      table_schema AS "table_schema",
      table_name AS "table_name",
      table_type AS "table_type",
      NULL AS "table_comment",
      column_name AS "column_name",
      ordinal_position AS "column_index",
      data_type AS "column_type",
      NULL AS "column_comment"
    FROM information_schema.columns
    WHERE table_schema IN (
      {% for schema in schemas %}
        '{{ schema }}'{% if not loop.last %},{% endif %}
      {% endfor %}
    )
    ORDER BY table_schema, table_name, ordinal_position
  {% endcall %}
  {{ return(load_result('catalog').table) }}
{% endmacro %}
