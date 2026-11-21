{# information_schema.columns has no table_type; it comes from
   information_schema.tables. Selecting it from columns failed every
   `dbt docs generate` (#891). Scoped to the target catalog as well, so a
   schema of the same name in another catalog does not add its tables. #}
{% macro sqe__get_catalog(information_schema, schemas) %}
  {% call statement('catalog', fetch_result=True) %}
    SELECT
      c.table_catalog AS "table_database",
      c.table_schema AS "table_schema",
      c.table_name AS "table_name",
      CASE t.table_type
        WHEN 'VIEW' THEN 'view'
        ELSE 'table'
      END AS "table_type",
      NULL AS "table_comment",
      c.column_name AS "column_name",
      c.ordinal_position AS "column_index",
      c.data_type AS "column_type",
      NULL AS "column_comment"
    FROM {{ sqe__information_schema_view(information_schema.database, 'columns') }} c
    JOIN {{ sqe__information_schema_view(information_schema.database, 'tables') }} t
      ON t.table_catalog = c.table_catalog
     AND t.table_schema = c.table_schema
     AND t.table_name = c.table_name
    WHERE c.table_catalog = '{{ information_schema.database }}'
      AND c.table_schema IN (
      {% for schema in schemas %}
        '{{ schema }}'{% if not loop.last %},{% endif %}
      {% endfor %}
    )
    ORDER BY c.table_schema, c.table_name, c.ordinal_position
  {% endcall %}
  {{ return(load_result('catalog').table) }}
{% endmacro %}
