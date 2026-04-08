{% macro sqe__load_csv_rows(model, agate_table) %}
  {% set batch_size = 1000 %}
  {% set cols = agate_table.column_names %}
  {% set col_list = cols | join(', ') %}

  {% for batch_start in range(0, agate_table.rows | length, batch_size) %}
    {% set batch_end = [batch_start + batch_size, agate_table.rows | length] | min %}
    {% call statement('seed_batch_' ~ loop.index) %}
      INSERT INTO {{ this }} ({{ col_list }})
      VALUES
      {% for row_idx in range(batch_start, batch_end) %}
        {% set row = agate_table.rows[row_idx] %}
        ({% for value in row %}
          {% if value is none %}NULL
          {% elif value is number %}{{ value }}
          {% elif value is string %}'{{ value | replace("'", "''") }}'
          {% else %}'{{ value }}'
          {% endif %}
          {% if not loop.last %},{% endif %}
        {% endfor %}){% if row_idx < batch_end - 1 %},{% endif %}
      {% endfor %}
    {% endcall %}
  {% endfor %}
{% endmacro %}

{% macro sqe__create_csv_table(model, agate_table) %}
  {% set column_override = config.get('column_types', {}) %}
  {% set cols %}
    {% for col_name in agate_table.column_names %}
      {% set col_type = column_override.get(col_name, adapter.convert_type(agate_table, loop.index0)) %}
      {{ col_name }} {{ col_type }}{% if not loop.last %},{% endif %}
    {% endfor %}
  {% endset %}

  {% call statement('create_seed_table') %}
    CREATE TABLE IF NOT EXISTS {{ this }} ({{ cols }})
  {% endcall %}
{% endmacro %}
