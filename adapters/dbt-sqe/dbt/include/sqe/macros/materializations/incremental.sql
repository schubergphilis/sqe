{% materialization incremental, adapter='sqe' %}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set strategy = config.get('incremental_strategy', 'append') -%}
  {%- set unique_key = config.get('unique_key') -%}

  {% if existing_relation is none %}
    {# First run — create the table #}
    {% call statement('main') %}
      {{ sqe__create_table_as(False, this, compiled_code) }}
    {% endcall %}
  {% else %}
    {# Incremental run #}
    {% if strategy == 'append' %}
      {% call statement('main') %}
        INSERT INTO {{ this }}
        ({{ compiled_code }})
      {% endcall %}

    {% elif strategy == 'delete+insert' %}
      {% if unique_key is none %}
        {{ exceptions.raise_compiler_error("delete+insert strategy requires a unique_key") }}
      {% endif %}
      {% set tmp_relation = make_temp_relation(this) %}
      {% call statement('create_tmp') %}
        {{ sqe__create_table_as(False, tmp_relation, compiled_code) }}
      {% endcall %}
      {% call statement('delete') %}
        DELETE FROM {{ this }}
        WHERE {{ unique_key }} IN (
          SELECT {{ unique_key }} FROM {{ tmp_relation }}
        )
      {% endcall %}
      {% call statement('insert') %}
        INSERT INTO {{ this }}
        (SELECT * FROM {{ tmp_relation }})
      {% endcall %}
      {{ adapter.drop_relation(tmp_relation) }}

    {% elif strategy == 'merge' %}
      {% if unique_key is none %}
        {{ exceptions.raise_compiler_error("merge strategy requires a unique_key") }}
      {% endif %}
      {% set dest_columns = adapter.get_columns_in_relation(this) %}
      {% set merge_update_columns = dest_columns | map(attribute='name') | list %}
      {% call statement('main') %}
        MERGE INTO {{ this }} AS target
        USING ({{ compiled_code }}) AS source
        ON target.{{ unique_key }} = source.{{ unique_key }}
        WHEN MATCHED THEN UPDATE SET
          {% for col in merge_update_columns %}
            target.{{ col }} = source.{{ col }}{% if not loop.last %},{% endif %}
          {% endfor %}
        WHEN NOT MATCHED THEN INSERT (
          {% for col in merge_update_columns %}
            {{ col }}{% if not loop.last %},{% endif %}
          {% endfor %}
        ) VALUES (
          {% for col in merge_update_columns %}
            source.{{ col }}{% if not loop.last %},{% endif %}
          {% endfor %}
        )
      {% endcall %}
    {% else %}
      {{ exceptions.raise_compiler_error("Invalid incremental strategy: " ~ strategy) }}
    {% endif %}
  {% endif %}

  {{ return({'relations': [this]}) }}
{% endmaterialization %}
