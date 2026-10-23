{% materialization incremental, adapter='sqe' %}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set strategy = config.get('incremental_strategy', 'append') -%}
  {%- set unique_key = config.get('unique_key') -%}
  {#- dbt takes one column name or a list of them. -#}
  {%- set unique_keys = [unique_key] if unique_key is string else unique_key -%}

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
      {# DELETE and INSERT commit together: readers never see the deleted
         keys missing, and a failed INSERT rolls the DELETE back. The temp
         table's CTAS and DROP autocommit, so they stay outside. #}
      {% do adapter.begin_transaction() %}
      {% call statement('delete') %}
        {% if unique_keys | length == 1 %}
        DELETE FROM {{ this }}
        WHERE {{ unique_keys[0] }} IN (
          SELECT {{ unique_keys[0] }} FROM {{ tmp_relation }}
        )
        {% else %}
        {# DELETE takes no correlated subquery; MERGE matches on every key. #}
        MERGE INTO {{ this }} AS target
        USING {{ tmp_relation }} AS source
        ON {% for k in unique_keys %}target.{{ k }} = source.{{ k }}{% if not loop.last %} AND {% endif %}{% endfor %}
        WHEN MATCHED THEN DELETE
        {% endif %}
      {% endcall %}
      {# dbt fails a model that never runs statement('main'). #}
      {% call statement('main') %}
        INSERT INTO {{ this }}
        (SELECT * FROM {{ tmp_relation }})
      {% endcall %}
      {% do adapter.commit_transaction() %}
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
        ON {% for k in unique_keys %}target.{{ k }} = source.{{ k }}{% if not loop.last %} AND {% endif %}{% endfor %}
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
