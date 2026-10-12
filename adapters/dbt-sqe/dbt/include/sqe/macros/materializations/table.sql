{% materialization table, adapter='sqe' %}
  {%- set existing_relation = load_cached_relation(this) -%}

  {# CREATE OR REPLACE swaps a table in one commit; dropping it first would
     leave no table if the CTAS fails. Only a view in the way is dropped. #}
  {% if existing_relation is not none and existing_relation.type != 'table' %}
    {{ adapter.drop_relation(existing_relation) }}
  {% endif %}

  {% call statement('main') %}
    {{ sqe__create_table_as(False, this, compiled_code) }}
  {% endcall %}

  {{ return({'relations': [this]}) }}
{% endmaterialization %}
