{% materialization table, adapter='sqe' %}
  {%- set existing_relation = load_cached_relation(this) -%}

  {% if existing_relation is not none %}
    {{ adapter.drop_relation(existing_relation) }}
  {% endif %}

  {% call statement('main') %}
    {{ sqe__create_table_as(False, this, compiled_code) }}
  {% endcall %}

  {{ return({'relations': [this]}) }}
{% endmaterialization %}
