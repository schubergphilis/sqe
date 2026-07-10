{% materialization view, adapter='sqe' %}
  {%- set existing_relation = load_cached_relation(this) -%}

  {% if existing_relation is not none and existing_relation.type != 'view' %}
    {{ adapter.drop_relation(existing_relation) }}
  {% endif %}

  {% call statement('main') %}
    {{ sqe__create_view_as(this, compiled_code) }}
  {% endcall %}

  {{ return({'relations': [this]}) }}
{% endmaterialization %}
