{% macro generate_schema_name(custom_schema_name, node) %}
    {#
        Use explicit model/schema config as-is (ex: ref, mart) instead of
        prefixing with target.schema (DuckDB default: main).
    #}
    {% if custom_schema_name is none %}
        {{ target.schema }}
    {% else %}
        {{ custom_schema_name | trim }}
    {% endif %}
{% endmacro %}