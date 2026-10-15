{# 
    Override dbt's default schema name generation.
    By default, dbt concatenates <target_schema>_<custom_schema> (e.g. gold_gold or default_gold).
    This macro ensures that when custom schema 'gold' is specified, 
    the schema name is strictly 'gold' and never prefixed or duplicated.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
