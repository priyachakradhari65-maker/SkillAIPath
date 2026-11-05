{# Override DBT's default schema naming to use custom schema names directly #}
{# Without this macro, DBT adds underscore prefix when target.schema is empty #}
{# This ensures: staging -> staging (not _staging), silver -> silver, etc. #}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}