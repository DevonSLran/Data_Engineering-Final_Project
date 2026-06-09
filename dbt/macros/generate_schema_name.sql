{#
  Use the model's configured `+schema` (e.g. "staging", "marts") as the literal
  schema name instead of dbt's default "<target>_<custom>" concatenation.
  Models with no custom schema fall back to the profile's target schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
