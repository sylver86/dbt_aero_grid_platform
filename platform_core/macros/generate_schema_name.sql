{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {# Se non hai definito uno schema nel dbt_project o nel modello, usa quello del profilo #}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}

    {# Se invece lo hai definito, concatena: dataset_del_profilo + nome_custom #}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
