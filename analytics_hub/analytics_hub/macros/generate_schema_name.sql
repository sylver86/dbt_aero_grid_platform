-- macros/generate_schema_name.sql di ANALYTICS_HUB
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {#
        Modelli di platform_core: punta ai dataset di produzione

        node.package_name è una proprietà del singolo modello che dbt sta compilando in quel momento.
        Indica esattamente "questo modello appartiene a quale progetto/package?".

        Quindi se dbt sta compilando fct_turbine_telemetry, node.package_name sarà 'platform_core'.
        Se sta compilando test_core_link, sarà 'analytics_hub'

        project_name è una variabile globale che ti dice "da quale progetto ho lanciato il comando dbt run?".
        Se viene eseguito dbt run dalla cartella di analytics_hub, project_name sarà sempre 'analytics_hub',
        indipendentemente da quale modello sta compilando.

    #}
    {%- if node.package_name == 'platform_core' -%}
        {%- if custom_schema_name is not none -%}
            prod_aero_grid_platform_{{ custom_schema_name | trim }}
        {%- else -%}
            prod_aero_grid_platform
        {%- endif -%}

    {# Modelli di analytics_hub: comportamento standard #}
    {%- elif custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
