{{ config(materialized='view') }}

select * from {{ ref('platform_core', 'fct_turbine_telemetry') }}                                   /* Puntiamo al progetto platform_core e prendiamo la tabella fct_turbine_telemetrys */
limit 10
