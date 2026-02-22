{{
    config(
        materialized='ephemeral'
    )
}}

Select
telemetry_key,
turbine_id,
measurement_at,
wind_speed_ms,
power_output_kw,
{{ elab_power_theoretical('wind_speed_ms') }} as theoretical_power_mw                               /* Creiamo tramite una macro UDF il campo dei power_kw teorici in base al vento rilevato */
from {{ ref('stg_turbine_telemetry') }}
