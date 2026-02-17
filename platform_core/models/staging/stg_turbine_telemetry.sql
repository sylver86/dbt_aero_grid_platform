
{{
    config(
            materialized='incremental',
            incremental_strategy='merge',
            unique_key='telemetry_key'
        )
}}



with source as (
    select * from {{ source('external_data_raw', 'raw_turbine_telemetry') }}
),

-- 1. Prima puliamo e rinominiamo
renamed as (
    select
        cast(timestamp as TIMESTAMP) as measurement_at,
        trim(turbine_id) as turbine_id,
        wind_speed_ms,
        case when rpm = -999.0 then null else rpm end as rpm,
        power_output_kw_ as power_output_kw,
        temperature_c,
        vibration_index
    from source
    where temperature_c <= 100
),

-- 2. Poi applichiamo logiche extra e chiavi univoche
deduplicated as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['turbine_id', 'measurement_at']) }} as telemetry_key,   /* Definiamo una chiave primaria. */
        *
    from renamed
    where vibration_index between 0 and 5 -- Logica di business specifica
    {% if is_incremental() %}
        and measurement_at > ( select max(measurement_at) from {{ this }} )
    {% endif %}
)

select * from deduplicated
