{#
  Configurazione del modello incrementale per BigQuery:
  - Partizionamento giornaliero per ottimizzare i costi di scansione
  - Clustering per turbine_id per velocizzare i filtri nelle dashboard
#}

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='telemetry_key',
        partition_by={
            "field": "measurement_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['turbine_id']
    )
}}

with source as (
    select * from {{ ref('stg_turbine_telemetry') }}
),

-- 1. Pulizia, rinomina e conversione tramite Macro
incremental as (
    select *,
    {{ kw_to_mw('power_output_kw') }} as power_output_mw
    from source

    {% if is_incremental() %}
        -- Filtro per caricare solo i nuovi dati basandosi sull'ultimo timestamp presente
        where measurement_at >= ( select max(measurement_at) from {{ this }} )
    {% endif %}
)

select * from incremental
