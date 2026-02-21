{#
  Configurazione del modello incrementale per BigQuery:
  - Partizionamento giornaliero per ottimizzare i costi di scansione
  - Clustering per turbine_id per velocizzare i filtri nelle dashboard
#}

/* Definisco un modello incrementale con strategia 'merge' usando la chiave 'telemetry_key' inoltre partiziono la tabella in BigQuery per data giornaliera ordinandoli per cluster_id*/

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='telemetry_key',
        on_schema_change='fail',
        partition_by={
            "field": "measurement_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['turbine_id']
    )
}}

with source as (
    select * from {{ ref('int_turbine_performance_check') }}                                        /* Leggiamo da questo modello ephemeral che introduce il campo della potenza teorica associata al vento della turbina */
),

-- 1. Pulizia, rinomina e conversione tramite Macro
incremental as (
    select source.*,
    {{ kw_to_mw('power_output_kw') }} as power_output_mw                                            /* Utilizzo una macro che consente di convertire i kw in mw dei valori */
    from source

    {% if is_incremental() %}
        -- Filtro per caricare solo i nuovi dati basandosi sull'ultimo timestamp presente
        where measurement_at >= ( select max(measurement_at) from {{ this }} )
    {% endif %}
)

select * from incremental
