
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
    AND

    /* Usiamo dbt.dateadd cosi universalmente utilizziamo un linguaggio SQL per ogni ambiente  */
    /* In sostanza il secondo elemento è calcolato "sottraendo" perchè abbiamo inserito il - calcolo di interval e quindi dando riferimento al timestamp corrente  i 7 giorni  */
    /* telemetry_start_date -> Variabile progetto: Carico gli ultimi 'n' giorni  rispetto al giorno corrente*/
    timestamp >= cast({{ dbt.dateadd(
        datepart="day",
        interval=-var("telemetry_start_date"),
        from_date_or_timestamp="current_timestamp()"
    ) }} as TIMESTAMP)
),

-- 2. Poi applichiamo logiche extra e chiavi univoche
deduplicated as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['turbine_id', 'measurement_at']) }} as telemetry_key,   /* Definiamo una chiave primaria ovvero un hash univoco dato dalla coppia dei campi. Usiamo la funzione dbt_utils.generate_surrogate_key */
        *
    from renamed
    where vibration_index between 0 and 5 -- Logica di business specifica
    {% if is_incremental() %}                                                                       /* Se il modello è configurato con materialized='incremental' e non sto facendo un full refresh verifico che la data è maggiore all'ultima */
        and measurement_at > ( select max(measurement_at) from {{ this }} )                         /* This è un riferimento circolare alla tabella di destinazione (prima che la nuova esecuzione termini) in pratica contiene i "dati storici" caricati fino all'ultima esecuzione andata a buon fine  */
    {% endif %}
)

select * from deduplicated
