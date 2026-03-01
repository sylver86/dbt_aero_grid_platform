This file is a merged representation of the entire codebase, combined into a single document by Repomix.

<file_summary>
This section contains a summary of this file.

<purpose>
This file contains a packed representation of the entire repository's contents.
It is designed to be easily consumable by AI systems for analysis, code review,
or other automated processes.
</purpose>

<file_format>
The content is organized as follows:
1. This summary section
2. Repository information
3. Directory structure
4. Repository files (if enabled)
5. Multiple file entries, each consisting of:
  - File path as an attribute
  - Full contents of the file
</file_format>

<usage_guidelines>
- This file should be treated as read-only. Any changes should be made to the
  original repository files, not this packed version.
- When processing this file, use the file path to distinguish
  between different files in the repository.
- Be aware that this file may contain sensitive information. Handle it with
  the same level of security as you would the original repository.
</usage_guidelines>

<notes>
- Some files may have been excluded based on .gitignore rules and Repomix's configuration
- Binary files are not included in this packed representation. Please refer to the Repository Structure section for a complete list of file paths, including binary files
- Files matching patterns in .gitignore are excluded
- Files matching default ignore patterns are excluded
- Files are sorted by Git change count (files with more changes are at the bottom)
</notes>

</file_summary>

<directory_structure>
analyses/
  .gitkeep
macros/
  .gitkeep
  calculate_theoretical_power.sql
  conversion_utils.sql
  generate_schema_name.sql
models/
  intermediate/
    _schema.yml
    int_turbine_performance_check.sql
    int_turbine_range_pivot.sql
    int_turbine_vibration_anomalies.py
  marts/
    docs/
      doc_business_logic.md
    _schema.yml
    exposures.yml
    fct_turbine_telemetry_v1.sql
    fct_turbine_telemetry_v2.sql
    time_spine.sql
  semantic/
    turbine_metrics.yml
  staging/
    _schema.yml
    _sources.yml
    stg_turbine_assets.sql
    stg_turbine_telemetry.sql
  _groups.yml
seeds/
  .gitkeep
  turbine_codes.csv
snapshots/
  .gitkeep
  snp_turbine_assets.sql
tests/
  staging/
    assert_power_consistent_with_wind.sql
  .gitkeep
.gitignore
dbt_project.yml
package-lock.yml
packages.yml
README.md
</directory_structure>

<files>
This section contains the contents of the repository's files.

<file path="analyses/.gitkeep">

</file>

<file path="macros/.gitkeep">

</file>

<file path="macros/calculate_theoretical_power.sql">
{% macro elab_power_theoretical(wind_column)%}

    ROUND(POWER(CAST({{ wind_column }} AS float64), 3) * 0.5, 2)

{% endmacro %}
</file>

<file path="macros/conversion_utils.sql">
{% macro kw_to_mw(column_name, precision=2) %}

    round( {{ column_name }} / 1000, {{ precision }} )

{% endmacro %}
</file>

<file path="macros/generate_schema_name.sql">
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {#
        Quando platform_core gira come package dentro un altro progetto,
        forza il dataset di produzione per i modelli pubblici

        node.package_name è una proprietà del singolo modello che dbt sta compilando in quel momento.
        Indica esattamente "questo modello appartiene a quale progetto/package?".

        Quindi se dbt sta compilando fct_turbine_telemetry, node.package_name sarà 'platform_core'.
        Se sta compilando test_core_link, sarà 'analytics_hub'

        project_name è una variabile globale che ti dice "da quale progetto ho lanciato il comando dbt run?".
        Se viene eseguito dbt run dalla cartella di analytics_hub, project_name sarà sempre 'analytics_hub',
        indipendentemente da quale modello sta compilando.

    #}
    {%- if node.package_name == 'platform_core' and project_name != 'platform_core' -%}
        {%- if custom_schema_name is not none -%}
            prod_aero_grid_platform_{{ custom_schema_name | trim }}
        {%- else -%}
            prod_aero_grid_platform
        {%- endif -%}

    {# Comportamento normale quando platform_core gira standalone #}
    {%- elif custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
</file>

<file path="models/intermediate/_schema.yml">
version: 2

models:
  - name: int_turbine_performance_check
    description: "Modello intermedio per il calcolo della potenza teorica. Materializzazione: Ephemeral."
    columns:
      - name: theoretical_power_kw
        description: "Potenza calcolata tramite macro fisica. Deve essere sempre >= 0."
        tests:
          - dbt_utils.accepted_range:
              arguments:
                min_value: 0
                inclusive: true
      - name: turbine_id
        tests:
          - relationships:                                                                          # Test di integrità referenziale
              arguments:
                to: ref('stg_turbine_assets')
                field: turbine_id
</file>

<file path="models/intermediate/int_turbine_performance_check.sql">
{{
    config(
        materialized='ephemeral',
        access='protected'
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
</file>

<file path="models/intermediate/int_turbine_range_pivot.sql">
/* 1. Definiamo la lista delle soglie */
{% set power_thresholds = [100, 500, 1000, 2000] %}

with performance as (
    select * from {{ ref('int_turbine_performance_check') }}
),

final as (
    select
        turbine_id,
        /* 2. Ciclo dinamico sulle soglie */
        {% for thresh in power_thresholds %}
            sum(case when power_output_kw >= {{ thresh }} then 1 else 0 end) as readings_above_{{ thresh }}_kw
            {% if not loop.last %},{% endif %}
        {% endfor %}
    from performance
    group by 1
)

select * from final
</file>

<file path="models/intermediate/int_turbine_vibration_anomalies.py">
import pandas as pd

# Modello intermediate creato come gli altri modelli SQL ma con codice Python.
# Dbt Aggancia questo modello nella DAG come tutti gli altri ma sfrutta la potenza di calcolo di python.
# Viene quindi creato come tutti nella esecuzione di dbt run, build.

# Il codice esegue un'analisi chiamata Z-Score Outlier Detection
# Calcoliamo la media e la deviazione standard per ogni turbina
# Dopo applichiamo la formula di z-score :

        # Valore 0 vuol dire che il dato del "power_output_mw" è nella media e rientra nella normalità
        # Valore = 3 vuol dire che il valore di "power_output_mw" è lontano dalla normalità



def model(dbt, session):
    # Configurazione
    dbt.config(materialized="table")
    df = dbt.ref("fct_turbine_telemetry").to_df()                                                   # Carica i dati dal mart SQL

    df['avg_power'] = df.groupby('turbine_id')['power_output_mw'].transform('mean')                 # Calcolo la media di power per turbina
    df['std_power'] = df.groupby('turbine_id')['power_output_mw'].transform('std')                  # Calcolo la deviazione standard per turbina

    # Usiamo lo Z-Score: (valore - media) / deviazione_standard
    df['z_score'] = (df['power_output_mw'] - df['avg_power']) / df['std_power']                     # Definisco lo zscore per gli outlier

    # Filtriamo le anomalie (Z-Score > 3 è lo standard statistico per gli outlier)
    df_anomalies = df[df['z_score'].abs() > 3].copy()                                               # Individuo gli outlier

    return df_anomalies
</file>

<file path="models/marts/docs/doc_business_logic.md">
{% docs wind_power_logic %}
Questa colonna calcola la potenza teorica della turbina utilizzando la legge fisica dell'energia eolica.
La formula applicata (tramite macro UDF) è la Velocità del Vento al cubo moltiplicata per 0.5.
Serve al business per calcolare le inefficienze aerodinamiche.
{% enddocs %}
</file>

<file path="models/marts/_schema.yml">
version: 2

models:

  - name: time_spine
    description: "Calendario base per il Semantic Layer"

    time_spine:
      standard_granularity_column: date_day                                                         # Indica a dbt qual è "l'asse temporale" quindi il campo principale da usare per i calcoli quando l'utente non ne specifica uno

    columns:
      - name: date_day
        data_type: date
        granularity: day                                                                            # Indichiamo a dbt qual è il "livello minimo" di aggregazione supportato dalla colonna.

  - name: fct_turbine_telemetry
    latest_version: 2
    description: "Tabella dei fatti certificata per la telemetria delle turbine. Contiene calcoli di performance teorica."
    config:
      contract:
        enforced: true                                                                              # <--- PER LA DEFINIZIONE DEL DATA MESH: Il Contratto è Attivo - i campi sono blindati

    versions:
      - v: 1
        deprecation_date: '2026-03-22'
      - v: 2
        columns:
          - name: power_corrected_mw                                                                # Questo è il campo aggiuntivo della versione 2
            data_type: float64

    columns:                                                                                        # Elenco delle colonne della versione base
      - name: telemetry_key
        data_type: string
        tests:
          - unique
          - not_null
      - name: turbine_id
        data_type: string
      - name: measurement_at
        data_type: timestamp
      - name: wind_speed_ms
        data_type: float64
      - name: power_output_kw
        data_type: float64
      - name: power_output_mw
        data_type: float64
      - name: theoretical_power_mw
        data_type: float64
        description: '{{ doc("wind_power_logic") }}'
</file>

<file path="models/marts/exposures.yml">
version: 2

exposures:
  - name: executive_wind_farm_monitor                                                               # Nome identificativo della exposure
    type: dashboard                                                                                 # Natura dell'asset esterno per categorizzarli visivamente (dashboard, notebook, analysis, ml, application)
    maturity: high                                                                                  # Indica l'affidabilità e lo stato di produzione dell'asset (high certificati per i dirigenti, medium uso dipartimentale, low in fase di test)
    url: https://powerbi.com/aerogrid/exec-monitor                                                  # Esempio di link della app dashboard che sfrutta i dati - usato nel sito di documentazione di dbt che rimanda ad app
    description: "Dashboard direzionale per il monitoraggio della potenza teorica vs reale."        # Descrizione obiettivo della dashboard (supporterebbe il formato md)
    depends_on:                                                                                     # Elenco dei modelli che fanno parte della exposures
      - ref('fct_turbine_telemetry')
      - ref('int_turbine_vibration_anomalies') # Se la dashboard usa anche il modello Python
    owner:
      name: "Eugenio Pasqua"                                                                        # Nome del responsabile (Data Steward o analista) dell'asset dati con nome ed email
      email: "eugenix86@gmail.com"
</file>

<file path="models/marts/fct_turbine_telemetry_v1.sql">
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
</file>

<file path="models/marts/fct_turbine_telemetry_v2.sql">
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
    {{ kw_to_mw('power_output_kw') }} as power_output_mw,                                           /* Utilizzo una macro che consente di convertire i kw in mw dei valori */
    {{ kw_to_mw('power_output_kw') }} *0.98 as power_corrected_mw                                   /* Nella versione v2 del modello aggiungiamo una colonna che rettifica la formula  */
    from source

    {% if is_incremental() %}
        -- Filtro per caricare solo i nuovi dati basandosi sull'ultimo timestamp presente
        where measurement_at >= ( select max(measurement_at) from {{ this }} )
    {% endif %}
)

select * from incremental
</file>

<file path="models/marts/time_spine.sql">
{{
    config(
        materialized = 'table'
    )
}}

with days as (

    {{

            dbt_utils.date_spine(
                datepart = "day",
                start_date = "cast('2020-01-01' as date)",
                end_date = "cast('2030-12-31' as date)"
            )

    }}

),

enriched_dates as (

    select
        cast(date_day as date) as date_day,

        -- Estrazione di Anno, Mese e Trimestre
        extract(year from date_day) as date_year,
        extract(month from date_day) as date_month,
        extract(quarter from date_day) as date_quarter,

        cast(extract(year from date_day) as string) || '-' || lpad(cast(extract(month from date_day) as string), 2, '0') as year_month

    from days
)

select * from enriched_dates
</file>

<file path="models/semantic/turbine_metrics.yml">
semantic_models:
  - name: total_power                                                                               # Nome della misura
    description: "Potenza totale erogata"

    model: ref('fct_turbine_telemetry')                                                             # Puntiamo al mart (v2 di default)


    defaults:
      agg_time_dimension: measurement_at


    entities:
      - name: turbine
        type: primary
        expr: turbine_id                                                                            # Qui definiamo su quale base calcolare la misura quindi sono i "soggetti" della analisi.



    measures:                                                                                       # Definiamo le aggregazioni base (SUM, COUNT, MIN, MAX) ovvero il "mattoncino" elementare.
      - name: total_power_mw                                                                        # Totale di erogazione
        description: "Somma della potenza corretta generata"
        expr: power_corrected_mw
        agg: sum

      - name: telemetry_count                                                                       # Conteggio delle rilevazioni
        expr: 1
        agg: sum


    dimensions:                                                                                     # Rappresentano gli attributi per cui  filtrare o raggruppare (es. Tempo, Modello Turbina, Location).
      - name: measurement_at                                                                        # Lo facciamo per granularità giornaliera
        type: time
        type_params:
          time_granularity: day

metrics:                                                                                            # Calcolo finale che l'utente vede. Può essere semplice, un calcolo tra più misure (Ratio), o una metrica che guarda a periodi precedenti (Rolling/Cumulative).
  # 1. Creiamo una metrica semplice che espone la measure della potenza
  - name: total_power_mw
    description: "Somma della potenza corretta generata"
    label: "Potenza Totale (MW)"
    type: simple
    type_params:
      measure: total_power_mw # <-- Qui "pesca" dal semantic_model

  # 2. Creiamo una metrica semplice che espone il conteggio
  - name: telemetry_count
    description: "Conteggio delle rilevazioni"
    label: "N° Telemetrie rilevate"
    type: simple
    type_params:
      measure: telemetry_count # <-- Qui "pesca" dal semantic_model

  # 3. Ora la tua metrica ratio funzionerà perfettamente
  - name: average_power_per_turbine
    description: "La potenza media reale corretta per ogni turbina"
    label: "Potenza Media (MW)"
    type: ratio                                                                                     # Per far funzionare un type ratio ci serve il numeratore e denominatore
    type_params:
      numerator: total_power_mw
      denominator: telemetry_count
</file>

<file path="models/staging/_schema.yml">
version: 2

models:
  - name: stg_turbine_assets
    description: "Tabella staging che costituisce l'anagrafica e l'asset delle turbine eolice"
    columns:
      - name: turbine_key
        data_type: string
        description: "Chiave surrogata della turbina"
        tests:
          - unique
          - not_null

      - name: turbine_id
        data_type: string
        description: "Chiave identificativa della turbina"
        tests:
          - not_null

      - name: model
        data_type: string
        description: "Modello della turbina"
        tests:
          - not_null

      - name: location
        data_type: string
        description: "Ubicazione della turbina"
        tests:
          - not_null

      - name: installation_date
        data_type: date
        description: "Data di installazione della turbina"
        tests:
          - not_null

      - name: capacity_kw
        data_type: int64
        description: "Capacità kw della turbina"
        tests:
          - not_null



#---------------------------------------------------------------------------------------------------


  - name: stg_turbine_telemetry
    description: "Monitoraggio sensoriale della turbina"
    columns:
      - name: telemetry_key
        data_type: string
        description: "Chiave surrogata della telemetria"
        tests:
          - not_null
          - unique

      - name: measurement_at
        data_type: timestamp
        description: "Momento della rilevazione sensoriale"
        tests:
          - not_null

      - name: turbine_id
        data_type: string
        description: "Identificativo della turbina"
        tests:
          - not_null
          - relationships:
              arguments:
                to: ref('stg_turbine_assets')                                                         # La tabella di riferimento
                field: turbine_id                                                                     # La chiave primaria della tabella di riferimento
                                                                                                    # Definisco un vincolo di Integrità Referenziale (molto simile a una Foreign Key nei database relazionali), ma con una differenza fondamentale: dbt non blocca l'inserimento dei dati, bensì testa se la relazione è rispettata dopo che i dati sono stati caricati.
                                                                                                    # IN PRATICA TUTTI I VALORI DI 'turbine_id' devono essere tra quelli definiti in 'turbine_id' della tabella "stg_turbine_assets"

      - name: wind_speed_ms
        data_type: float64
        description: "Velocità percepita del vento"
        tests:
          - not_null
          - dbt_utils.accepted_range:                                                               # Definiamo un range di valori plausibili per il campo
              min_value: 0
              max_value: 50
              inclusive: true

      - name: rpm
        data_type: float64
        description: "RPM"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 500
              inclusive: true


      - name: power_output_kw
        data_type: float64
        description: "Potenza erogata in kw"
        tests:
          - not_null
          - dbt_utils.accepted_range:
                min_value: 0
                max_value: 4000
                inclusive: true

      - name: temperature_c
        data_type: float64
        description: "Temperatura rilevata"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 120
              inclusive: true

      - name: vibration_index
        data_type: float64
        description: "Vibrazione rilevata"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 5
              inclusive: true
</file>

<file path="models/staging/_sources.yml">
sources:

  - name: external_data_raw                                                                         # Nome della sorgente - eseguendo dbt run-operation generate_base_model --args '{"source_name": "external_data_raw", "table_name": "raw_turbine_assets"}' ottengo testo descrittivo delle strutture per impostare i modelli base.
    description: "Dati grezzi caricati su DWH da script Python vs BigQuery"
    database: aero-grid-cert-project                                                                # Nome "Database che è il progetto Bigquery
    schema: aero_grid_raw                                                                           # Nome "Schema"  che è il dataset  BigQuery


    tables:

      - name: raw_turbine_assets                                                                    # Nome della tabella fisica su Bigquery
        description: "Dati di telemetria delle turbine eoliche"

        columns:
          - name: turbine_id
            description: "Identificativo univoco della turbina"
            tests:
              - not_null

          - name: model
            description: "Modello della turbina"
            tests:
              - not_null

          - name: location
            description: "Locazione di dove risulta ubicata la turbina"

          - name: installation_date
            description: "Data di installazione della turbina eolica"

          - name: capacity_kw
            description: "Capacità in kw espressa della turbina eolica"


      - name: raw_turbine_telemetry

        # Impostiamo la freshness sulle rilevazioni sensoriali
        config:
          freshness:
            warn_after:
              count: 12
              period: hour # Impostiamo un controllo di freshness ogni 12 ore (warning).

            error_after:
              count: 24
              period: hour # Impostiamo un alert se non pervengono dati nuovi dopo 24 ore.

          loaded_at_field: timestamp

        columns:
          - name: timestamp
            description: "Momento della rilevazione del sensore"

          - name: turbine_id
            description: "Identificativo univoco della turbina cui è stata fatta la rilevazione sensoriale"

          - name: wind_speed_ms
            description: "Rilevazione della velocità del vento"

          - name: rpm
            description: "Rilevazione degli rpm della turbina eolica"

          - name: power_output_kw_
            description: "Rilevazione della potenza in kw della turbina eolica"

          - name: temperature_c
            description: "Rilevazione della temperatura della turbina eolica"

          - name: vibration_index
            description: "Indice di vibrazione rilevato"
</file>

<file path="models/staging/stg_turbine_assets.sql">
with source as (

    select * from {{ source('external_data_raw', 'raw_turbine_assets') }}

),

renamed as (

    select
        trim(turbine_id) as turbine_id,
        model,
        location,
        cast(installation_date as date) as installation_date,
        case when capacity_kw = 0 then null else capacity_kw end as capacity_kw

    from source

),

deduplicated as (

    select distinct
    {{ dbt_utils.generate_surrogate_key(['turbine_id']) }} as turbine_key,                          /* Chiave primaria */
    *
    from renamed

)

select * from deduplicated
</file>

<file path="models/staging/stg_turbine_telemetry.sql">
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
</file>

<file path="models/_groups.yml">
# models/_groups.yml
groups:
  - name: platform_core_public
    owner:
      name: "Eugenio Pasqua"
      email: "eugenix86@gmail.com"
</file>

<file path="seeds/.gitkeep">

</file>

<file path="seeds/turbine_codes.csv">
code,severity,description,action_required
E-000,Info,Sistema Operativo Normale,Nessuna azione
E-101,Low,Velocità vento insufficiente per avvio,Monitoraggio passivo
E-102,Low,Modalità Test/Calibrazione,Contattare centro controllo
W-201,Medium,Vibrazione sopra soglia standard,Ispezione visiva programmata
W-205,Medium,Temperatura riduttore alta,Verifica livello olio
C-503,Critical,Vibrazione Critica (Z-Score > 3),Arresto immediato automatico
C-509,Critical,Surriscaldamento Generatore,Arresto di emergenza
M-900,Info,Manutenzione Programmata,Esclusione da calcolo KPI
</file>

<file path="snapshots/.gitkeep">

</file>

<file path="snapshots/snp_turbine_assets.sql">
{% snapshot turbine_assets %}

/* Definisco lo snapshot verificando cosa cambia nei 3 campi indicati usando la chiave 'turbine_key' */

{{
    config(
        unique_key='turbine_id',
        strategy='check',
        check_cols=['capacity_kw', 'location', 'model']
    )
}}

with source_data as (
    select * from {{ source('external_data_raw', 'raw_turbine_assets') }}
),

deduplicated as (
                                                                                                    -- Usiamo una window function per numerare le righe con lo stesso ID
                                                                                                    -- Ordiniamo per una data o un ID se presente, altrimenti prendiamo una riga arbitraria
    select *,
        row_number() over (
            partition by turbine_id
            order by installation_date desc
        ) as row_num
    from source_data
)

-- Selezioniamo solo la riga numero 1 per ogni turbine_id in questo modo evitiamo duplicazioni nello snapshot
select * except(row_num)
from deduplicated
where row_num = 1

{% endsnapshot %}
</file>

<file path="tests/staging/assert_power_consistent_with_wind.sql">
/* Test che verifica  la coerenza tra velocità del vento e produzione elettrica.
   Regola: Una turbina non può produrre energia (power_output_kw > 0) se il vento
           è sotto la soglia di "cut-in" (es. meno di 2 m/s).
           Se succede, c'è un errore nel sensore o nei dati.
   Questo test si esegue al lancio di dbt test, dbt lo esegue automaticamente.
*/

Select
a.turbine_id,
a.wind_speed_ms,
a.power_output_kw,
b.capacity_kw
from {{ ref('stg_turbine_telemetry') }} a
left join
{{ref('stg_turbine_assets')}} b
on a.turbine_id = b.turbine_id
Where (wind_speed_ms < 2.0 and power_output_kw > 0)
or a.power_output_kw > b.capacity_kw
</file>

<file path="tests/.gitkeep">

</file>

<file path=".gitignore">
target/
dbt_packages/
logs/
dbt_internal_packages/
</file>

<file path="dbt_project.yml">
# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: 'platform_core'
version: '1.0.0'

# This setting configures which "profile" dbt uses for this project.
profile: 'aero_grid_platform'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In this example config, we tell dbt to build all models in the example/
# directory as views. These settings can be overridden in the individual model
# files using the `{{ config(...) }}` macro.

models:
  platform_core:                                                                                    # Importante specificare il percorso della directory.
    staging:
      +materialized: view
      +schema: stg
    intermediate:
      +materialized: view
      +schema: int
    marts:
      +access: public                                                                               # Importante per abilitare il Mesh verso un altro progetto e quindi definiamo anche al suo interno il 'contract'
      +group: platform_core_public                                                                  # Definizione del gruppo che è responsabile dell'esposizione del modello.
      +materialized: table
      +schema: mart

seeds:                                                                                              # Configurazione dei seeds (lookup table)
  platform_core:
      turbine_codes:                                                                                # Nome del file seed
        +alias: stg_lookup_turbine_error_code                                                       # Nome della tabella
        +schema: stg                                                                                # Andiamo a distribuire i seed nel layer stg
        +column_types:
          code: string
          severity: string
          description: string

snapshots:
  platform_core:                                                                                    # Il nome del progetto dbt
    +target_schema: aero_grid_snapshots                                                             # Lo schema finale


vars:
  telemetry_start_date: 7                                                                           # Variabile che imposta di considerare solo le ultime 7 rilevazioni
</file>

<file path="package-lock.yml">
packages:
  - name: codegen
    package: dbt-labs/codegen
    version: 0.14.0
  - name: dbt_utils
    package: dbt-labs/dbt_utils
    version: 1.3.0
  - name: dbt_project_evaluator
    package: dbt-labs/dbt_project_evaluator
    version: 0.10.0
sha1_hash: 42f55b42a682a8cc5616036e532e8d712685eb19
</file>

<file path="packages.yml">
packages:
  - package: dbt-labs/codegen
    version: 0.14.0

  - package: dbt-labs/dbt_utils
    version: 1.3.0 # Usa la versione compatibile con dbt 1.10+

  - package: dbt-labs/dbt_project_evaluator
    version: 0.10.0 # o l'ultima versione disponibile
</file>

<file path="README.md">
Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
</file>

</files>
