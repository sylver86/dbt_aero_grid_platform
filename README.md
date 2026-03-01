 
# 🌬️ AeroGrid Platform: Enterprise IoT Data Architecture

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.9+-blue?logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/dbt--core-1.8+-orange?logo=dbt&logoColor=white" alt="dbt" />
  <img src="https://img.shields.io/badge/GCP_BigQuery-Cloud--DWH-blue?logo=googlecloud&logoColor=white" alt="Google Cloud Platform" />
  <img src="https://img.shields.io/badge/FinOps-cost--efficient-success?logo=moneygram&logoColor=white" alt="FinOps" />
  <img src="https://img.shields.io/badge/dbt--evaluator-validated-brightgreen?logo=checkmarx&logoColor=white" alt="dbt project evaluator" />
  <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="License" />
</p>

**AeroGrid Platform** è un'infrastruttura dati Enterprise end-to-end progettata per l'ingestion, l'elaborazione e l'analisi avanzata di dati telemetrici IoT provenienti da una flotta di turbine eoliche. 

Sviluppato per simulare scenari reali ad alta intensità di dati (tipici del settore Energy/Aerospace), il progetto trasforma terabyte di rilevazioni grezze e non strutturate in Data Products certificati, pronti per la Business Intelligence e algoritmi di Predictive Maintenance.



---

## 🎯 Executive Summary & Valore di Business
Il progetto affronta e risolve le sfide critiche dell'ingegneria dei dati moderna per scenari ad alta intensità, posizionandosi come una soluzione "Enterprise-Ready". L'architettura implementa le best practice e gli standard ufficiali dbt Labs, strutturandosi su 4 pilastri strategici:

### 🏛️ 1. Architettura e Governance
* **Data Mesh & Domain-Driven Design (Multi-Project):** Suddivisione in due progetti dbt distinti e interdipendenti per evitare colli di bottiglia organizzativi. `platform_core` (Producer) è gestito dal team Data Engineering per le trasformazioni core; `analytics_hub` (Consumer) è dedicato alla BI. Una macro custom forza l'ambiente consumer a interrogare sempre la produzione reale, garantendo il disaccoppiamento senza duplicazione dei dati.
* **Architettura Medallion & Time Spine:** Strutturazione rigorosa in layer Staging (normalizzazione), Intermediate (logiche di business) e Marts (Gold Layer). Include l'implementazione di una Time Spine ininterrotta (2020-2030) vitale per gestire i tipici "buchi" di trasmissione dell'IoT e supportare aggregazioni temporali perfette.

### 🛡️ 2. Resilienza e Data Quality Industriale
* **Gestione 'Late Arriving Data' (Self-Healing):** Gestione automatica dei ritardi di rete IoT tramite pattern di UPSERT. I modelli incrementali sfruttano la strategia merge e chiavi Hash MD5 (surrogate keys) per accodare i nuovi pacchetti e sovrascrivere eventuali ritrasmissioni, annullando il rischio di duplicati.
* **Data Contracts & Model Versioning:** Il data product principale è blindato da rigidi Data Contracts (`enforced: true`) che impediscono modifiche distruttive allo schema. Le evoluzioni sono gestite tramite Model Versioning nativo, mantenendo le vecchie versioni operative (con `deprecation_date`) per garantire migrazioni a zero-downtime per i team a valle.
* **Quality Assurance a 3 Livelli & Fisica dei Dati:** Oltre ai test relazionali e ai limiti parametrici, il progetto implementa Singular Tests SQL che validano vere e proprie leggi fisiche industriali (es. impossibilità di generare energia in assenza di vento), isolando immediatamente anomalie hardware sfuggite ai sensori.
* **Source Freshness & SLA Monitoring:** Controlli rigorosi sulle fonti grezze per monitorare la latenza. In ambito eolico, intercettare oltre 24h di mancata trasmissione trasforma la pipeline dati in un sistema di allerta operativa precoce contro guasti ai gateway SCADA.

### 💡 3. Advanced Analytics & Astrazione
* **Polyglot Transformation (dbt-Python per Manutenzione Predittiva):** I calcoli procedurali statistici complessi non vengono forzati in SQL. Il progetto esegue nativamente nel DWH modelli Python (pandas via Dataproc) per l'individuazione di anomalie vibrazionali tramite Z-Score, fornendo dati pronti per interventi di manutenzione predittiva.
* **Semantic Layer & MetricFlow:** Astrazione totale delle logiche di business dal codice fisico. I KPI (come la Potenza Media per Turbina, calcolata dinamicamente come ratio) sono definiti centralmente in YAML, creando una vera "Single Source of Truth" interrogabile da qualsiasi tool BI.
* **Data Lineage Esteso & Exposures:** Il Lineage Graph (DAG) si estende oltre il DWH fino ai tool applicativi (es. dashboard direzionali PowerBI), abilitando una Impact Analysis istantanea e indicando chiaramente l'ownership dei Data Steward.

### ⚙️ 4. Scalabilità ed Efficienza (FinOps & DevOps)
* **Ottimizzazione Costi BigQuery (FinOps):** Architettura progettata per abbattere i costi di I/O. L'uso combinato di partizionamento temporale (`partition_by`), clustering, modelli incrementali e filtri dinamici di lookback in staging azzera i "full-table scan", massimizzando il Partition Pruning.
* **Storicizzazione Asset (SCD Type 2):** Tracciamento automatico del ciclo di vita fisico dell'hardware tramite i dbt Snapshots. Spostamenti o revamping delle turbine non alterano retroattivamente i KPI passati, garantendo un audit trail energetico immutabile.
* **Metaprogrammazione Jinja (DRY):** Utilizzo di macro e costrutti for-loop dinamici per automatizzare aggregazioni complesse (come i range di potenza pivotati), riducendo drasticamente il debito tecnico e accelerando il time-to-market di nuove feature.
* **DevOps, Slim CI & Deferral:** Pipeline ottimizzate che sfruttano il confronto di stato (`manifest.json`) e il deferral (`--defer`) per elaborare e testare esclusivamente i modelli modificati durante le Pull Request, importando i nodi genitore dalla produzione per una CI velocissima ed economica.

---

## 🧰 Technology Stack Summary

```mermaid
mindmap
    root((AeroGrid<br/>Platform))
        Cloud
            Google Cloud Platform
            BigQuery (DWH)
            Dataproc Serverless
        Transformation
            dbt Core 1.8+
            dbt-bigquery plugin
            dbt_utils
            codegen
        Languages
            SQL (T-SQL compatible)
            Python 3.9+
            Jinja2 Templating
            YAML Configuration
        Python Libraries
            pandas
            numpy
            polars
            PyYAML
            google-cloud-bigquery
        Data Patterns
            Medallion Architecture
            Data Mesh (Multi-Project)
            SCD Type 2 (Snapshots)
            Incremental MERGE
            Surrogate Keys (MD5)
        Quality
            Data Contracts
            Source Freshness SLA
            3-Tier Testing
            Singular Physics Tests
        Analytics
            Semantic Layer / MetricFlow
            Z-Score Anomaly Detection
            Predictive Maintenance
        BI & Exposure
            PowerBI
            Lineage & Impact Analysis
            Data Steward Ownership
        DevOps
            Slim CI (state:modified)
            Deferral (--defer)
            YAML-Driven Config

```

---

## 🏗️ Architettura e Stack Tecnologico
L'architettura si divide in tre macro-moduli, separati fisicamente per supportare pipeline CI/CD indipendenti:

* **Python Data Ingestion (`data_ops_ingestion`):** Modulo ad oggetti per la simulazione e l'ingestion dei dati sensoriali. Implementa logiche di Strict Type Safety verso BigQuery e inietta volontariamente anomalie (valori nulli, outlier termici) per testare la resilienza della pipeline a valle.

```mermaid

classDiagram
    class TurbineDataGenerator {
        -config: dict
        -logger: Logger
        -folder_script: Path
        -folder_project: Path
        -num_turbine: int
        -turbine_id_value: list
        +__init__(config_path: str)
        +generate_metadata() → DataFrame
        +generate_telemetry() → DataFrame
        +run() → None
    }

    class BigQueryIngestor {
        -config: dict
        -logger: Logger
        -folder_project: Path
        -path_to_json: Path
        -client: bigquery.Client
        +__init__(config_path_yaml: str)
        +get_datasets() → list
        +create_dataset_if_not_exists(dataset_id, location)
        +load_table(file_path, table_name, autodetect, schema)
        -_get_telemetry_schema() → list[SchemaField]
        +run() → None
    }

    class SectionFilter {
        +filter(record) → bool
    }

    class setup_logging {
        <<function>>
        +setup_logging() → None
    }

    TurbineDataGenerator ..> setup_logging : uses
    BigQueryIngestor ..> setup_logging : uses
    BigQueryIngestor ..> TurbineDataGenerator : reads output CSVs

    note for TurbineDataGenerator "Anomaly Injection:\n• 18 NULL records\n• 50 Outliers (temp=550°C, rpm=-999)\n• 15 Duplicate rows"
    note for BigQueryIngestor "Type Safety:\n• Metadata: autodetect\n• Telemetry: explicit schema\n  (TIMESTAMP, STRING, FLOAT64)"

```

* **Producer Domain (`platform_core`):** Progetto dbt Core dedicato al Data Engineering puro. Mappa le fonti, sanifica i dati, storicizza le anagrafiche (SCD2) e applica complessi modelli fisico-matematici.
* **Consumer Domain (`analytics_hub`):** Progetto dbt Core per la Business Intelligence. Importa i dati dal layer core tramite le logiche di Cross-Project References tipiche del Data Mesh, ignorando gli ambienti di dev e puntando direttamente alla produzione.

### 🏗️ High-Level Focus Architettura

```mermaid
graph TB
    subgraph "🌐 Data Sources"
        S1[("🏭 SCADA Gateway<br/>Turbine Sensors")]
        S2[("📊 Asset Registry<br/>Turbine Metadata")]
    end

    subgraph "🐍 Ingestion Layer — Python"
        direction TB
        GEN["generate_data.py<br/><i>TurbineDataGenerator</i><br/>Synthetic data + anomaly injection"]
        ING["ingest_raw_data.py<br/><i>BigQueryIngestor</i><br/>Strict Type Safety + WRITE_TRUNCATE"]
    end

    subgraph "☁️ Google Cloud Platform"
        BQ_RAW[("BigQuery<br/><b>aero_grid_raw</b><br/>Raw Landing Zone")]

        subgraph "⚙️ platform_core — Producer Domain (dbt)"
            STG["Staging Layer<br/><code>stg_*</code><br/>Views"]
            INT["Intermediate Layer<br/><code>int_*</code><br/>Ephemeral + Python"]
            MART["Marts Layer<br/><code>fct_*</code><br/>Incremental Tables"]
            SNAP["Snapshots<br/><code>snp_*</code><br/>SCD Type 2"]
            SEED["Seeds<br/><code>turbine_codes</code><br/>Lookup Tables"]
        end

        subgraph "📊 analytics_hub — Consumer Domain (dbt)"
            CONS["Consumer Models<br/>Cross-Project Ref<br/>→ Always reads PROD"]
        end

        BQ_MART[("BigQuery<br/><b>aero_grid_mart</b><br/>Gold Layer")]
    end

    subgraph "📈 Consumption"
        PBI["PowerBI<br/>Executive Dashboard"]
        ML["Python / Dataproc<br/>Predictive Maintenance"]
        API["API / Notebooks<br/>Ad-hoc Analytics"]
    end

    S1 --> GEN
    S2 --> GEN
    GEN --> ING
    ING --> BQ_RAW
    BQ_RAW --> STG
    STG --> INT
    INT --> MART
    MART --> BQ_MART
    STG --> SNAP
    SEED --> STG
    BQ_MART --> CONS
    CONS --> PBI
    MART --> ML
    BQ_MART --> API

    style BQ_RAW fill:#ff6b6b,color:#fff
    style BQ_MART fill:#51cf66,color:#fff
    style MART fill:#339af0,color:#fff
```


### 🥇 Medallion Architecture — Layer Detail

```mermaid
graph TB
    subgraph "🟤 BRONZE — Raw Landing Zone"
        RAW_TEL["raw_turbine_telemetry<br/><i>7 columns, untyped</i><br/>timestamp, turbine_id,<br/>wind_speed_ms, rpm,<br/>power_output_kw_,<br/>temperature_c, vibration_index"]
        RAW_ASS["raw_turbine_assets<br/><i>5 columns</i><br/>turbine_id, model,<br/>location, installation_date,<br/>capacity_kw"]
    end

    subgraph "🥈 SILVER — Staging Layer (schema: stg)"
        STG_TEL["stg_turbine_telemetry<br/><b>Incremental MERGE</b><br/>━━━━━━━━━━━━━━━<br/>+ telemetry_key (MD5 hash)<br/>+ CAST timestamp → TIMESTAMP<br/>+ TRIM turbine_id<br/>+ rpm sentinel -999 → NULL<br/>+ temperature_c ≤ 100 filter<br/>+ vibration 0..5 filter<br/>+ Lookback: last 7 days<br/>+ Deduplication: DISTINCT"]
        STG_ASS["stg_turbine_assets<br/><b>View</b><br/>━━━━━━━━━━━━━━━<br/>+ turbine_key (MD5 hash)<br/>+ CAST installation_date → DATE<br/>+ capacity_kw: 0 → NULL<br/>+ Deduplication: DISTINCT"]
        SEED_LC["stg_lookup_turbine_error_code<br/><b>Seed (CSV)</b><br/>━━━━━━━━━━━━━━━<br/>code, severity,<br/>description, action_required"]
    end

    subgraph "🥇 GOLD — Intermediate + Marts"
        INT_PERF["int_turbine_performance_check<br/><b>Ephemeral</b><br/>━━━━━━━━━━━━━━━<br/>+ theoretical_power_mw<br/>&nbsp;&nbsp;= ROUND(wind³ × 0.5, 2)<br/>&nbsp;&nbsp;via macro elab_power_theoretical"]
        INT_PIVOT["int_turbine_range_pivot<br/><b>View</b><br/>━━━━━━━━━━━━━━━<br/>Jinja FOR loop su soglie:<br/>[100, 500, 1000, 2000] kW<br/>→ readings_above_*_kw"]
        INT_ZSCORE["int_turbine_vibration_anomalies<br/><b>Table (Python / Dataproc)</b><br/>━━━━━━━━━━━━━━━<br/>Z-Score = (val - μ) / σ<br/>Filter: |Z| > 3<br/>pandas groupby + transform"]
        FCT["fct_turbine_telemetry<br/><b>Incremental MERGE</b><br/>━━━━━━━━━━━━━━━<br/>+ power_output_mw (kW→MW macro)<br/>+ theoretical_power_mw<br/>+ Partition: DAY on measurement_at<br/>+ Cluster: turbine_id<br/>+ Data Contract: enforced ✅<br/>+ on_schema_change: fail"]
    end

    subgraph "📸 Snapshots"
        SCD["snp_turbine_assets<br/><b>SCD Type 2</b><br/>━━━━━━━━━━━━━━━<br/>strategy: check<br/>check_cols: capacity_kw,<br/>&nbsp;&nbsp;location, model<br/>unique_key: turbine_id"]
    end

    RAW_TEL --> STG_TEL
    RAW_ASS --> STG_ASS
    STG_TEL --> INT_PERF
    INT_PERF --> INT_PIVOT
    INT_PERF --> FCT
    FCT --> INT_ZSCORE
    RAW_ASS --> SCD
    STG_ASS -.->|ref integrity test| STG_TEL

    style RAW_TEL fill:#cd853f,color:#fff
    style RAW_ASS fill:#cd853f,color:#fff
    style STG_TEL fill:#c0c0c0,color:#333
    style STG_ASS fill:#c0c0c0,color:#333
    style FCT fill:#ffd700,color:#333
    style INT_ZSCORE fill:#9b59b6,color:#fff
```

### 🔄 Data Flow — End-to-End Pipeline

```mermaid
flowchart LR
    subgraph "1. GENERATE"
        A["🎲 Synthetic Data<br/>15 turbine × 1000 readings<br/>+ 18 NULLs<br/>+ 50 Outliers<br/>+ 15 Duplicates"]
    end

    subgraph "2. INGEST"
        B["📤 BigQuery Load<br/>WRITE_TRUNCATE<br/>Strict Schema for Telemetry<br/>Autodetect for Metadata"]
    end

    subgraph "3. STAGE"
        C["🧹 Cleansing<br/>CAST types<br/>TRIM strings<br/>NULL sentinels → NULL<br/>Surrogate Keys (MD5)<br/>Deduplication<br/>Lookback Filter (7 days)"]
    end

    subgraph "4. INTERMEDIATE"
        D["🔬 Enrichment<br/>Theoretical Power (Jinja UDF)<br/>Vibration Z-Score (Python)<br/>Range Pivot (Jinja loop)"]
    end

    subgraph "5. MART"
        E["🏆 Gold Layer<br/>Incremental MERGE<br/>Partition by DAY<br/>Cluster by turbine_id<br/>Data Contract enforced"]
    end

    subgraph "6. CONSUME"
        F["📊 BI & ML<br/>PowerBI Dashboard<br/>Anomaly Alerts<br/>Predictive Maintenance"]
    end

    A -->|CSV files| B
    B -->|aero_grid_raw| C
    C -->|Views| D
    D -->|Ephemeral| E
    E -->|Tables| F

    style A fill:#ffe066,color:#333
    style B fill:#ffa94d,color:#333
    style C fill:#ff8787,color:#fff
    style D fill:#da77f2,color:#fff
    style E fill:#51cf66,color:#fff
    style F fill:#339af0,color:#fff

```

### 🔀 Data Mesh — Multi-Project Topology

```mermaid
graph LR
    subgraph "PRODUCER — platform_core"
        direction TB
        PC_STG["staging/"]
        PC_INT["intermediate/"]
        PC_MART["marts/<br/><b>fct_turbine_telemetry</b><br/>Data Contract ✅"]
        PC_SEM["semantic/<br/>MetricFlow definitions"]
        PC_SNAP["snapshots/<br/>SCD2 assets"]

        PC_STG --> PC_INT --> PC_MART
        PC_MART --> PC_SEM
    end

    subgraph "CONSUMER — analytics_hub"
        direction TB
        AH_DEP["dependencies.yml<br/><code>projects:<br/>&nbsp;&nbsp;- name: platform_core<br/>&nbsp;&nbsp;&nbsp;&nbsp;path: ../platform_core</code>"]
        AH_MOD["models/<br/>BI-layer views &<br/>report-ready aggregations"]

        AH_DEP --> AH_MOD
    end

    PC_MART ==>|"Cross-Project Ref<br/>{{ ref('platform_core', 'fct_turbine_telemetry') }}<br/>⚠️ Always reads PRODUCTION"| AH_MOD

    subgraph "GOVERNANCE"
        GOV1["🛡️ Data Contract<br/>enforced: true"]
        GOV2["📋 Model Versioning<br/>latest_version + deprecation"]
        GOV3["👤 Data Steward<br/>Eugenio Pasqua<br/>(exposures.yml)"]
    end

    PC_MART --- GOV1
    PC_MART --- GOV2
    PC_MART --- GOV3

    style PC_MART fill:#339af0,color:#fff
    style AH_MOD fill:#51cf66,color:#fff
    style GOV1 fill:#ff6b6b,color:#fff
    style GOV2 fill:#ff6b6b,color:#fff
    style GOV3 fill:#ff6b6b,color:#fff

```



---

## ✨ Enterprise Features Implementate
Questo repository è stato sviluppato seguendo rigorosamente gli standard ufficiali di dbt Labs e validato tramite il pacchetto `dbt_project_evaluator`.

* 🛡️ **Data Contracts & Versioning:** La fact table principale è blindata tramite contract: `enforced: true`. Evoluzioni strutturali sono gestite tramite `latest_version` e politiche di deprecazione programmate, garantendo zero disservizi per gli analisti BI.
* 🐍 **Polyglot Data Transformation (dbt-Python):** I calcoli procedurali complessi (come lo Z-Score per la rilevazione delle anomalie vibrazionali) non sono forzati in SQL, ma eseguiti nativamente nel DWH sfruttando modelli Python integrati nel DAG (via Dataproc Serverless).
* ⚙️ **Slim CI & Deferral:** Predisposizione per l'automazione DevOps tramite i flag `--state` e `--defer`, processando in CI solo il codice alterato durante le Pull Request, importando i nodi genitore direttamente dalla produzione.
* 📏 **Semantic Layer (MetricFlow):** Astrazione delle logiche aggregative dal codice SQL fisico. Metriche complesse (es. potenze medie e ratio) sono definite in YAML (`turbine_metrics.yml`), garantendo una singola "Source of Truth" per l'azienda.
* 🧪 **Advanced Data Quality (Data Physics):** Oltre ai test relazionali e ai bound parametrici (`dbt_utils.accepted_range`), il progetto include test SQL singolari per validare veri e propri principi fisici (es. impossibilità di generare energia in assenza di vento).

---

## 📂 Struttura del Repository (Monorepo)

```text
aero-grid-platform/
├── data_ops_ingestion/          # ELT Ingestion Engine (Python/Pandas/GCP)
│   ├── config/                  
│   ├── src/                     
│   └── utils/                   
├── platform_core/               # PRODUCER: Core Data Engineering (dbt)
│   ├── macros/                  # Jinja utils & Dynamic Schema override
│   ├── models/
│   │   ├── staging/             # Hashing, standardizzazione e SLA monitor
│   │   ├── intermediate/        # Ephemeral views, Python Models, Jinja loops
│   │   ├── marts/               # Modelli Incrementali versionati (Gold Layer)
│   │   └── semantic/            # Definizione del layer semantico
│   ├── snapshots/               # SCD Type 2 per gli asset fisici
│   └── tests/                   # Singular tests sulla fisica dei dati
└── analytics_hub/               # CONSUMER: Business Intelligence (dbt)
    ├── dependencies.yml         # Puntamento locale a platform_core
    └── models/                  

```

---

## 🚀 Getting Started

### Prerequisiti

* Python 3.9+
* dbt-core 1.8+ e plugin `dbt-bigquery`
* Credenziali attive per Google Cloud Platform (BigQuery)

### Setup Ambiente

**1. Configurazione Profilo (`profiles.yml`)**
Configura il file `~/.dbt/profiles.yml` puntando al tuo progetto Google Cloud.

**2. Esecuzione Ingestion**
Simula la generazione e il caricamento dei dati telemetrici grezzi:

```bash
cd data_ops_ingestion
python src/ingest_raw_data.py

```

**3. Build della Data Platform (Platform Core)**
Installa le dipendenze ed esegui l'intera pipeline di trasformazione e validazione:

```bash
cd ../platform_core
dbt deps
dbt build

```

*(Il comando `build` concatenerà automaticamente run, test, snapshot e validazione seed).*

**4. Esplorazione tramite Analytics Hub**
Per simulare il lavoro del team BI che accede ai dati governati:

```bash
cd ../analytics_hub
dbt deps
dbt run

```

---

*Progettato e sviluppato da Eugenio Pasqua.*


---

