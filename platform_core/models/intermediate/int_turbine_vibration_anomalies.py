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
