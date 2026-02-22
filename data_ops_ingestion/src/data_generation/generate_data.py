#-Istanziamo le librerie e gli oggetti che usiamo --------------------------------------------------

import datetime
import random
import sys
import os
from pathlib import Path
import logging
import numpy as np
import pandas as pd
import polars as pl
import yaml


class TurbineDataGenerator:

    def __init__(self, config_path) -> None:

        # Lettura del file YAML di configurazione
        with open(config_path, 'r', encoding='utf-8') as file:
            try:
                self.config = yaml.safe_load(file)
            except yaml.YAMLError as exc:
                self.logger.info(f"Errore durante la lettura dello YAML: {exc}",
                                  extra={'section': 'INIZIALIZZAZIONE GENERATORE DATI SORGENTI'})

        # Ulteriore inizializzazione del logger
        Root = Path(__file__).parent.parent.parent.resolve()
        sys.path.append(str(Root))                                                                  # sys.path è una lista di stringhe che contiene tutti i percorsi (directory) in cui Python va a cercare i moduli nel momento in cui viene usato import nome_modulo.
        from utils.logger_config import setup_logging
        setup_logging()

        # Inizializzazione dei path (trova la cartella dove risiede fisicamente il file .py)
        self.folder_script = Path(__file__).parent.resolve()
        self.folder_project = self.folder_script.parent.parent

        # Verifichiamo cartella di output in caso la creiamo
        os.makedirs(os.path.join(self.folder_project, self.config['output']['folder']), exist_ok=True)



        np.random.seed(int(self.config['simulation']['seed']))                                      # Definiamo il seme generativo dei dati e lo prendiamo dal file di configurazione

        self.num_turbine = self.config['simulation']['num_turbines']
        self.turbine_id_value = [f'T-{elem:03}' for elem in np.random.choice(self.num_turbine,
                                                                             self.num_turbine)]     # Creazione dei valori del campo turbine_id (T001, T002...); il :03 indica che devono esserci 3 zeri come format


    # Modulo di creazione dei dati 'tubines_metadata.csv' ----------------------------------------------
    def generate_metadata(self) -> None:
        """
        Genera l'anagrafica (master data) delle turbine eoliche.

        Il metodo crea un dataset strutturato che identifica ogni asset della flotta,
        definendo caratteristiche statiche come modello, locazione geografica e
        capacità nominale. I dati generati servono come tabella di dimensione per
        le analisi di join con i dati telemetrici.

        Returns:
            pd.DataFrame: Un DataFrame contenente le seguenti colonne:

                - turbine_id:           Identificativo univoco della turbina.
                - model:                Modello tecnologico del produttore.
                - location:             Sito geografico di installazione.
                - installation_date:    Data di messa in servizio dell'asset.
                - capacity_kw:          Potenza massima generabile in Kilowatt.

        Note:
            Il file risultante viene salvato automaticamente nel percorso
            definito nella configurazione 'output:folder'.
        """

        self.logger.info("Applicazione avviata", extra={'section': 'GENERAZIONE DEI DATI SORGENTI'})

        models_available = ['SG-170-15', 'V150-4.2', 'GE-Haliade-X', 'SG-132-3.4', 'V112-3.3']          # Definiamo i possibili modelli di turbine (es. Siemens, Vestas, GE)
        models_fake = np.random.choice(models_available, self.num_turbine)                              # Generazione di 1000 modelli casuali dalla lista

        locations_available = ['North-Sea-Offshore',                                                    # Definiamo le locazioni (es. Parchi eolici famosi o coordinate)
                                'Sicilia-Enna-West',
                                'Puglia-Foggia-North',
                                'Sardegna-Oristano',
                                'Toscana-Piombino']

        locations_fake = np.random.choice(locations_available, self.num_turbine)

        data_start = pd.Timestamp("2020-01-01 09:00:00")                                                # Data di installazione
        increment = np.random.choice(self.config['simulation']['interval_minutes'], self.num_turbine)
        installation_date = [data_start + datetime.timedelta(days=int(elem)) for elem in increment]     # Creiamo le 10 date di installazione

        capacity_min = self.config['business_rules']['capacity_range'][0]
        capacity_max = self.config['business_rules']['capacity_range'][1]
        capacity_kw = np.random.choice(random.randint(capacity_min, capacity_max), self.num_turbine)    # Creiamo 10 valori di capacità per ciascuna turbina.

        turbine_model = {
            'turbine_id': self.turbine_id_value,
            'model': models_fake,
            'location': locations_fake,
            'installation_date': installation_date,
            'capacity_kw': capacity_kw
        }

        df = pd.DataFrame(turbine_model)
        df.to_csv(os.path.join(self.folder_project, self.config['output']['folder'], 'turbines_metadata.csv'), index=False)
        self.logger.info(f"\n\nScritti i dati su file sorgente:{os.path.join(self.folder_project , 'raw_data', 'turbines_metadata.csv')}...\n\n"
                    , extra={'section': 'SCRITTURA FILE urbines_metadata.csv'})
        self.logger.info(f"\n\nOutput:\n{df.head(5)}", extra={'section': 'SCRITTURA FILE urbines_metadata.csv'})

        return df




# Modulo di creazione dei dati 'sensor_data.csv' ---------------------------------------------------

    def generate_telemetry(self) -> None:

        """
        Genera i dati telemetrici sintetici per i sensori delle turbine.

        La funzione esegue le seguenti operazioni:
        1. Genera una serie temporale basata su incrementi definiti nel config.
        2. Calcola metriche fisiche (RPM, Power Output).
        3. Applica logiche di business (es. cut-in speed per la produzione elettrica).
        4. Introduce anomalie sintetiche (NULL, Outliers, Duplicati) per test di resilienza.
        5. Salva il dataset risultante in formato CSV nella cartella di output definita.

        Returns:
            None: La funzione scrive direttamente il file sul filesystem.

        Raises:
            KeyError: Se mancano parametri necessari nel file di configurazione YAML.
            OSError: Se non è possibile scrivere il file nella cartella di destinazione.
        """

        num_measurements = self.config['simulation']['num_measurements']

        turbine_id_value = np.random.choice(self.turbine_id_value,num_measurements)                 # Selezioniamo le rilevazioni dei 10 sensori

        data_start = pd.Timestamp("2023-01-01 09:00:00")                                            # Definisco una data di inizio
        increment = np.random.choice(self.config['simulation']['interval_minutes'],
                                     num_measurements).cumsum()                                     # Definisco casualmente l'intervallo di incremento e faccio una somma cumulata
        timestamp_value = data_start + pd.to_timedelta(increment, unit='min')                       # Sommo alla data iniziale questo incremento cumulato a intervalli di 1,5,10 minuti

        # Velocità del vento in ms
        wind_speed_ms_value = [np.random.uniform(0, self.config['business_rules']['wind_cut_in_speed']) for _ in range(num_measurements)]

        # Creazione del campo rpm
        power_output_kw_value = [0 if elem < 3 else 10 * elem for elem in wind_speed_ms_value]
        rpm_value = [elem*60 for elem in wind_speed_ms_value]

        temp_min = self.config['business_rules']['temp_range'][0]
        temp_max = self.config['business_rules']['temp_range'][1]

        # Creazione del campo temperature_celsius float (uniform) (compreso tra 25 e 85 gradi)
        temperature_celsius_value = [round(random.uniform(temp_min, temp_max), 2) for _ in range(num_measurements)]

        vibration_min = self.config['business_rules']['vibration_range'][0]
        vibration_max = self.config['business_rules']['vibration_range'][1]

        # Creazione del campo vibration_level
        vibration_index_value = [round(random.uniform(vibration_min, vibration_max), 2) for _ in range(num_measurements)]


        # Creazione dei files csv

        variabili = {                                                                               # Creiamo un dizionario per generare il Dataframe
        'timestamp': timestamp_value,
        'turbine_id': turbine_id_value,
        'wind_speed_ms': wind_speed_ms_value,
        'rpm': rpm_value,
        'power_output_kw_': power_output_kw_value,
        'temperature_c': temperature_celsius_value,
        'vibration_index':vibration_index_value
        }


        # Creazione del DataFrame
        df_pandas = pd.DataFrame(variabili)

        # Null
        null_indices = np.random.choice(df_pandas.index, self.config['data_quality']['null_records'],
                                         replace=False)                                             # Scegliamo 'x' indici casuali per rendere nulle alcune temperature e vibrazioni
        df_pandas.loc[null_indices, ['temperature_c', 'vibration_index']] = None                    # Generiamo dei valori 'null' volontariamente

        # Outliers
        outlier_indices = np.random.choice(df_pandas.index,
                                           self.config['data_quality']['outlier_records'],
                                            replace=False)                                          # Creiamo 'x' record con valori assurdi
        df_pandas.loc[outlier_indices[0:5], 'temperature_c'] = 550.0                                # Surriscaldamento impossibile
        df_pandas.loc[outlier_indices[5:10], 'rpm'] = -999.0                                        # Errore elettronico

        # Duplicati
        duplicates = df_pandas.sample(self.config['data_quality']['duplicate_rows'])                   # Selezioniamo 'x' righe a caso da duplicare
        df_pandas = pd.concat([df_pandas, duplicates], ignore_index=True)


        # Mischiamo i dati
        df_pandas = df_pandas.sample(frac=1).reset_index(drop=True)                                 # Dopo aver aggiunto i duplicati in coda, il dataset è "ordinato" in modo innaturale. Mischiamo il 100% del dataframe per non avere i duplicati tutti alla fine


        # Scriviamo il dataset
        df_pandas.to_csv(os.path.join(self.folder_project, 'raw_data', 'sensor_data.csv'), index=False)
        self.logger.info(f"\n\nScritti i dati su file sorgente:{os.path.join(self.folder_project, 'raw_data', 'sensor_data.csv')}...\n\n"
                    , extra={'section': 'SCRITTURA FILE sensor_data.csv'})

        self.logger.info(f"\n\n {df_pandas.head()}", extra={'section': 'SCRITTURA FILE sensor_data.csv'})

        return df_pandas


    def run(self):
        """ Metodo che genera entrambi i files """
        self.generate_metadata()
        self.generate_telemetry()





if __name__ == "__main__":

    # Crea un logger con il nome del modulo corrente
    logger = logging.getLogger(f"{Path(__file__)} - {__name__} ")                          # In questo modo istanziamo il logger in base a dove viene eseguito.


    folder_script = Path(__file__).parent.resolve()
    folder_project = folder_script.parent.parent

    generator_data = TurbineDataGenerator(os.path.join(folder_project,'config','simulation_config.yaml'))
    generator_data.run()

    logger.info("Creazione files avvenuta con successo!",  extra={'section': 'CREAZIONE FILE DATI SORGENTI'})
