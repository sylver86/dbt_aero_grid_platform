from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import sys
from pathlib import Path
import logging
import yaml
import os
from google.cloud.exceptions import NotFound

                                                                                                    # Definiamo la Root a livello di modulo per usarla ovunque
MODULE_DIR = Path(__file__).parent.parent.parent.resolve()
ROOT_DIR = MODULE_DIR.parent
                                                                                                    # Impostiamo il logger
sys.path.append(str(MODULE_DIR))
from utils.logger_config import setup_logging
setup_logging()



class BigQueryIngestor:

    def __init__(self, config_path_yaml):
        """
        Inizializza la connessione al Dwh BigQuery

            :param config_path_yaml: fornisce il percorso del file YAML da considerare
                                     per l'inizializzazione.

        """

        self.logger = logging.getLogger(f"{Path(__file__).stem} - {__name__}")
        self.folder_project = Path(__file__).parent.parent.parent.resolve()

                                                                                                    # 1. Lettura YAML
        try:
            with open(config_path_yaml, 'r', encoding='utf-8') as file:
                self.config = yaml.safe_load(file)
        except Exception as exc:
            self.logger.error(f"Errore lettura YAML: {exc}", extra={'section': 'INIT'})
            raise

                                                                                                    # 2. Percorso del file avente le credenziali JSON (Usiamo la ROOT assoluta)
        self.path_to_json = ROOT_DIR / "dbt-service-account.json"

        if not self.path_to_json.exists():
            self.logger.error(f"JSON non trovato in: {self.path_to_json}", extra={'section': 'AUTH'})
            raise FileNotFoundError(f"Manca il file credenziali: {self.path_to_json}")


                                                                                                    # 3. Connessione
        credentials = service_account.Credentials.from_service_account_file(str(self.path_to_json))
        self.client = bigquery.Client(
            credentials=credentials,
            project=self.config['connection_raw']['project_id']
        )



    def get_datasets(self):
        """
            Metodo che fornisce la lista dei datasets del Dwh
        """
        return self.datasets



    def create_dataset_if_not_exists(self, dataset_id, location="EU"):
        """
            Verifica l'esistenza di un dataset su DWH e lo crea se manca.
            Prende come parametri:
                - dataset_id: nome del dataset
                - location: dove devono risiedere fisicamente i dati del dwh
        """

        dataset_ref = self.client.dataset(dataset_id)                                               # Costruiamo il riferimento completo: project_id.dataset_id
        try:
            self.client.get_dataset(dataset_ref)                                                    # Proviamo a recuperare il dataset dal cloud
            self.logger.info(f"Il dataset '{dataset_id}' esiste già.",
                             extra={'section': 'CHECK_DATASET BIGQUERY'})
        except NotFound:
            self.logger.info(f"Dataset '{dataset_id}' non trovato. Creazione in corso...",          # Se non viene trovato, lo creiamo
                             extra={'section': 'CREATE_DATASET BIGQUERY'})

            dataset = bigquery.Dataset(dataset_ref)                                                 # Otteniamo un puntatore al dataset di BigQuery
            dataset.location = location                                                             # Definiamo la location

            self.client.create_dataset(dataset, timeout=30)                                         # Procediamo alla creazione
            self.logger.info(f"Dataset '{dataset_id}' creato con successo in {location}.",
                             extra={'section': 'CREATE_DATASET BIGQUERY'})




    def load_table(self, file_path: str, table_name: str, autodetect: bool = True, schema: list = None) -> None:
        """
        Carica un file CSV su una tabella BigQuery con logica WRITE_TRUNCATE.
        Prende come parametri :

            - file_path: percorso del csv da caricare
            - table_name: nome tabella da creare su Dwh
            - autodetect: per determinare automaticamente lo schema
        """

        # 1. Recuperiamo il dataset_id dalla configurazione
        dataset_id = self.config['connection_raw']['dataset_id']

        # Costruiamo il riferimento completo della tabella: project.dataset.table                   # In BigQuery configuriamo gli oggetti TableReference
        table_ref = self.client.dataset(dataset_id).table(table_name)

        # 2. Configurazione del Job di caricamento                                                  # Questo oggetto sostituisce l'interfaccia grafica dei "Flat File Source" di SSIS
        job_config = bigquery.LoadJobConfig(
            skip_leading_rows=1,                                                                    # Salta l'header (la prima riga) del CSV
            source_format=bigquery.SourceFormat.CSV,                                                # Definisce il formato sorgente (può essere CSV, JSON, PARQUET, etc.)
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,                             # WRITE_TRUNCATE: Svuota la tabella e riscrive tutto (simile a TRUNCATE TABLE + INSERT)
        )

        # Gestiamo la logica dello schema in modo dinamico
        if schema:
            job_config.schema = schema                                                              # Se passiamo una lista di SchemaField, forziamo i tipi di dato
            job_config.autodetect = False                                                           # Se c'è uno schema, spegniamo l'autodetect per sicurezza
        else:
            job_config.autodetect = autodetect                                                      # Se non c'è schema, BigQuery scansiona le prime righe per indovinare i tipi (Autodetect)

        self.logger.info(f"Inizio caricamento: {file_path} -> {table_name} (Autodetect: {job_config.autodetect})",
                        extra={'section': 'DATA_INGESTION'})


        # 3. Esecuzione del Job
        try:
            with open(file_path, "rb") as source_file:                                              # Eseguo il job con i parametri impostati.
                load_job = self.client.load_table_from_file(                                        # Il metodo "load_table_from_file" crea un 'Job' sul server di Google.
                    source_file,
                    table_ref,
                    job_config=job_config
                )
                                                                                                    # Attendiamo che il job finisca (metodo bloccante) quindi .result() trasforma l'operazione da asincrona a sincrona (metodo bloccante).
            load_job.result()                                                                       # Mette in pausa l'esecuzione script e restituisce un oggetto "LoadJob"
                                                                                                    # Recuperiamo i dettagli della tabella finale per il log
            destination_table = self.client.get_table(table_ref)
            self.logger.info(f"Caricamento completato con successo. Righe caricate: {destination_table.num_rows}",
                             extra={'section': 'DATA_INGESTION'})

        except Exception as e:
            self.logger.error(f"Errore durante il caricamento su BigQuery: {e}",
                              extra={'section': 'DATA_INGESTION'})
            raise





    def _get_telemetry_schema(self) -> list[bigquery.SchemaField]:
            """Definisce lo schema esplicito per la telemetria per garantire il typing."""
            return [
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("turbine_id", "STRING"),
                bigquery.SchemaField("wind_speed_ms", "FLOAT64"),
                bigquery.SchemaField("rpm", "FLOAT64"),
                bigquery.SchemaField("power_output_kw_", "FLOAT64"),
                bigquery.SchemaField("temperature_c", "FLOAT64"),
                bigquery.SchemaField("vibration_index", "FLOAT64"),
            ]



    def run(self) -> None:
            """Esegue il workflow completo di ingestion."""
                                                                                                    # Costruisci i percorsi dei file (usando pathlib per sicurezza)
            raw_data_path = self.folder_project / self.config['output']['folder']

                                                                                                    # 1. Carica Metadata (Autodetect è sufficiente per dati anagrafici semplici)
            self.load_table(
                file_path=str(raw_data_path / "turbines_metadata.csv"),
                table_name=self.config['connection_raw']['metadata_table'],
                autodetect=True
            )

                                                                                                    # 2. Carica Telemetry (Schema manuale OBBLIGATORIO per i timestamp)
            self.load_table(
                file_path=str(raw_data_path / "sensor_data.csv"),
                table_name=self.config['connection_raw']['telemetry_table'],
                autodetect=False,
                schema=self._get_telemetry_schema()
            )




# Main ---------------------------------------------------------------------------------------------
if __name__ == "__main__":

    import traceback # Importalo qui per il test

    logger = logging.getLogger(Path(__file__).stem)

    # Leggiamo il file di configurazione per la connessione
    path_config_bq = MODULE_DIR / 'config' / 'ingest_config.yaml'

    # Tentiamo la connessione al Datawarehouse BigQuery di GCP
    try:
        # Inizializzazione
        conn = BigQueryIngestor(str(path_config_bq))

        logger.info("Inizializzazione Ingestor completata.", extra={'section': 'CONNESSIONE'})

        # Orchestrazione
        conn.create_dataset_if_not_exists('aero_grid_raw')
        conn.run()

        # Messaggio di successo (Corretto senza int())
        logger.info("🚀 INGESTION COMPLETATA CON SUCCESSO!", extra={'section': 'RUN'})

    except Exception as e:
        logger.error(f"❌ ERRORE FATALE durante l'ingestion: {e}", extra={'section': 'FATAL'})
        traceback.print_exc()
        sys.exit(1)
