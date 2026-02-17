import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import os
import sys


# Definiamo la radice del progetto risalendo di un livello da utils/
ROOT_DIR = Path(__file__).parent.parent.resolve()


class SectionFilter(logging.Filter):                                                                # Aggiunge la sezione di default se manca
    def filter(self, record):
        if not hasattr(record, 'section'):
            record.section = 'GENERALE'
        return True


def setup_logging():
    # Configurazione di base
    log_level = os.environ.get("LOGLEVEL", "INFO").upper()

    # 1. Definiamo il File Handler rotativo
    file_handler = RotatingFileHandler(
        filename=os.path.join(ROOT_DIR, 'logs', 'app.log'),
        maxBytes=5*1024*1024,                                                                       # Dimensione max prima che il file ruoti
        backupCount=3,                                                                              # Numero di file storici da conservare sul disco
        encoding='utf-8'
    )

    # 2. Definiamo la Console Handler
    console_handler = logging.StreamHandler()


    # 3. Applichiamo il filtro "sezione" sia al file che console handler
    file_handler.addFilter(SectionFilter())
    console_handler.addFilter(SectionFilter())


    # 4. Configuriamo il logger
    logging.basicConfig(
        handlers=[file_handler, console_handler],                                                   # Oltre che scrivere su file scriviamo su output quindi non avremo bisogno di print.
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(section)s] - %(message)s\n',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Metodo che evita che crash silenziosi passino inosservati
    def handle_exception(exc_type, exc_value, exc_traceback):                                       # Ci assicuriamo che intercettiamo errori non in try-catch e scriviamo in log
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)                                  # Se l'eccezione è un interruzione da utente (tastiera) , non scriviamo nulla nel log
            return
        logging.error("Eccezione non gestita", exc_info=(exc_type, exc_value, exc_traceback))       # Altrimenti lo scriviamo

    sys.excepthook = handle_exception                                                               # Sovrascriviamo il gestore errori predefinito con il nuovo che è integrato con il log

    return None
