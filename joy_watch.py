import pandas as pd
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Ruta del archivo CSV
csv_file = "./records_ant/data_readings.csv"


# Inicializar DataFrame
df = pd.DataFrame()


class CSVHandler(FileSystemEventHandler):
    def on_modified(self, event):
        global df
        if event.src_path.endswith(".csv"):  # type: ignore
            print(f"Archivo modificado: {event.src_path}")
            df = pd.read_csv(csv_file)
            print(df.tail())  # Muestra las últimas filas agregadas


# Configurar el observador
observer = Observer()
event_handler = CSVHandler()
observer.schedule(event_handler, path=csv_file, recursive=False)
observer.start()
print("Observando cambios en el archivo CSV...")

try:
    while True:
        time.sleep(1)  # Mantener el script en ejecución
except KeyboardInterrupt:
    observer.stop()

observer.join()
