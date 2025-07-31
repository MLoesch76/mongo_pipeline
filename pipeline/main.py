'''
Einstiegsdatei in die Pipeline

python main.py          um das projekt zu starten
'''

from pathlib import Path
from typing import Iterable, Generator

from extract import extract_csv
from transform import transform_all
from load import MongoLoader

DATA_PATH = Path(__file__).parent.parent / "data" 

def batch(iterable: Iterable, size: int) -> Generator[list, None, None]:
    ''' 
    Teilt ein beliebiges Iterable in gleich große Batches auf.
     
    Yields:
        Liste mit jeweils 'size' Einträgen
    '''
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    # der Rest der noch übrig bleibt
    yield batch

def main() -> None:
    '''
    Einstiegsfunktion, die die ETL-Pipeline ausführt: EXTRACT -> TRANSFORM -> LOAD

    '''
    # 1. Daten extrahieren aus einer CSV Datei

    # Prüfung ob Datei geöffnet werden kann
    # raw_rows = open(DATA_PATH / "sensor.csv")
    # print(raw_rows)
    # raw_rows.close()

    raw_rows = extract_csv(DATA_PATH / "sensor.csv")

    # Ausgabe zeilenweise
    # print(next(raw_rows))

    # Um alle auszugeben und nicht nur zeilenweise
    # print(list(raw_rows))

    # 2. Daten transformieren
    sensor_data = transform_all(raw_rows)
    # Augabe einer Zeile
    # print(next(sensor_data))
    # Um alle auszugeben und nicht nur zeilenweise
    # print(list(raw_rows))

    # 3. Daten in die MongoDB laden
    with MongoLoader(
        url = 'mongodb://localhost:27017',
        db_name = 'sensordb',
        collection = 'sensordata'
    ) as loader:
        for sensor_batch in batch(sensor_data, size = 10):      # Listen der Länge 10
            number_docs = loader.load(sensor_batch)
            # print(sensor_batch)
            print(f'Batch geladen: {number_docs} Einträge.')

    print('Pipeline finished!')


# print('Name:', __name__)                # gibt __main__ zurück

if __name__ == '__main__':              # nimmt Namen an des aktuellen moduls: hier also main aber könnte auch anders sein
    main()                              # Wenn ich importiere gibt es aber den Namen der importierten Datei zurück
    