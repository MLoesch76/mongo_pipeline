"""
Einstiegsdatei in die Pipeline

python main.py
"""

from pathlib import Path
from typing import Iterable, Generator

from extract import extract_csv
from transform import transform_all
from load import MongoLoader


DATA_PATH = Path(__file__).parent.parent / "data"


def batch(iterable: Iterable, size: int) -> Generator[list, None, None]:
    """Teilt ein beliebes Iterable in gleich große Batches auf.

    Yields:
        Liste mit jeweils `size` Einträgen.
    """
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    # der Rest, der noch übrig bleibt
    yield batch


def main() -> None:
    """
    Einstiegsfunktion, die die ETL-Pipeline ausführt: EXTRACT->TRANSFORM->LOAD
    """

    # 1. Daten extrahieren aus einer CSV-Datei
    raw_rows = extract_csv(DATA_PATH / "sensors.csv")

    # 2. Daten transformieren
    sensor_data = transform_all(raw_rows)

    # 3. Daten in die MongoDB laden
    with MongoLoader(
        url="mongodb://localhost:27017",
        db_name="sensorDB",
        collection="sensordata",
    ) as loader:
        for sensor_batch in batch(sensor_data, size=10):
            number_docs = loader.load(sensor_batch)
            print(f"Batch geladen: {number_docs} Einträge.")

    print("Pipeline finished!")


if __name__ == "__main__":
    main()
