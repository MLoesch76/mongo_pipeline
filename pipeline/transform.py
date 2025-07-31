from typing import Iterable, Generator
from datetime import datetime
from models import SensorData


def transform_row(row: dict[str, str]) -> SensorData | None:
    '''
    Wandelt ein rohes Zeilen-Dict in ein SensorData Objekt um.
    Die Validierung findet in den Settern statt.

    Returns:
        SensorData-Objekt oder None
    '''

    # Versuche ein SensorData-Objekt zu erstellen. Falls klappt gib es zurück
    # Falls nicht klappt gebe None zurück
    try:
        return SensorData(
            sensor_id = row['sensor_id'],
            timestamp = datetime.fromisoformat(row ['timestamp']),
            value = float(row['value'])
        )
    except (ValueError, TypeError) as e:
        print(f'Skipping invalid row {row}: {e}')
        return None

def transform_all(rows: Iterable[dict]) -> Generator[SensorData, None, None]:
    '''
    Args:
        rows: Iterable von Zeilen Dicts

    Example einer row:
        {"sensor_id": "A1", "timestamp": "2025-07-29T10:00:24", "value": "22.2"}
    '''

    for row in rows:
        obj = transform_row(row)
        if obj:
            yield obj