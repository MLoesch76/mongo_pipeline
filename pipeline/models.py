from datetime import datetime

# D4,2025-07-29T10:00:19,30.5

# Regeln:
# 1. Sensor ID muss gegeben sein
# 2. timestamp muss vom Typ datetime sein
# 3. Wert 0 < value < 100

class SensorData:
    ''' Single source of truth '''
    def __init__(self, sensor_id: str, timestamp: datetime, value: float):
        self.sensor_id = sensor_id
        self.timestamp = timestamp
        self.value = value
    
    @property
    def sensor_id(self) -> str:
        return self._sensor_id
    
    @sensor_id.setter
    def sensor_id(self, value) -> None:
        if not value.strip():
            raise ValueError('Die Sensor ID muss gegeben sein')
        self._sensor_id = value.strip()

    @property
    def value(self) -> str:
        return self._value
    
    @value.setter
    def value(self, value) -> None:
        if not (0 < value < 100):
            raise ValueError('Der Wert muss im folgenden Bereich liegen: 0 < value < 100')
        self._value = value

    def to_dict(self) -> dict:
        return {
            'sensor_id': self.sensor_id,
            'timestamp': self.timestamp,
            'value': self.value
        }

    def __repr__(self) -> str:
        return f'SensorData({self._sensor_id!r}, {self.timestamp!r}, {self._value})'
    
if __name__ == '__main__':
    # Ausgabe
    obj = SensorData(sensor_id="A2", timestamp=datetime.now(), value= 11.1)
    print(obj)
    # erzeugt einen Fehler, da die Sensor ID nicht gegeben ist
    obj = SensorData(sensor_id=" ", timestamp=datetime.now(), value= 11.1)
    print(obj)

