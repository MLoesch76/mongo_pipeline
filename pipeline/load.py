from types import TracebackType
from typing import Iterable
from contextlib import AbstractContextManager
# import logging

from pymongo import MongoClient
from pymongo.collection import Collection

from models import SensorData

# logger = logging.getLogger('load logger')

class MongoLoader(AbstractContextManager):
    def __init__(self, url: str, db_name: str, collection: str):
        # self.url = url                                    # nicht benötigt da nur hier lokal
        # self.db_name = db_name                            # nicht benötigt da nur hier lokal
        self.collection = collection

        self.client = MongoClient(url)                      # wird in exit funktion genutzt
        self.collection = self.client[db_name][collection]  # wird in load funktion genutzt

    def load(self, sensor_data: Iterable[SensorData]) -> int:
        '''
        Trage die Sensordaten in die MongoDB.

        Returns:
            Anzahl der eingetragenen Dokumente
        '''
        docs = [entry.to_dict() for entry in sensor_data]
        if docs:
            self.collection.insert_many(docs) # type: ignore
            # print(docs)
            return len(docs)
        # logger.error('Es wurden keine Objekte gespeichert')     # wenn logger genutzt siehe oben
        return 0

    def __exit__(self, *args, **kwargs):
        ''' Wenn der Kontext geschlossen wird. '''
        self.client.close()

if __name__ == '__main__':
    with MongoLoader(
        url = 'mongodb://localhost:27017',
        db_name = 'Herodb',
        collection = 'heroes'
) as loader:
        # loader.load()
        # print(loader)
        print(loader.client)