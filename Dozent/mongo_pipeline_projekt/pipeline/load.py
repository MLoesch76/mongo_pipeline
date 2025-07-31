# load.py
from contextlib import AbstractContextManager
from typing import Iterable
import logging

from pymongo import MongoClient
from pymongo.collection import Collection

from models import SensorData

logger = logging.getLogger("load logger")


class MongoLoader(AbstractContextManager):
    def __init__(self, url: str, db_name: str, collection: str):
        # self.url = url
        # self.db_name = db_name
        self.collection = collection

        self.client = MongoClient(url)
        self.collection = self.client[db_name][collection]

    def load(self, sensor_data: Iterable[SensorData]) -> int:
        """Trage die Sensordaten in die MongoDb.

        Returns:
            Anzahl der eingetragenen Dokumente
        """
        docs = [entry.to_dict() for entry in sensor_data]
        if docs:
            self.collection.insert_many(docs)  # type: ignore
            return len(docs)

        return 0

    def __exit__(self, *args, **kwargs):
        """Wenn der Kontext geschlossen wird."""
        self.client.close()


if __name__ == "__main__":
    with MongoLoader(
        url="mongodb://localhost:27017", db_name="heroDB", collection="heroes"
    ) as loader:
        print(loader.client)
