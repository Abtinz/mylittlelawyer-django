from __future__ import annotations

from django.conf import settings
from pymongo import MongoClient

_client: MongoClient | None = None


def get_mongo_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(settings.MONGODB_URI)
    return _client


def get_mongo_db():
    return get_mongo_client()[settings.MONGODB_DB_NAME]
