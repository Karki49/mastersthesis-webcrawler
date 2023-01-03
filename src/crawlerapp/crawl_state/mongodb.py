from datetime import datetime
from hashlib import sha256
from typing import Dict
from urllib.parse import urlsplit

from pymongo import MongoClient
from pymongo.database import Database

import configs
from crawlerapp.crawl_state.interfaces import UrlCrawlState

MONGO_CLIENT = MongoClient(configs.CRAWL_STATE_BACKEND_MONGODB_URI)


class MongoUrlCrawlState(UrlCrawlState):
    """
    use crawl_state_db;
    db['<domain>'].createIndex({"_id":1});
    db['<domain>'].createIndex({"dt":1}, { expireAfterSeconds: 3600 });

    Eg:
    use all-hostnames;
    db['nytimes.com'].createIndex({"_id":1});
    db['nytimes.com'].createIndex({"dt":1}, { expireAfterSeconds: 3600 });

    """

    mongo_database: Database = None
    DB_NAME = 'crawl_state_db'

    def __init__(self, sanitized_url: str):
        assert sanitized_url
        self.sanitized_url: str = sanitized_url
        self.url_hash = sha256(self.sanitized_url.encode('utf-8')).hexdigest()
        self.db_client: Database = self._create_db_session()
        self.state: Dict = None
        self.__collection_name: str = None

    @classmethod
    def initialize_db_connection(cls) -> None:
        cls.mongo_client = MONGO_CLIENT[cls.DB_NAME]

    @classmethod
    def close_db_connection(cls) -> None:
        assert cls.mongo_database
        cls.mongo_database.client.close()

    def _create_db_session(self) -> Database:
        assert self.mongo_database
        return self.mongo_database

    @property
    def collection_name(self) -> str:
        if not self.__collection_name:
            hostname_port = urlsplit(self.sanitized_url).netloc
            hostname = hostname_port.split(":")[0]
            assert hostname
            self.__collection_name = hostname
        return self.__collection_name

    def retrieve_crawl_state(self) -> None:
        crawl_state = self.db_client[self.collection_name].find_one({"_id": self.url_hash})
        if crawl_state:
            self.state = crawl_state
        else:
            self.state = dict(_id=self.url_hash, url=self.sanitized_url, status=None, ttl_dt=None)
        return

    def is_url_seen(self) -> bool:
        return self.state['status'] >= self.SEEN_FLAG

    def is_url_page_downloaded(self) -> bool:
        return self.state['status'] == self.PAGE_DOWNLOADED_FLAG

    def flag_seen(self) -> None:
        new_state = self.state | dict(status=self.SEEN_FLAG, ttl_dt=datetime.utcnow())
        self.db_client[self.collection_name].insert_one(new_state)
        self.state['status'] = new_state

    def flag_url_page_downloaded(self) -> None:
        new_state = self.state | dict(status=self.PAGE_DOWNLOADED_FLAG, ttl_dt=None)
        self.db_client[self.collection_name].insert_one(new_state)
        self.state['status'] = new_state

    def _url_seen_with_time_span(self):
        return True
