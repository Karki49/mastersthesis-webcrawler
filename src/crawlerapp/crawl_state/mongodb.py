from datetime import datetime
from typing import Dict
from urllib.parse import urlsplit

from pymongo import MongoClient

from crawlerapp.crawl_state.interfaces import UrlCrawlState


class MongoUrlCrawlState(UrlCrawlState):

    mongo_client: MongoClient = None

    def __init__(self, sanitized_url: str):
        assert sanitized_url
        self.sanitized_url: str = sanitized_url
        self.db_client: MongoClient = self._create_connection()
        self.state: Dict = None
        self.__collection_name: str = None

    @property
    def collection_name(self) -> str:
        if not self.__collection_name:
            hostname_port = urlsplit(self.sanitized_url).netloc
            hostname = hostname_port.split(":")[0]
            assert hostname
            self.__collection_name = hostname
        return self.__collection_name

    @classmethod
    def _create_connection(cls) -> MongoClient:
        if cls.mongo_client is None:
            cls.mongo_client = MongoClient(host='')
        return cls.mongo_client

    def retrieve_crawl_state(self) -> None:
        crawl_state = self.db_client[self.collection_name].find_one({"sanitized_url": self.sanitized_url})
        if crawl_state:
            self.state = crawl_state
        else:
            self.state = dict(url=self.sanitized_url, status=None, updated_on=None)
        return

    def is_url_seen(self) -> bool:
        return self.state['status'] == self.SEEN_FLAG

    def is_url_page_downloaded(self) -> bool:
        return self.state['status'] == self.PAGE_DOWNLOADED_FLAG

    def flag_seen(self) -> None:
        self.state['status'] = self.SEEN_FLAG
        self.db.set(name=self.sanitized_url, value=self.state, ex=self.SEEN_TIME_THRESHOLD)

    def flag_url_page_downloaded(self) -> None:
        self.state['status'] = self.PAGE_DOWNLOADED_FLAG
        self.db.set(name=self.sanitized_url, value=self.state, ex=None)

    def _url_seen_with_time_span(self):
        if self.state['updated_on'] is None:
            return True
        t_now = datetime.utcnow()
        if (t_now - self.state['updated_on']).total_seconds() <= self.SEEN_TIME_THRESHOLD:
            return True
        else:
            return False
