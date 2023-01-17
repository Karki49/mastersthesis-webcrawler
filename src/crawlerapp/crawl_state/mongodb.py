import datetime
from functools import cached_property
from hashlib import sha256
from typing import Dict

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from tldextract import tldextract

import configs
from crawlerapp import Interval
from crawlerapp import logger
from crawlerapp.crawl_state.interfaces import UrlCrawlState


# MONGO_CLIENT = MongoClient(configs.CRAWL_STATE_BACKEND_MONGODB_URI)

class MongoUrlCrawlState(UrlCrawlState):
    """
    use crawl_state_db;
    db['<domain>'].createIndex({"_id":1});
    db['<domain>'].createIndex({"dt":1}, { expireAfterSeconds: 3600 });

    Eg:
    use all-hostnames;
    db['nytimes.com'].createIndex({"_id":1});
    db['nytimes.com'].createIndex({"dt":1}, { expireAfterSeconds: 3600 });

    data structure:
    {
        _id: ObjectId()
        hash_url: unique string
        status: int, >=0
        url: string
        ttl_field: utc datetime
    }

    """

    mongo_database: Database = None
    DB_NAME = 'crawl_state_db'

    def __init__(self, sanitized_url: str):
        assert sanitized_url
        self.sanitized_url: str = sanitized_url
        self.url_hash = sha256(self.sanitized_url.encode('utf-8')).hexdigest()
        self.db_client: Database = self._create_db_session()
        self.state: Dict = None
        self.__is_state_in_db: bool = None

    @classmethod
    def initialize_db_connection(cls) -> None:
        cls.mongo_client = MongoClient(configs.CRAWL_STATE_BACKEND_MONGODB_URI)
        cls.mongo_database = cls.mongo_client[cls.DB_NAME]

    @classmethod
    def close_db_connection(cls) -> None:
        assert cls.mongo_database is not None
        cls.mongo_database.client.close()

    def _create_db_session(self) -> Database:
        # Mongodb Database object does not implement truth, hence `is not None` necessary
        assert self.mongo_database is not None
        return self.mongo_database

    @cached_property
    def collection_name(self) -> str:
        # extracts google from https://abc.google.com,  https://www.google.com:8080/path,  https://www.google.co.uk,  https://www.google.in/whatever
        name = tldextract.extract(self.sanitized_url).domain
        return name

    def retrieve_crawl_state(self) -> None:
        with Interval() as dt:
            dict_ = self.db_client[self.collection_name].find_one({"url_hash": self.url_hash}, {"_id": False, "status":True})
        logger.info(f'mongodb retrieve miliseconds: {dt.milisecs}')
        if dict_:
            self.state = dict(url_hash=self.url_hash, url=self.sanitized_url, status=dict_.get('status'))
            self.__is_state_in_db: bool = True
        else:
            self.state = dict(url_hash=self.url_hash, url=self.sanitized_url, status=None)
            self.__is_state_in_db: bool = False
        return

    def _is_state_in_db(self) -> bool:
        assert self.state
        return self.__is_state_in_db is True

    def is_url_seen(self) -> bool:
        if not self._is_state_in_db():
            return False
        return self.state['status'] is not None and self.state['status'] >= self.SEEN_FLAG

    def is_url_page_downloaded(self) -> bool:
        if not self._is_state_in_db():
            return False
        return self.state['status'] is not None and self.state['status'] == self.PAGE_DOWNLOADED_FLAG

    def _upsert_state(self, target_status: int, persist:bool) -> None:
        if not self._is_state_in_db():
            if persist:
                new_state = self.state | dict(status=target_status)
            else:
                new_state = self.state | dict(status=target_status, ttl_field=datetime.datetime.utcnow())
            with Interval() as dt:
                self.db_client[self.collection_name].insert_one(new_state)
            logger.info(f'mongodb insert miliseconds: {dt.milisecs}')
            self.state = new_state
        else:
            filter_ = {'url_hash': self.url_hash}
            if persist:
                update_ = {'$set': {'status': target_status}, '$unset': {'ttl_field': ''}}
            else:
                update_ = {'$set': {'status': target_status}}
            with Interval() as dt:
                self.db_client[self.collection_name].update_one(filter=filter_,
                                                                update=update_)
            logger.info(f'mongodb update miliseconds: {dt.milisecs}')
            self.state['status'] = target_status
        self.__is_state_in_db = True

    def flag_seen(self) -> None:
        self._upsert_state(target_status=self.SEEN_FLAG, persist=False)

    def flag_url_page_downloaded(self) -> None:
        self._upsert_state(target_status=self.PAGE_DOWNLOADED_FLAG, persist=True)

    # def _url_seen_with_time_span(self):
    #     return self.state['status'] is None

    def _url_seen_with_time_span(self):
        return True

if __name__ == '__main__':
    import time
    cl = MongoClient(configs.CRAWL_STATE_BACKEND_MONGODB_URI)

    ttl_time = 2
    cls = MongoUrlCrawlState
    cls.DB_NAME = 'test'
    MongoUrlCrawlState.SEEN_TIME_THRESHOLD = ttl_time
    MongoUrlCrawlState.initialize_db_connection()

    col : Collection = cl[cls.DB_NAME].aayushkarki
    col.drop()
    col.create_index('url_hash', unique=True)
    col.create_index('ttl_field', expireAfterSeconds=ttl_time)
    time.sleep(5)


    url = 'https://aayushkarki/path/1'
    scb = MongoUrlCrawlState(sanitized_url=url)
    scb.retrieve_crawl_state()
    assert scb.is_url_seen() is False
    assert scb.is_url_page_downloaded() is False
    assert scb.should_ignore() is False

    scb.flag_seen()
    assert scb.is_url_seen() is True
    assert scb.is_url_page_downloaded() is False
    assert scb.should_ignore() is True

    scb = MongoUrlCrawlState(sanitized_url=url)
    scb.retrieve_crawl_state()
    assert scb.is_url_seen() is True
    assert scb.is_url_page_downloaded() is False
    assert scb.should_ignore() is True

    scb = MongoUrlCrawlState(sanitized_url=url)
    scb.retrieve_crawl_state()
    assert scb.is_url_seen() is True
    assert scb.state['status'] is not None
    assert scb.should_ignore() is True
    time.sleep(ttl_time+40) # MONGODB takes a lot of time to remote TTL field
    scb.retrieve_crawl_state()
    assert scb.is_url_seen() is False
    assert scb.state['status'] is None
    assert scb._url_seen_with_time_span() is True
    assert scb.is_url_page_downloaded() is False
    assert scb.should_ignore() is False
    scb.flag_url_page_downloaded()
    assert scb.is_url_seen() is True
    assert scb.is_url_page_downloaded() is True
    assert scb.should_ignore() is True

    time.sleep(ttl_time + 30) # MONGODB takes a lot of time to remote TTL field
    scb.retrieve_crawl_state()
    scb = MongoUrlCrawlState(sanitized_url=url)
    scb.retrieve_crawl_state()
    assert scb.is_url_page_downloaded() is True
    assert scb.is_url_seen() is True
    assert scb.should_ignore() is True

    print("---cleaning up---")
    cls = MongoUrlCrawlState
    col.drop()
    MongoUrlCrawlState.close_db_connection()