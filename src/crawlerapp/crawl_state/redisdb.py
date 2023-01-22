import json
from hashlib import sha256
from typing import Dict

from redis import ConnectionPool
from redis import Redis

import configs
from crawlerapp import Interval
from crawlerapp import logger
from crawlerapp.crawl_state.interfaces import UrlCrawlState


class RedisUrlCrawlState(UrlCrawlState):


    connection_pool: ConnectionPool = None

    def __init__(self, sanitized_url: str):
        self.url = sanitized_url
        self.url_hash = sha256(self.url.encode('utf-8')).hexdigest()
        self.db: Redis = self._create_db_session()
        self.state: Dict = None

    @classmethod
    def initialize_db_connection(cls) -> None:
        # TODO complete the following
        cls.connection_pool = ConnectionPool.from_url(url=configs.CRAWL_STATE_BACKEND_REDIS_URI,
                                                      max_connections=80)

    @classmethod
    def close_db_connection(cls) -> None:
        if cls.connection_pool:
            cls.connection_pool.disconnect()

    def _create_db_session(self) -> Redis:
        return Redis(connection_pool=self.connection_pool)

    def retrieve_crawl_state(self):
        with Interval() as dt:
            state_json: str = self.db.get(self.url_hash)
        logger.info(f'read milliseconds: {dt.milisecs}')
        if state_json:
            self.state = json.loads(state_json)
        else:
            self.state = dict(status=None, url=self.url)

    def is_url_seen(self) -> bool:
        if self.state['status'] is None:
            return False
        return self.state['status'] >= self.SEEN_FLAG

    def is_url_page_downloaded(self) -> bool:
        if self.state['status'] is None:
            return False
        return self.state['status'] == self.PAGE_DOWNLOADED_FLAG

    def flag_seen(self) -> None:
        self.state['status'] = self.SEEN_FLAG
        with Interval() as dt:
            self.db.set(name=self.url_hash, value=json.dumps(self.state), ex=self.SEEN_TIME_THRESHOLD)
        logger.info(f'write insert milliseconds: {dt.milisecs}')

    def flag_url_page_downloaded(self) -> None:
        self.state['status'] = self.PAGE_DOWNLOADED_FLAG
        with Interval() as dt:
            self.db.set(name=self.url_hash, value=json.dumps(self.state), ex=None)
        logger.info(f'write insert milliseconds: {dt.milisecs}')

    def _url_seen_with_time_span(self):
        return True


if __name__ == '__main__':
    import time
    uri = configs.CRAWL_STATE_BACKEND_REDIS_URI
    uri = f'{uri[:-1]}1'
    configs.CRAWL_STATE_BACKEND_REDIS_URI = uri

    ttl_time = 2
    cls = RedisUrlCrawlState
    cls.SEEN_TIME_THRESHOLD = ttl_time
    cls.initialize_db_connection()

    url = 'https://aayushkarki/path/1'
    scb = cls(sanitized_url=url)
    scb.db.flushdb()
    scb.retrieve_crawl_state()
    assert scb.is_url_seen() is False
    assert scb.is_url_page_downloaded() is False
    assert scb.should_ignore() is False

    scb.flag_seen()
    assert scb.is_url_seen() is True
    assert scb.is_url_page_downloaded() is False
    assert scb.should_ignore() is True

    scb = cls(sanitized_url=url)
    scb.retrieve_crawl_state()
    assert scb.is_url_seen() is True
    assert scb.is_url_page_downloaded() is False
    assert scb.should_ignore() is True

    scb = cls(sanitized_url=url)
    scb.retrieve_crawl_state()
    assert scb.is_url_seen() is True
    assert scb.state['status'] is not None
    assert scb.should_ignore() is True
    time.sleep(ttl_time+2)
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

    time.sleep(ttl_time + 2)
    scb.retrieve_crawl_state()
    scb = cls(sanitized_url=url)
    scb.retrieve_crawl_state()
    assert scb.is_url_page_downloaded() is True
    assert scb.is_url_seen() is True
    assert scb.should_ignore() is True

    scb.db.flushdb()
    cls.close_db_connection()
