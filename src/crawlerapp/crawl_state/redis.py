import json
from typing import Any
from typing import Dict

from crawlerapp import Interval
from crawlerapp import logger
from crawlerapp.crawl_state.interfaces import UrlCrawlState
from redis import Redis, ConnectionPool
from hashlib import sha256


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
        cls.connection_pool = ConnectionPool.from_url(url='redis://localhost:6379/1',
                                                      max_connections=80)
        # cls.connection_pool = ConnectionPool(max_connections=80,
        #                                      host='',
        #                                      port=0,
        #                                      db=0,
        #                                      username=None,
        #                                      password=None)

    @classmethod
    def close_db_connection(cls) -> None:
        if cls.connection_pool:
            cls.connection_pool.disconnect()

    def _create_db_session(self) -> Redis:
        return Redis(connection_pool=self.connection_pool)

    def retrieve_crawl_state(self):
        with Interval() as dt:
            state_json: str = self.db.get(self.url_hash)
        logger.info(f'redis retrieve miliseconds: {dt.milisecs}')
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
        logger.info(f'redis insert miliseconds: {dt.milisecs}')

    def flag_url_page_downloaded(self) -> None:
        self.state['status'] = self.PAGE_DOWNLOADED_FLAG
        with Interval() as dt:
            self.db.set(name=self.url_hash, value=json.dumps(self.state), ex=None)
        logger.info(f'redis insert miliseconds: {dt.milisecs}')

    def _url_seen_with_time_span(self):
        return True
