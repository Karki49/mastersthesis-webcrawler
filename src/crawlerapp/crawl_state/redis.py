import json
from typing import Any
from typing import Dict

from crawlerapp.crawl_state.interfaces import UrlCrawlState
from redis import Redis, ConnectionPool
from hashlib import sha256


class RedisUrlCrawlState(UrlCrawlState):

    connection_pool: ConnectionPool = None

    def __init__(self, sanitized_url: str):
        self.url = sanitized_url
        self.url_hash = sha256(self.url.encode('utf-8')).hexdigest()
        self.db: Redis = self._create_connection()
        self.state: Dict = None

    @classmethod
    def _create_connection(cls) -> Any:
        if cls.connection_pool is None:
            #TODO complete the following
            cls.connection_pool = ConnectionPool.from_url(url='redis://localhost:6379/1',
                                                          max_connections=80)
            # cls.connection_pool = ConnectionPool(max_connections=80,
            #                                      host='',
            #                                      port=0,
            #                                      db=0,
            #                                      username=None,
            #                                      password=None)
        return Redis(connection_pool=cls.connection_pool)

    def retrieve_crawl_state(self):
        state_json: str = self.db.get(self.url_hash)
        if state_json:
            self.state = json.loads(state_json)
        else:
            self.state = dict(status=None, url=self.url)

    def is_url_seen(self) -> bool:
        return self.state['status'] == self.SEEN_FLAG

    def is_url_page_downloaded(self) -> bool:
        return self.state['status'] == self.PAGE_DOWNLOADED_FLAG

    def flag_seen(self) -> None:
        self.state['status'] = self.SEEN_FLAG
        self.db.set(name=self.url_hash, value=json.dumps(self.state), ex=self.SEEN_TIME_THRESHOLD)

    def flag_url_page_downloaded(self) -> None:
        self.state['status'] = self.PAGE_DOWNLOADED_FLAG
        self.db.set(name=self.url_hash, value=json.dumps(self.state), ex=None)

    def _url_seen_with_time_span(self):
        return True

