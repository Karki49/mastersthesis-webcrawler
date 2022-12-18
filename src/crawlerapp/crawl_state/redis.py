from typing import Any
from typing import Dict

from crawlerapp.crawl_state.interfaces import UrlCrawlState
from redis import Redis


class RedisUrlCrawlState(UrlCrawlState):

    def __init__(self, sanitized_url: str):
        self.url = sanitized_url
        self.db: Redis = self._connect_to_db()
        self.state: Dict = None

    @classmethod
    def _connect_to_db(cls) -> Any:
        return None and Redis(host='', port='',username='', password='', decode_responses=True, db=0)

    def retrieve_crawl_state(self):
        state: Dict = self.db.get(self.url)
        self.state = state or dict(url=self.url, status=None)

    def is_url_seen(self) -> bool:
        return self.state['status'] == self.SEEN_FLAG

    def is_url_page_downloaded(self) -> bool:
        return self.state['status'] == self.PAGE_DOWNLOADED_FLAG

    def flag_seen(self) -> None:
        self.state['status'] = self.SEEN_FLAG
        self.db.set(name=self.url, value=self.state, ex=self.SEEN_TIME_THRESHOLD)

    def flag_url_page_downloaded(self) -> None:
        self.state['status'] = self.PAGE_DOWNLOADED_FLAG
        self.db.set(name=self.url, value=self.state, ex=None)

    def _url_seen_with_time_span(self):
        return True


if __name__ == '__main__':
    print(RedisUrlCrawlState('dasdsad').should_ignore())