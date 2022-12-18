from abc import ABC
from abc import abstractmethod
from typing import Any


class UrlCrawlState(ABC):
    SEEN_FLAG = 1
    PAGE_DOWNLOADED_FLAG = 2
    SEEN_TIME_THRESHOLD = 3600 # seconds

    @abstractmethod
    def __init__(self, sanitized_url: str):
        pass

    @classmethod
    @abstractmethod
    def _create_connection(cls) -> Any:
        raise Exception('Not Implemented')

    @abstractmethod
    def retrieve_crawl_state(self):
        raise Exception('Not Implemented')

    @abstractmethod
    def is_url_seen(self) -> bool:
        raise Exception('Not Implemented')

    @abstractmethod
    def is_url_page_downloaded(self) -> bool:
        raise Exception('Not Implemented')

    @abstractmethod
    def flag_seen(self) -> None:
        raise Exception('Not Implemented')

    @abstractmethod
    def flag_url_page_downloaded(self) -> None:
        raise Exception('Not Implemented')

    @abstractmethod
    def _url_seen_with_time_span(self):
        raise Exception('Not Implemented')

    def should_ignore(self) -> bool:
        if self.is_url_page_downloaded():
            return True
        if self.is_url_seen() and self._url_seen_with_time_span():
            return True
        return False
