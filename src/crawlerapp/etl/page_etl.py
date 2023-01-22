import time

from crawlerapp.crawl_state.interfaces import UrlCrawlState


def simulate_etl(sanitized_url: str, UrlCrawlState__class_name: str) -> None:
    if UrlCrawlState__class_name == 'MongoUrlCrawlState':
        from crawlerapp.crawl_state.mongodb import MongoUrlCrawlState
        class_: UrlCrawlState = MongoUrlCrawlState
    elif UrlCrawlState__class_name == 'RedisUrlCrawlState':
        from crawlerapp.crawl_state.redisdb import RedisUrlCrawlState
        class_: UrlCrawlState = RedisUrlCrawlState
    elif UrlCrawlState__class_name == 'ScyllaUrlCrawlState':
        from crawlerapp.crawl_state.scylladb import ScyllaUrlCrawlState
        class_: UrlCrawlState = ScyllaUrlCrawlState
    else:
        raise Exception(f'{UrlCrawlState__class_name} is not a recongnized UrlCrawlState class name')

    url_crawl_state: UrlCrawlState = class_(sanitized_url=sanitized_url)

    url_crawl_state.retrieve_crawl_state()
    if url_crawl_state.should_ignore():
        return

    time.sleep(1)  # simulate etl time
    if not url_crawl_state.is_url_page_downloaded():
        url_crawl_state.flag_url_page_downloaded()
