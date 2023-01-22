import scrapy
from scrapy import Request
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor

from crawlerapp import logger
from crawlerapp.crawl_state.interfaces import UrlCrawlState
from crawlerapp.utility.urls import remove_fragments
from crawlerapp.utility.urls import sanitize_url


class SpiderSuperClass(scrapy.Spider):
    name = "Not Implemented Spider"
    start_urls = []

    crawl_lxml_link_extractor: LxmlLinkExtractor = None
    scrape_lxml_link_extractor: LxmlLinkExtractor = None

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.scrape_links_seen: set = set()
        self.url_crawl_state__classname: str = kw['urlCrawlState__Classname']
        ## Importing UrlCrawlState class subtypes like this does not make sense at first,
        ## but if i import ScyllaUrlCrawlState outside the spider, it will lead to an error in Twisted Reactor. Not sure why.
        if self.url_crawl_state__classname == 'MongoUrlCrawlState':
            from crawlerapp.crawl_state.mongodb import MongoUrlCrawlState
            class_: UrlCrawlState = MongoUrlCrawlState
        elif self.url_crawl_state__classname == 'RedisUrlCrawlState':
            from crawlerapp.crawl_state.redisdb import RedisUrlCrawlState
            class_: UrlCrawlState = RedisUrlCrawlState
        elif self.url_crawl_state__classname == 'ScyllaUrlCrawlState':
            from crawlerapp.crawl_state.scylladb import ScyllaUrlCrawlState
            class_: UrlCrawlState = ScyllaUrlCrawlState
        else:
            raise Exception(f'{self.url_crawl_state__classname} is not a recongnized UrlCrawlState class name')

        self.url_crawl_state__class: UrlCrawlState = class_
        self.url_crawl_state__class.initialize_db_connection()
        self.scraped_link_count = 0
        self.fetched_pages_count = 0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def _process_scraped_url(self, sanitized_url: str) -> None:
        url = sanitized_url
        if url in self.scrape_links_seen:
            return

        self.scrape_links_seen.add(url)
        crawl_state = self.url_crawl_state__class(sanitized_url=url)
        crawl_state.retrieve_crawl_state()
        if not crawl_state.should_ignore():
            crawl_state.flag_seen()
            # TODO: Send to celery
        return

    def parse(self, response: Request):
        self.fetched_pages_count += 1
        if self.fetched_pages_count <= 400:
            for link in self.crawl_lxml_link_extractor.extract_links(response=response):
                yield Request(url=link.url, callback=self.parse)

        for link in self.scrape_lxml_link_extractor.extract_links(response=response):
            self.scraped_link_count += 1
            self._process_scraped_url(link.url)

    @staticmethod
    def close(spider, reason):
        closed = getattr(spider, 'closed', None)
        url_crawl_state__class: UrlCrawlState = spider.url_crawl_state__class
        logger.info(f'{spider.name} crawl state class is {url_crawl_state__class.__name__}')
        logger.info(f'{spider.name} fetched_pages_count:{spider.fetched_pages_count}')
        logger.info(f'{spider.name} scraped_link_count:{spider.scraped_link_count}')
        url_crawl_state__class.close_db_connection()
        logger.info('crawlstate db connection closed.')
        if callable(closed):
            return closed(reason)


class Law360Spider(SpiderSuperClass):
    name = "law360"

    start_urls = [
        'https://www.law360.com/industries/specialized-health-services/articles'
    ]

    crawl_lxml_link_extractor = LxmlLinkExtractor(
        allow_domains=['law360.com'],
        allow=[r'https://'],
        deny=[r'\?', '^http:'],
        process_value=remove_fragments)

    scrape_lxml_link_extractor = LxmlLinkExtractor(
        allow_domains=['law360.com'],
        allow=[r'https://'],
        deny=[r'\?', '^http:'],
        process_value=sanitize_url)


class NYTimesSpider(SpiderSuperClass):
    name = "nytimes"

    start_urls = [
        'https://www.nytimes.com/section/science',
        'https://www.nytimes.com/section/business',
        'https://www.nytimes.com/section/books',
        'https://www.nytimes.com/section/food',
        'https://www.nytimes.com/section/realestate',
        'https://www.nytimes.com/section/opinion',
    ]

    crawl_lxml_link_extractor = LxmlLinkExtractor(
        allow_domains=['nytimes.com'],
        allow=['^https://.*/science/', '^https://.*/business/',
               '^https://.*/books/', '^https://.*/food/',
               '^https://.*/realestate/', '^https://.*/opinion/'],
        deny=[r'\?', '^http:'],
        process_value=remove_fragments)

    scrape_lxml_link_extractor = LxmlLinkExtractor(
        allow_domains=['nytimes.com'],
        allow=[r'https://'],
        deny=[r'\?', '^http:'],
        process_value=sanitize_url)


class CnnSpider(SpiderSuperClass):
    name = "cnn"

    start_urls = [
        'https://edition.cnn.com/world',
        'https://edition.cnn.com/world/americas',
        'https://edition.cnn.com/world/middle-east'
    ]

    crawl_lxml_link_extractor = LxmlLinkExtractor(
        allow_domains=['cnn.com'],
        allow=['^https://.*/2023/'],
        deny=[r'\?', '^http:'],
        process_value=remove_fragments)

    scrape_lxml_link_extractor = LxmlLinkExtractor(
        allow_domains=['cnn.com'],
        allow=[r'https://', r'^https://.*/202\d/', r'^https://.*/201\d/'],
        deny=[r'\?', '^http:'],
        process_value=sanitize_url)


class RottenTomatoesSpider(SpiderSuperClass):
    name = "rotten_tomatoes"

    start_urls = [
        'https://www.rottentomatoes.com/'
    ]

    crawl_lxml_link_extractor = LxmlLinkExtractor(
        allow_domains=[r'^https://.*/m/', r'^https://.*/tv/', r'^https://.*/article/'],
        allow=['^https://'],
        deny=[r'\?', '^http:'],
        process_value=remove_fragments)

    scrape_lxml_link_extractor = LxmlLinkExtractor(
        allow_domains=['rottentomatoes.com'],
        allow=[r'^https://.*/m/', r'^https://.*/tv/', r'^https://.*/article/'],
        deny=[r'\?', '^http:'],
        process_value=sanitize_url)


class SimpleTestSpider1(SpiderSuperClass):
    name = "simple_test_1"
    start_urls = []
    crawl_lxml_link_extractor = None

    scrape_lxml_link_extractor = None

    def start_requests(self):
        self.log(message="==>>" * 20 + " Spider seed urls started")
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)
