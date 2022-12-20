import scrapy
from scrapy import Request
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor

from crawlerapp.crawl_state.interfaces import UrlCrawlState
from crawlerapp.crawl_state.mongodb import MongoUrlCrawlState
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
        self.url_crawl_state__class: UrlCrawlState = kw['urlCrawlState__Class']
        self.url_crawl_state__class.initialize_db_connection()

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def _process_scraped_url(self, sanitized_url: str) -> None:
        url = sanitized_url
        if url in self.scrape_links_seen:
            return

        self.scrape_links_seen.add(url)
        crawl_state = MongoUrlCrawlState(sanitized_url=url)
        crawl_state.retrieve_crawl_state()
        if not crawl_state.should_ignore():
            crawl_state.flag_seen()
            # TODO: Send to celery
        return

    def parse(self, response: Request):
        for link in self.crawl_lxml_link_extractor.extract_links(response=response):
            yield Request(url=link.url, callback=self.parse)

        for link in self.scrape_lxml_link_extractor.extract_links(response=response):
            self._process_scraped_url(link.url)

    @staticmethod
    def close(spider, reason):
        closed = getattr(spider, 'closed', None)
        url_crawl_state__class: UrlCrawlState = spider.urlCrawlState__Class
        url_crawl_state__class.close_db_connection()
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


class SimpleTestSpider1(scrapy.Spider):
    name = "simple_test_1"
    start_urls = []

    def start_requests(self):
        self.log(message="==>>" * 20 + " Spider seed urls started")
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)
