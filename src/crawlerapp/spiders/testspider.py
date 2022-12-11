import random
from pprint import pprint

import scrapy
from scrapy import Request
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.spiders import SitemapSpider
from scrapy.utils.project import get_project_settings


class TestSpider(scrapy.Spider):
    name = "testspider"
    start_urls = [
            # 'https://quotes.toscrape.com/page/2/',
            # 'https://quotes.toscrape.com/page/3/',
        'https://www.law360.com/industries/specialized-health-services/articles'
        ]

    crawl_lxml_link_extractor = LxmlLinkExtractor(
                      allow_domains=['law360.com'],
                      allow=[r'https://'],
                      deny=[r'\?', '^http:'],
                      process_value=lambda url: url)

    scrape_lxml_link_extractor = LxmlLinkExtractor(
                      allow_domains=['law360.com'],
                      allow=[r'https://'],
                      deny=[r'\?', '^http:'],
                      process_value=lambda url: url)

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)


    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response: Request):
        links_scrape = self.scrape_lxml_link_extractor.extract_links(response=response)
        filename = f'test.html'
        urls = [link.url for link in self.crawl_lxml_link_extractor.extract_links(response=response)]

        with open(f'/tmp/{filename}', 'a') as f:
            # for link in links_scrape:
            #     f.write(link.url+'\n')
            f.write(f"Crawled:{len(urls)} "+response.url + '\n')
        self.log(f'Saved file {filename}')

        cnt = 0
        random.shuffle(urls)
        for url in urls:
            if cnt<2:
                cnt += 1
                yield Request(url=url, callback=self.parse)


if __name__ == '__main__':
    c = CrawlerProcess(settings=get_project_settings())
    # c = CrawlerProcess(settings=None)

    c.crawl(TestSpider)
    c.start()
    print('xxx' * 25)
    c.stop()

