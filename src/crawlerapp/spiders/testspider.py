import random

import scrapy
from scrapy import Request
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.utils.project import get_project_settings

from crawlerapp.utility.urls import remove_fragments
from crawlerapp.utility.urls import sanitize_url


class SpiderSuperClass(scrapy.Spider):
    name = "Not Implemented Spider"
    start_urls = []

    crawl_lxml_link_extractor: LxmlLinkExtractor = None

    scrape_lxml_link_extractor: LxmlLinkExtractor = None


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
        # for url in urls:
        #     if cnt<2:
        #         cnt += 1
        #         yield Request(url=url, callback=self.parse)



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

if __name__ == '__main__':
    Law360Spider()
    exit(0)
    c = CrawlerProcess(settings=get_project_settings())
    # c = CrawlerProcess(settings=None)


    c.crawl(Law360Spider)
    c.start()
    print('xxx' * 25)
    c.stop()
