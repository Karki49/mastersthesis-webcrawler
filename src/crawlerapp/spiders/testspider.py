from datetime import datetime

import scrapy
from scrapy import Request
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.spiders import SitemapSpider


class TestSpider(scrapy.Spider):
    name = "testspider"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start_requests(self):
        urls = [
            'https://quotes.toscrape.com/page/2/',
            'https://quotes.toscrape.com/page/3/',
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        page = response.url.split("/")[-2]
        filename = f'quotes-{page}.html'
        with open(f'/tmp/{filename}', 'wb') as f:
            f.write(response.body)
        self.log(f'Saved file {filename}')


class SitemapNYT(SitemapSpider):
    name = 'nyt'
    sitemap_urls = ['https://www.nytimes.com/sitemaps/new/news-1.xml.gz']
    sitemap_rules = [('/opinion/', 'parse_article')]
    allowed_domains = ['www.nytimes.com', 'nytimes.com']

    # def start_requests(self):
    #     for url in self.sitemap_urls:
    #         yield Request(url, callback=self.parse_article)

    def parse_article(self, response):
        print('parse_article url:', response.url)
        yield {'url': response.url}

if __name__ == '__main__':
    c = CrawlerProcess({
    'FEED_FORMAT': 'csv',     # csv, json, xml
    'FEED_URI': 'output.csv', #
    })

    c.crawl(SitemapNYT)
    c.start()
    print('xxx' * 25)
    # c.stop()

