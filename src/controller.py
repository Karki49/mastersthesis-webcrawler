from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from crawlerapp.spiders.testspider import TestSpider


if __name__ == '__main__':
    exit(0)
    process = CrawlerProcess(settings=get_project_settings())
    process.crawl(crawler_or_spidercls=TestSpider)
    process.start()
    process.stop()
