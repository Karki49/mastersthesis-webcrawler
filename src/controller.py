from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from crawlerapp.spiders.testspider import TestSpider


if __name__ == '__main__':
    # pp(dict(proj_settings))
    proj_settings = get_project_settings()
    for (k,v) in dict(proj_settings).items():
        continue
        print(k, v)

    DOWNLOADER_MIDDLEWARES = dict(proj_settings)['DOWNLOADER_MIDDLEWARES']
    print(dict(DOWNLOADER_MIDDLEWARES))
    process = CrawlerProcess(settings=proj_settings)
    process.crawl(crawler_or_spidercls=TestSpider)
    process.start()
