from celery import Celery
from kombu import Queue
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from configs import RMQ_URI
from crawlerapp import logger
from crawlerapp.etl.page_etl import simulate_etl

q_args = {
    # 'x-message-ttl': 15000,
}
kw = {
    'durable': True, 'x-queue-type': 'classic',
    'queue_arguments': q_args}

crawl_job_q = Queue(name='crawl_job_q', **kw)
page_etl_q = Queue(name='etl_job_q', **kw)
results_q = Queue(name='results_backend_q', **kw)

app = Celery('celeryapp',
             # backend='rpc://',
             backend=None,
             broker=RMQ_URI)


@app.task(queue=crawl_job_q.name)
def test_function():
    print('==' * 15, 'whatever')


@app.task(queue=crawl_job_q.name)
def start_spider(spider_name: str, url_crawl_state_classname:str):
    logger.info(f"spider:{spider_name} started")
    c = CrawlerProcess(settings=get_project_settings())
    kw = {'urlCrawlState__Classname': url_crawl_state_classname}
    c.crawl(spider_name, **kw)
    c.start()
    c.stop()
    logger.info(f"spider:{spider_name} finished")


@app.task(queue=page_etl_q.name)
def page_etl(sanitized_url: str, url_crawl_state_classname: str) -> None:
    simulate_etl(sanitized_url=sanitized_url, UrlCrawlState__class_name=url_crawl_state_classname)


if __name__ == '__main__':
    import sys
    import time
    from crawlerapp.spiders.testspider import Law360Spider, NYTimesSpider, CnnSpider, RottenTomatoesSpider

    url_crawl_state_classname_list = ('RedisUrlCrawlState', 'MongoUrlCrawlState', 'ScyllaUrlCrawlState')
    url_crawl_state_db = sys.argv[1]
    assert url_crawl_state_db in url_crawl_state_classname_list

    print(f"Using class:{url_crawl_state_db}, sending crawl job tasks in 5 seconds...")
    time.sleep(5)

    for spider in (Law360Spider, NYTimesSpider, CnnSpider, RottenTomatoesSpider):
        start_spider.delay(spider_name=spider.name, url_crawl_state_classname=url_crawl_state_db)

    print("crawl tasks sent.")
