from celery import Celery
from kombu import Queue
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from crawlerapp import logger

q_args = {
    # 'x-message-ttl': 15000,
}
kw = {
    'durable': True, 'x-queue-type': 'classic',
    'queue_arguments': q_args}

crawl_job_q = Queue(name='crawl_job_q', **kw)
results_q = Queue(name='results_backend_q', **kw)

app = Celery('celeryapp',
             # backend='rpc://',
             backend=None,
             broker='pyamqp://guest:guest@localhost//')

# download_page_q = Queue(name='q6', **kw)
# app.conf.task_queues = (
#     crawl_job_q,
# )


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


if __name__ == '__main__':
    from crawlerapp.spiders.testspider import Law360Spider, NYTimesSpider, CnnSpider, RottenTomatoesSpider

    url_crawl_state_classname_list = ('RedisUrlCrawlState', 'MongoUrlCrawlState', 'ScyllaUrlCrawlState')
    # for spider in (Law360Spider, NYTimesSpider, CnnSpider, RottenTomatoesSpider):
    for spider in (Law360Spider, NYTimesSpider, CnnSpider, RottenTomatoesSpider):
        start_spider.delay(spider_name=spider.name, url_crawl_state_classname=url_crawl_state_classname_list[0])

    print("crawl tasks sent to queue")

