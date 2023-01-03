from celery import Celery
from kombu import Queue
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

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
    c = CrawlerProcess(settings=get_project_settings())
    kw = {'urlCrawlState__Classname': url_crawl_state_classname}
    c.crawl(spider_name, **kw)
    c.start()
    c.stop()


if __name__ == '__main__':
    from crawlerapp.spiders.testspider import Law360Spider

    # start_spider.delay(spider_name=SimpleTestSpider1.name)
    start_spider(spider_name=Law360Spider.name, url_crawl_state_classname='MongoUrlCrawlState')

    print("crawl tasks sent to queue")

