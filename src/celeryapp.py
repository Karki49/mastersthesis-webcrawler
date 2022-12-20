from celery import Celery
from kombu import Queue
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

app = Celery('celeryapp',
             backend='rpc://',
             broker='pyamqp://guest:guest@localhost//')

q_args = {
    # 'x-message-ttl': 15000,
}
kw = {
    'durable': True, 'x-queue-type': 'classic',
    'queue_arguments': q_args}

crawl_job_q = Queue(name='crawl_job_q', **kw)


# download_page_q = Queue(name='q6', **kw)
# app.conf.task_queues = (
#     crawl_job_q,
# )


@app.task(queue=crawl_job_q.name)
def test_function():
    print('==' * 15, 'whatever')


@app.task(queue=crawl_job_q.name)
def start_spider(spider_name: str):
    c = CrawlerProcess(settings=get_project_settings())
    c.crawl(spider_name)
    c.start()
    c.stop()
    # print('===' * 20, 'simulate spider start...')
    # import time
    # time.sleep(3)


if __name__ == '__main__':
    from crawlerapp.spiders.testspider import SimpleTestSpider1

    start_spider.delay(spider_name=SimpleTestSpider1.name)
    # start_spider.delay(spider_name=Law360Spider.name)

    print("crawl tasks sent to queue")
