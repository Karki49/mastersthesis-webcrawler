__all__ = ['RequestInterceptDownloaderMiddleware', 'ResponseInterceptDownloaderMiddleware']

import random

from faker import Faker
from scrapy import Request
from scrapy import signals
from scrapy.exceptions import IgnoreRequest
from scrapy.http import Response


def create_user_agents():
    target_size = 200
    bucket_size = target_size // 3
    fake = Faker(0)
    chrome_agents = [fake.chrome(version_from=66, version_to=120, build_from=800, build_to=899) for i in
                     range(bucket_size)]
    ff_agents = [fake.firefox() for i in range(bucket_size)]
    safari_agents = [fake.safari() for i in range(bucket_size)]
    agents = list()
    for agent in set(chrome_agents + ff_agents + safari_agents):
        ua = agent
        if 'Mobile' in ua:
            continue
        if 'Android' in ua:
            continue
        agents.append(ua)
    assert (len(agents) >= 40)
    random.shuffle(agents)
    random.shuffle(agents)
    return agents[:target_size]


FAKE_USER_AGENTS_POOL = create_user_agents()

DEFAULT_REQUEST_META = {
    'proxy': 'https://'
}


class CrawlerappSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class CrawlerappDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request: Request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request: Request, response: Response, spider):
        # Called with the response returned from the downloader.
        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request: Request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class RequestInterceptDownloaderMiddleware(CrawlerappDownloaderMiddleware):

    def process_request(self, request: Request, spider):
        assert (request.method == 'GET')
        #### Uncomment the following
        request.headers[b'User-Agent'] = random.choice(FAKE_USER_AGENTS_POOL).encode('utf-8')
        # request.meta.update(DEFAULT_REQUEST_META) # no need to use encoding
        # proxy_auth = basic_auth_header('usernamee', 'passwordssss')
        # request.headers[b'Proxy-Authorization'] = proxy_auth

        if request.meta.get('_HEADER_REQUESTED_', False):
            pass
        else:
            request.method = 'HEAD'
            request.meta['_HEADER_REQUESTED_'] = True
        return None


class ResponseInterceptDownloaderMiddleware(CrawlerappDownloaderMiddleware):
    def process_response(self, request: Request, response: Response, spider):
        assert request.meta['_HEADER_REQUESTED_'] is not None
        if request.method == 'GET':
            return response

        assert (request.method == 'HEAD')
        if response.status >= 400:
            raise IgnoreRequest

        content_type = response.headers.get(b'Content-Type', b'')
        if content_type == b'text/html' or b'text/html' in content_type:
            request.method = 'GET'
            request.dont_filter = True
            return request

        raise IgnoreRequest
