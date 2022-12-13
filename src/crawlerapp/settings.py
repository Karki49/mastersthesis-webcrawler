# Scrapy settings for crawlerapp project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'crawlerapp'

SPIDER_MODULES = ['crawlerapp.spiders']
NEWSPIDER_MODULE = 'crawlerapp.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'crawlerapp (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 3

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 2
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 2
CONCURRENT_REQUESTS_PER_IP = 2

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
## This is the default spider middleware order
# SPIDER_MIDDLEWARES = {
#     'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 50,
#     'scrapy.spidermiddlewares.offsite.OffsiteMiddleware': 500,
#     'crawlerapp.middlewares.CrawlerappSpiderMiddleware': 543,
#     'scrapy.spidermiddlewares.referer.RefererMiddleware': 700,
#     'scrapy.spidermiddlewares.urllength.UrlLengthMiddleware': 800,
#     'scrapy.spidermiddlewares.depth.DepthMiddleware': 900,
# }

SPIDER_MIDDLEWARES = {
   'crawlerapp.middlewares.CrawlerappSpiderMiddleware': 543,
}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
## This is the default downloader middleware order.

# {'scrapy.downloadermiddlewares.httpauth.HttpAuthMiddleware': 300,
#  'scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware': 350,
#  'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': 400,
#  'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': 500,
#  'scrapy.downloadermiddlewares.retry.RetryMiddleware': 550,
#  'scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware': 580,
#  'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 590,
#  'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': 600,
#  'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': 700,
#  'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 750,
#  'scrapy.downloadermiddlewares.stats.DownloaderStats': 850
#  }

DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.httpauth.HttpAuthMiddleware': None,
    'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': None,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': None,

    'crawlerapp.middlewares.RequestInterceptDownloaderMiddleware': 1001,
    'crawlerapp.middlewares.ResponseInterceptDownloaderMiddleware': 1002
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
#ITEM_PIPELINES = {
#    'crawlerapp.pipelines.CrawlerappPipeline': 300,
#}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = False
# The initial download delay
AUTOTHROTTLE_START_DELAY = 1
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 1
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
HTTPCACHE_ENABLED = False
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'
TWISTED_REACTOR = 'twisted.internet.asyncioreactor.AsyncioSelectorReactor'

DEPTH_LIMIT = 2

# scrapy.spidermiddlewares.urllength.UrlLengthMiddleware
URLLENGTH_LIMIT = 300
