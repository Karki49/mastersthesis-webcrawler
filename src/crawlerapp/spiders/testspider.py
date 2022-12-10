import scrapy

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


