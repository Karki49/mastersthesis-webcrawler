import os
from datetime import datetime
from os.path import join

from crawlerapp.crawl_state.interfaces import UrlCrawlState
from crawlerapp.crawl_state.mongodb import MongoUrlCrawlState
from crawlerapp.crawl_state.redis import RedisUrlCrawlState
from data_generator.fake_urls import DEST_DIR

source_url_filenames = ['com.cnn.business.urls', 'com.cnn.edition.urls', 'com.cnn.urls', 'com.cnn.www.urls',
                        'com.law360.jobs.urls', 'com.law360.urls', 'com.law360.us.urls', 'com.law360.www.urls',
                        'com.nytimes.global.urls', 'com.nytimes.urls', 'com.nytimes.us.urls', 'com.nytimes.www.urls']

source_urls_full_paths = []

def write_data_to_db(DBClass:UrlCrawlState, full_path:str):
    cnt = 0
    with open(full_path, 'r+') as f:
        t1 = datetime.now()
        for line in f.readlines():
            url = line.strip()
            st = DBClass(sanitized_url=url)
            st.retrieve_crawl_state()
            st.flag_seen()
            cnt += 1
            if cnt % 5000 == 0:
                t2 = datetime.now()
                print(f'time:{(t2-t1).total_seconds()}s, count:{cnt}')

    print(f'time:{(t2-t1).total_seconds()}s, count:{cnt}, fullpath:{full_path}')
    return

def write_data_to_redis():
    for path in source_urls_full_paths:
        write_data_to_db(RedisUrlCrawlState, path)

def write_data_to_mongodb():
    for path in source_urls_full_paths:
        write_data_to_db(MongoUrlCrawlState, path)

if __name__ == '__main__':
    source_urls_full_paths = [join(DEST_DIR, filename) for filename in source_url_filenames]
    print(os.listdir(DEST_DIR))
    exit(0)
    # write_data_to_db(DBClass=RedisUrlCrawlState)
