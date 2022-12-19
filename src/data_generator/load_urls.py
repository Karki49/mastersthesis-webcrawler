import os
import time
from datetime import datetime
from hashlib import sha256
from os.path import join

from crawlerapp.crawl_state.interfaces import UrlCrawlState
from crawlerapp.crawl_state.mongodb import MongoUrlCrawlState
from crawlerapp.crawl_state.redis import RedisUrlCrawlState
from crawlerapp.crawl_state.scylladb import ScyllaUrlCrawlState
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
            st.flag_url_page_downloaded()
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

def write_data_to_scylladb():
    for path in source_urls_full_paths:
        write_data_to_db(ScyllaUrlCrawlState, path)

def scylladb_test(fpath):
    from cassandra.cluster import Cluster
    cluster = Cluster(['0.0.0.0'], port=55045) # since this port is mapped to 9042 on docker.
    session = cluster.connect(keyspace='spc1', wait_for_all_pools=True)
    print(session.is_shutdown)
    session.shutdown()
    time.sleep(10)
    print(session.is_shutdown)
    cnt = 0
    t1 = datetime.now()
    with open(fpath, 'r') as f:
        for line in f.readlines():
            cnt += 1
            if cnt % 103 > 0:
                continue
            url = line.rstrip()
            url_hash = sha256(url.encode('utf-8')).hexdigest()

            # statement = f"insert into spc1.alldomains(url_hash, first_seen, last_updated, status, url) VALUES ('{url_hash}', toTimestamp(now()), toTimestamp(now()), 1, '{url}');"
            statement = f"select url_hash, status, url from spc1.alldomains where url_hash='{url_hash}'"

            rows = session.execute(statement)

            t1 = datetime.now()
            # rows = session.execute(statement)
            if cnt % 103 == 0:
                t2 = datetime.now()
                print(dict(rows))
                print(f'cnt:{cnt}, time:{(t2 - t1).total_seconds()}s, count:{cnt}')

    print(f'time:{(t2 - t1).total_seconds()}s, count:{cnt}')
    return

if __name__ == '__main__':
    # source_urls_full_paths = [join(DEST_DIR, filename) for filename in source_url_filenames]
    # print(os.listdir(DEST_DIR))
    # exit(0)
    # write_data_to_db(DBClass=RedisUrlCrawlState)

    path = join(DEST_DIR, 'com.nytimes.us.urls')
    write_data_to_db(ScyllaUrlCrawlState, path)

