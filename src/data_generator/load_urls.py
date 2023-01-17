import json
import time
from datetime import datetime
from hashlib import sha256
from os.path import join

import redis

import configs
from crawlerapp.crawl_state.interfaces import UrlCrawlState
from crawlerapp.crawl_state.mongodb import MongoUrlCrawlState
from crawlerapp.crawl_state.redisdb import RedisUrlCrawlState
from crawlerapp.crawl_state.scylladb import ScyllaUrlCrawlState
from data_generator.fake_urls import DEST_DIR

source_url_filenames = ['com.cnn.business.urls', 'com.cnn.edition.urls', 'com.cnn.urls', 'com.cnn.www.urls',
                        'com.law360.jobs.urls', 'com.law360.urls', 'com.law360.us.urls', 'com.law360.www.urls',
                        'com.nytimes.global.urls', 'com.nytimes.urls', 'com.nytimes.us.urls', 'com.nytimes.www.urls',
                        'com.rottentomatoes.career.urls', 'com.rottentomatoes.editorial.urls',
                        'com.rottentomatoes.urls', 'com.rottentomatoes.www.urls']

source_urls_full_paths = [join(DEST_DIR, filename) for filename in source_url_filenames]


def write_data_to_db(DBClass:UrlCrawlState, full_path:str, chunk_size:int=5000):
    cnt = 0
    with open(full_path, 'r+') as f:
        data = list()
        for line in f.readlines():
            url = line.strip()
            url_hash = sha256(url.encode('utf-8')).hexdigest()
            value = (url_hash, 2, url)
            data.append(value)
            cnt += 1
            if cnt % chunk_size == 0:
                yield data
                data = list()
    if data:
        yield data
    return


def write_data_to_redis():
    uri = f'{configs.CRAWL_STATE_BACKEND_REDIS_URI[:-1]}2'
    print(uri)
    db = redis.Redis.from_url(url=uri)
    for path in source_urls_full_paths:
        pipe = db.pipeline()
        for triple_list in write_data_to_db(RedisUrlCrawlState, path):
            for url_hash, status, url in triple_list:
                key = url_hash
                value = json.dumps(dict(status=status, url=url))
                pipe.set(name=key, value=value)
        pipe.execute()

def write_data_to_mongodb():
    uri = configs.CRAWL_STATE_BACKEND_MONGODB_URI
    print(uri)
    from pymongo import MongoClient, InsertOne
    cl = MongoClient(uri)
    db = cl.thesisdb
    for col_name in ('cnn', 'law360', 'nytimes', 'rottentomatoes'):
        db[col_name].drop()
        db[col_name].create_index('url_hash', unique=True)
        db[col_name].create_index('ttl_field', expireAfterSeconds=3600)

    for path in source_urls_full_paths:
        print(path)
        if 'cnn' in path:
            col_name = 'cnn'
        if 'law360' in path:
            col_name = 'law360'
        if 'nytimes' in path:
            col_name = 'nytimes'
        if 'rottentomatoes' in path:
            col_name = 'rottentomatoes'
        print(f'inserting to column: {col_name}')
        col = db[col_name]
        for triple_list in write_data_to_db(None, path, chunk_size=5000):
            documents = [dict(url_hash=url_hash, status=status, url=url) for url_hash, status, url in triple_list]
            col.insert_many(documents)

    cl.close()

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

    # path = join(DEST_DIR, 'com.nytimes.us.urls')
    # for d in write_data_to_db(ScyllaUrlCrawlState, path):
    #     print(d)

    # write_data_to_redis()
    write_data_to_mongodb()