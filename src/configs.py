import os
from configparser import ConfigParser

SRC_DIRECTORY = os.path.dirname(__file__)
config_ini_path = os.path.join(SRC_DIRECTORY, 'configs.ini')
assert os.path.exists(config_ini_path)

config = ConfigParser()
config.read(config_ini_path)

CRAWL_STATE_BACKEND_REDIS_URI = config['redis']['CRAWL_STATE_DB_URI']
CRAWL_STATE_BACKEND_MONGODB_URI = config['mongodb']['CRAWL_STATE_DB_URI']
CRAWL_STATE_BACKEND_SCYLLADB_HOSTNAME_LIST = config['scylladb']['CRAWL_STATE_DB_HOSTS'].split(',')

if __name__ == '__main__':
    print(CRAWL_STATE_BACKEND_REDIS_URI)
    print(CRAWL_STATE_BACKEND_MONGODB_URI)
    print(CRAWL_STATE_BACKEND_SCYLLADB_HOSTNAME_LIST)
