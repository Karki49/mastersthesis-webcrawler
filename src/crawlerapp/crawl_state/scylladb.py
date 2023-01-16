from hashlib import sha256
from typing import Any
from typing import Dict

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.cluster import Session
from cassandra.query import BatchStatement

import configs
from crawlerapp import Interval
from crawlerapp import logger
from crawlerapp.crawl_state.interfaces import UrlCrawlState


class ScyllaUrlCrawlState(UrlCrawlState):
    """
        create keyspace spc1 with
            replication = {'class':'SimpleStrategy', 'replication_factor': 1} ;

        create table IF NOT EXISTS spc1.alldomains(
            url_hash varchar PRIMARY KEY,
            status  tinyint,
            url     varchar
        );
    """

    KEYSPACE: str = 'spc1'
    tablename: str = 'alldomains'
    cluster: Cluster = None

    def __init__(self, sanitized_url: str):
        assert sanitized_url
        self.sanitized_url: str = sanitized_url
        self.url_hash = sha256(self.sanitized_url.encode('utf-8')).hexdigest()
        self.session: Session = self._create_db_session()
        self.state: Dict = None
        self.__is_state_in_db:bool = None

    @classmethod
    def initialize_db_connection(cls) -> None:
        cls.cluster = Cluster(configs.CRAWL_STATE_BACKEND_SCYLLADB_HOSTNAME_LIST,
                              port=configs.CRAWL_STATE_BACKEND_SCYLLADB_PORT)  # since this port is mapped to 9042 on docker.

    def _create_db_session(self) -> Any:
        assert self.cluster
        sess = self.cluster.connect(keyspace=self.KEYSPACE, wait_for_all_pools=False)
        return sess

    @classmethod
    def close_db_connection(cls) -> None:
        if cls.cluster:
            cls.cluster.shutdown()

    def retrieve_crawl_state(self):
        query = f"select status from {self.tablename} where url_hash='{self.url_hash}'"
        with Interval() as dt:
            one_row = self.session.execute(query).one()
        logger.info(f'scylladb retrieve miliseconds: {dt.milisecs}')
        if one_row is None:
            self.state = dict(url_hash=self.url_hash, status=None, url=self.sanitized_url)
            self.__is_state_in_db = False
        else:
            self.state = dict(url_hash=self.url_hash, status=one_row.status, url=self.sanitized_url)
            self.__is_state_in_db = True

    def _is_state_in_db(self) -> bool:
        assert self.state
        return self.__is_state_in_db is True

    def is_url_seen(self) -> bool:
        if not self._is_state_in_db():
            return False
        if self.state['status'] is None:
            return False
        if self.state['status'] >= self.SEEN_FLAG:
            return True
        return False

    def is_url_page_downloaded(self) -> bool:
        if not self._is_state_in_db():
            return False
        if self.state['status'] is None:
            return False
        if self.state['status'] >= self.PAGE_DOWNLOADED_FLAG:
            return True
        return False

    def flag_seen(self) -> None:
        batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)
        if not self._is_state_in_db():
            first_statement = f"insert into {self.tablename}(url_hash, url) " \
                          f"VALUES ('{self.url_hash}', '{self.sanitized_url}');"
            second_statement = f"update {self.tablename} using ttl {self.SEEN_TIME_THRESHOLD} " \
                           f"set status={self.SEEN_FLAG} where url_hash='{self.url_hash}';"
            batch.add(first_statement)
            batch.add(second_statement)
            with Interval() as dt:
                self.session.execute(batch)
            logger.info(f'scylladb insert miliseconds: {dt.milisecs}')
        else:
            query = f"update {self.tablename} USING TTL {self.SEEN_TIME_THRESHOLD} " \
                    f"set status={self.SEEN_FLAG} where url_hash='{self.url_hash}';"
            with Interval() as dt:
                self.session.execute(query)
            logger.info(f'scylladb update miliseconds: {dt.milisecs}')
        self.state['status'] = self.SEEN_FLAG
        self.__is_state_in_db = True

    def flag_url_page_downloaded(self) -> None:
        if not self._is_state_in_db():
            query = f"insert into {self.tablename}(url_hash, status, url) " \
                    f"VALUES ('{self.url_hash}', {self.PAGE_DOWNLOADED_FLAG}, '{self.sanitized_url}'); "
            with Interval() as dt:
                self.session.execute(query)
            logger.info(f'scylladb insert miliseconds: {dt.milisecs}')
        else:
            query = f"update {self.tablename} " \
                    f"set status={self.PAGE_DOWNLOADED_FLAG} where url_hash='{self.url_hash}';"
            with Interval() as dt:
                self.session.execute(query)
            logger.info(f'scylladb update miliseconds: {dt.milisecs}')

        self.state['status'] = self.PAGE_DOWNLOADED_FLAG
        self.__is_state_in_db = True

    def _url_seen_with_time_span(self):
        if not self.__is_state_in_db:
            return True
        assert self.state
        if self.state['status'] is None:
            return True
        else:
            return False

if __name__ == '__main__':
    # tests
    ttl_time = 3
    ScyllaUrlCrawlState.SEEN_TIME_THRESHOLD = ttl_time
    ScyllaUrlCrawlState.initialize_db_connection()

    url = 'https://aayushkarki/path/1'
    scb = ScyllaUrlCrawlState(sanitized_url=url)
    scb.retrieve_crawl_state()
    assert scb.is_url_seen() is False
    assert scb.is_url_page_downloaded() is False

    scb.flag_seen()
    assert scb.is_url_seen() is True
    print(scb.state)
    assert scb.is_url_page_downloaded() is False

    scb = ScyllaUrlCrawlState(sanitized_url=url)
    scb.retrieve_crawl_state()
    assert scb.is_url_seen() is True
    assert scb.is_url_page_downloaded() is False

    import time
    scb = ScyllaUrlCrawlState(sanitized_url=url)
    scb.retrieve_crawl_state()
    assert scb.is_url_seen() is True
    assert scb.state['status'] is not None
    time.sleep(ttl_time+3)
    scb.retrieve_crawl_state()
    assert scb.is_url_seen() is False
    assert scb.state['status'] is None

    assert scb._url_seen_with_time_span() is True
    assert scb.is_url_page_downloaded() is False

    scb.flag_url_page_downloaded()
    assert scb.is_url_seen() is True
    assert scb.is_url_page_downloaded() is True

    scb = ScyllaUrlCrawlState(sanitized_url=url)
    scb.retrieve_crawl_state()
    assert scb.is_url_page_downloaded() is True
    assert scb.is_url_seen() is True
