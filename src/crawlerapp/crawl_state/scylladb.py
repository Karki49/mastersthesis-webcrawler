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

# This does not initiate connection to cluster; just defines the connection.
SCYLLA_CLUSTER = Cluster(configs.CRAWL_STATE_BACKEND_SCYLLADB_HOSTNAME_LIST,
                         port=configs.CRAWL_STATE_BACKEND_SCYLLADB_PORT)  # since this port is mapped to 9042 on docker.


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

    KEYSPACE = 'spc1'
    tablename = f'{KEYSPACE}.alldomains'
    scylla_session: Session = None

    def __init__(self, sanitized_url: str):
        assert sanitized_url
        self.sanitized_url: str = sanitized_url
        self.url_hash = sha256(self.sanitized_url.encode('utf-8')).hexdigest()
        self.session: Session = self._create_db_session()
        self.partial_state: Dict = None

    @classmethod
    def initialize_db_connection(cls) -> None:
        cls.scylla_session = SCYLLA_CLUSTER.connect(keyspace=cls.KEYSPACE, wait_for_all_pools=True)

    def _create_db_session(self) -> Any:
        assert self.scylla_session
        return self.scylla_session

    @classmethod
    def close_db_connection(cls) -> None:
        if cls.scylla_session:
            cls.scylla_session.shutdown()

    def retrieve_crawl_state(self):
        query = f"select url_hash, status from {self.tablename} where url_hash='{self.url_hash}'"
        with Interval() as dt:
            result = self.session.execute(query)
        logger.info(f'scylladb query miliseconds: {dt.milisecs}')
        hash_v_status = dict(result)
        assert len(hash_v_status) <= 1
        if len(hash_v_status) == 0:
            self.partial_state = dict()
        else:
            url_hash, status = hash_v_status.popitem()
            self.partial_state = dict(url_hash=url_hash, status=status)

    def is_url_seen(self) -> bool:
        if len(self.partial_state) == 0:  # if dict empty
            return False
        status = self.partial_state['status']
        if status is None:
            return False
        if status >= self.SEEN_FLAG:
            return True

    def is_url_page_downloaded(self) -> bool:
        if len(self.partial_state) == 0:  # if dict empty
            return False
        status = self.partial_state['status']
        if status is None:
            return False
        if status >= self.PAGE_DOWNLOADED_FLAG:
            return True

    def flag_seen(self) -> None:
        batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)
        if not self.partial_state:
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
            logger.info(f'scylladb single update miliseconds: {dt.milisecs}')
        self.partial_state['status'] = self.SEEN_FLAG

    def flag_url_page_downloaded(self) -> None:
        if not self.partial_state:
            query = f"insert into {self.tablename}(url_hash, status, url) " \
                    f"VALUES ('{self.url_hash}', {self.PAGE_DOWNLOADED_FLAG}, '{self.sanitized_url}'); "
        else:
            query = f"update {self.tablename} " \
                    f"set status={self.PAGE_DOWNLOADED_FLAG} where url_hash='{self.url_hash}';"
        self.session.execute(query)
        self.partial_state['status'] = self.PAGE_DOWNLOADED_FLAG

    def _url_seen_with_time_span(self):
        if len(self.partial_state) == 0:  # if dict empty
            return True
        if self.partial_state.get('status', None) is None:
            return True
        return False
