from hashlib import sha256
from typing import Any
from typing import Dict

from cassandra.cluster import Cluster
from cassandra.cluster import Session

from crawlerapp.crawl_state.interfaces import UrlCrawlState

cluster = Cluster(['0.0.0.0'], port=55045)  # since this port is mapped to 9042 on docker.
KEYSPACE = 'spc1'
scylla_session: Session = cluster.connect(keyspace=KEYSPACE, wait_for_all_pools=True)


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

    tablename = f'{KEYSPACE}.alldomains'

    def __init__(self, sanitized_url: str):
        assert sanitized_url
        self.sanitized_url: str = sanitized_url
        self.url_hash = sha256(self.sanitized_url.encode('utf-8')).hexdigest()
        self.session: Session = scylla_session
        self.partial_state: Dict = None

    @classmethod
    def _create_connection(cls) -> Any:
        raise Exception("No need to implement this method.")

    def retrieve_crawl_state(self):
        query = f"select url_hash, status from {self.tablename} where url_hash='{self.url_hash}'"
        result = self.session.execute(query)
        # dict(result) will produce either {} or a filled dictionary.
        self.partial_state = dict(result)

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
        if not self.partial_state:
            first_query = f"insert into {self.tablename}(url_hash, url) " \
                          f"VALUES ('{self.url_hash}', '{self.sanitized_url}');"
            self.session.execute(first_query)
            second_query = f"update {self.tablename} using ttl {self.SEEN_TIME_THRESHOLD} " \
                           f"set status={self.SEEN_FLAG} where url_hash='{self.url_hash}';"
            self.session.execute(second_query)
        else:
            query = f"update {self.tablename} USING TTL {self.SEEN_TIME_THRESHOLD} " \
                    f"set status={self.SEEN_FLAG} where url_hash='{self.url_hash}';"
            self.session.execute(query)
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
