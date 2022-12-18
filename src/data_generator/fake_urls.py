import os.path
import random
from os.path import dirname
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

from faker import Faker

DEST_DIR: str = os.path.join(dirname(dirname(dirname(__file__))),
                             'data')


def create_urls(fake: Faker, hostname: str, slugs: list = [], batch_size: int = 40) -> set:
    urls = []
    dedup = set()
    slugs = [s.strip('/') for s in slugs]
    for i in range(batch_size):
        page = random.choice(slugs) if slugs else ''
        url = fake.uri()
        parsed = urlsplit(url)
        if page:
            urlpath = f'{page}/{fake.iana_id()}{parsed.path}'
            parsed = parsed._replace(netloc=hostname, path=urlpath)
        else:
            parsed = parsed._replace(netloc=hostname)

        final_url = urlunsplit(parsed)
        partial_url = final_url.split('://')[-1]
        if partial_url not in dedup:
            urls.append(final_url)
            dedup.add(partial_url)
    return urls


def write_to_file(urls: list, filename: str, file_mode='a'):
    filepath = os.path.join(DEST_DIR, filename)
    lines = [f'{url}\n' for url in urls]
    del urls
    with open(filepath, file_mode) as f:
        f.writelines(lines)
    return


def generate_urls_and_write_to_file(hostname: str, batch_size: int):
    Faker.seed(5)
    fake = Faker()
    urls = create_urls(fake,
                       hostname=hostname,
                       slugs=['news', 'sports', 'health', 'politics'],
                       batch_size=batch_size)

    words = hostname.split('.')
    words.reverse()
    reversed_hostname = '.'.join(words)

    write_to_file(urls,
                  filename=f'{reversed_hostname}.urls',
                  file_mode='w')


if __name__ == '__main__':
    pass
    # generate_urls_and_write_to_file(hostname='www.nytimes.com', batch_size=50000)
    # generate_urls_and_write_to_file(hostname='nytimes.com', batch_size=50000)
    # generate_urls_and_write_to_file(hostname='global.nytimes.com', batch_size=50000)
    # generate_urls_and_write_to_file(hostname='us.nytimes.com', batch_size=50000)

    # generate_urls_and_write_to_file(hostname='www.cnn.com', batch_size=50000)
    # generate_urls_and_write_to_file(hostname='cnn.com', batch_size=50000)
    # generate_urls_and_write_to_file(hostname='edition.cnn.com', batch_size=50000)
    # generate_urls_and_write_to_file(hostname='business.cnn.com', batch_size=50000)

    # generate_urls_and_write_to_file(hostname='www.law360.com', batch_size=50000)
    # generate_urls_and_write_to_file(hostname='law360.com', batch_size=50000)
    # generate_urls_and_write_to_file(hostname='jobs.law360.com', batch_size=50000)
    # generate_urls_and_write_to_file(hostname='us.law360.com', batch_size=50000)
