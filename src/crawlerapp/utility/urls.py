from urllib.parse import urldefrag
from urllib.parse import urlparse
from urllib.parse import urlunparse


def remove_fragments(url: str) -> str:
    assert url
    return urldefrag(url).url


def sanitize_url(url: str) -> str:
    assert url
    url = urldefrag(url).url
    parsed = urlparse(url, allow_fragments=False)
    user_info_hostname_port = parsed.netloc
    hostname_port = user_info_hostname_port.split('@')[-1]
    replaced = parsed._replace(netloc=hostname_port)
    sanitized_url = urlunparse(replaced).rstrip('/')
    assert sanitized_url
    return sanitized_url


if __name__ == '__main__':
    input_url = 'https://user:name@what.ever.com:80/asdasd/bgdf?q=123&cvasd=564#frag'
    output_url = 'https://what.ever.com:80/asdasd/bgdf?q=123&cvasd=564'
    assert (output_url == sanitize_url(input_url))
