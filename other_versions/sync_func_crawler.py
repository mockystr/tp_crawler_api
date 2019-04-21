from bs4 import BeautifulSoup
from settings import RPS, START_URL
from urllib.parse import urljoin, urlparse, urldefrag
from pprint import pprint
from elasticsearch import Elasticsearch
from time import time, sleep
import requests


def main():
    initialize_index()
    worker()


def initialize_index():
    created = False
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "members": {
                "dynamic": "strict",
                "properties": {
                    "url": {
                        "type": "text"
                    },
                    "text": {
                        "type": "text"
                    }
                }
            }
        }
    }
    try:
        if es.indices.exists(index_name):
            es.indices.delete(index_name)

        es.indices.create(index=index_name, ignore=400, body=settings)
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def worker():
    for i, link in enumerate(links):
        # print(link)
        resp = requests.get(link)
        new_links, soup = get_links(resp.text)

        for n in new_links:
            if n not in links:
                links.append(n)

        ret = es.create(index=index_name,
                        doc_type='crawler',
                        id=i * int(time() * 1_000_000),
                        body={'text': clean_text(soup), 'url': link},
                        timeout=None)
        assert ret['result'] == 'created'

        if i > max_count:
            break

        sleep(sleep_time)

    pprint(links)


def clean_text(soup):
    [script.extract() for script in soup(["script", "style"])]
    text = soup.get_text()
    lines = [line.strip() for line in text.splitlines()]
    chunks = [phrase.strip() for line in lines for phrase in line.split("  ")]
    text = '\n'.join(chunk for chunk in chunks if chunk)
    return text


def get_links(html):
    soup = BeautifulSoup(html, 'lxml')
    absolute_links = list(map(lambda x: x if x.startswith(('http://', 'https://')) else urljoin(start_url, x),
                              [i.get('href', '') for i in soup.find_all('a')]))
    links = [urldefrag(x)[0] for x in absolute_links if x.startswith(domain)]
    return links, soup


if __name__ == '__main__':
    es = Elasticsearch()

    start_url = START_URL
    domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(start_url))
    index_name = ''.join([i for i in start_url
                          if i not in ('[', '"', '*', '\\\\', '\\', '<', '|', ',', '>', '/', '?', ':')])
    rps = RPS
    max_count = 1000
    sleep_time = 1 / rps
    links = [start_url]

    t0 = time()

    main()
    # 64.30136132240295
    print(time() - t0)
