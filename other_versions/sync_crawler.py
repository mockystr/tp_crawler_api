from bs4 import BeautifulSoup
from settings import RPS, START_URL
from urllib.parse import urljoin, urlparse, urldefrag
from pprint import pprint
from elasticsearch import Elasticsearch
from time import time, sleep
import requests


class Crawler:
    def __init__(self, start_url, rps=10, max_count=1000):
        self.start_url = start_url
        self.domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(self.start_url))
        self.index_name = ''.join([i for i in self.start_url
                                   if i not in ('[', '"', '*', '\\\\', '\\', '<', '|', ',', '>', '/', '?', ':')])
        self.rps = rps
        self.max_count = max_count
        self.sleep_time = 1 / self.rps
        self.links = [self.start_url]

    def main(self):
        self.initialize_index()
        self.worker()

    def initialize_index(self):
        es = Elasticsearch()
        created = False
        settings = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mapping": {
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
            if es.indices.exists(self.index_name):
                es.indices.delete(self.index_name)

            es.indices.create(index=self.index_name, ignore=400, body=settings)
            created = True
        except Exception as ex:
            print(str(ex))
        finally:
            return created

    def worker(self):
        es = Elasticsearch()

        for i, link in enumerate(self.links):
            # print(link)
            resp = requests.get(link)
            new_links, soup = self.get_links(resp.text)

            for n in new_links:
                if n not in self.links:
                    self.links.append(n)

            ret = es.create(index=self.index_name,
                            doc_type='crawler',
                            id=i * int(time() * 1_000_000),
                            body={'text': self.clean_text(soup), 'url': link},
                            timeout=None)
            assert ret['result'] == 'created'

            if i > self.max_count:
                break

            sleep(self.sleep_time)

        pprint(self.links)

    @staticmethod
    def clean_text(soup):
        [script.extract() for script in soup(["script", "style"])]
        text = soup.get_text()
        lines = [line.strip() for line in text.splitlines()]
        chunks = [phrase.strip() for line in lines for phrase in line.split("  ")]
        text = '\n'.join(chunk for chunk in chunks if chunk)
        return text

    def get_links(self, html):
        soup = BeautifulSoup(html, 'lxml')
        absolute_links = list(map(lambda x: x if x.startswith(('http://', 'https://')) else urljoin(self.start_url, x),
                                  [i.get('href', '') for i in soup.find_all('a')]))
        links = [urldefrag(x)[0] for x in absolute_links if x.startswith(self.domain)]
        return links, soup


if __name__ == '__main__':
    # start_url = START_URL
    # domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(self.start_url))
    # index_name = ''.join([i for i in self.start_url
    #                            if i not in ('[', '"', '*', '\\\\', '\\', '<', '|', ',', '>', '/', '?', ':')])
    # rps = RPS
    # max_count = 1000
    # sleep_time = 1 / rps
    # links = [start_url]

    t0 = time()

    # main()
    Crawler(start_url=START_URL, rps=RPS).main()
    # 69.67483377456665
    print(time() - t0)
