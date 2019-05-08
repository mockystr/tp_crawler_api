import asyncio
import aiohttp
from bs4 import BeautifulSoup
from settings import RPS, START_URL
from urllib.parse import urljoin, urlparse, urldefrag
from pprint import pprint
from aioelasticsearch import Elasticsearch
from time import time
import logging
import asyncpool


class Crawler:
    def __init__(self, start_url, loop, rps=10, max_count=1000):
        self.start_url = start_url
        self.domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(self.start_url))
        self.index_name = ''.join([i for i in self.start_url
                                   if i not in ('[', '"', '*', '\\\\', '\\', '<', '|', ',', '>', '/', '?', ':')])
        self.loop = loop
        self.rps = rps
        self.max_count = max_count
        self.sleep_time = 1 / self.rps
        self.set_links = set()
        self.links = asyncio.Queue()
        self.tmp_id = 0

    async def initialize_index(self, es):
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
            if await es.indices.exists(self.index_name):
                await es.indices.delete(self.index_name)

            await es.indices.create(index=self.index_name, ignore=400, body=settings)
            created = True
        except Exception as ex:
            print(str(ex))
        finally:
            return created

    async def main(self):
        async with Elasticsearch([{'host': 'localhost', 'port': 9200}]) as es:
            await self.initialize_index(es)
            await self.links.put(self.start_url)

            async with aiohttp.ClientSession() as session:
                async with asyncpool.AsyncPool(self.loop, num_workers=10,
                                               name="CrawlerPool", logger=logging.getLogger("CrawlerPool"),
                                               worker_co=self.worker) as pool:
                    link = await self.links.get()
                    await pool.push(link, es, session)

                    while True:
                        if not self.links.empty():
                            link = await self.links.get()
                        else:
                            await asyncio.sleep(0.2)
                            if self.links.empty():
                                break

                            link = await self.links.get()

                        await asyncio.sleep(self.sleep_time)
                        await pool.push(link=link, es=es, session=session)

    async def worker(self, link, es, session):
        async with session.get(link) as resp:
            if self.tmp_id > self.max_count:
                return
            # print(self.tmp_id)
            # print(link)

            new_links, soup = await self.get_links(await resp.text())
            self.set_links.add(link)

            self.tmp_id += 1
            for n in new_links:
                if n not in self.set_links:
                    await self.links.put(n)
                    self.set_links.add(n)

            await es.create(index=self.index_name,
                            doc_type='crawler',
                            id=self.tmp_id,
                            body={'text': await self.clean_text(soup), 'url': link})

    @staticmethod
    async def clean_text(soup):
        [script.extract() for script in soup(["script", "style"])]
        await asyncio.sleep(0)
        text = soup.get_text()
        lines = [line.strip() for line in text.splitlines()]
        chunks = [phrase.strip() for line in lines for phrase in line.split("  ")]
        text = '\n'.join(chunk for chunk in chunks if chunk)
        return text

    async def get_links(self, html):
        soup = BeautifulSoup(html, 'lxml')
        await asyncio.sleep(0)
        absolute_links = list(map(lambda x: x if x.startswith(('http://', 'https://')) else urljoin(self.start_url, x),
                                  [i.get('href', '') for i in soup.find_all('a')]))
        links = [urldefrag(x)[0] for x in absolute_links if x.startswith(self.domain)]
        return links, soup


if __name__ == '__main__':
    this_loop = asyncio.get_event_loop()
    t0 = time()
    c = Crawler(start_url=START_URL, loop=this_loop, rps=RPS, max_count=1000)
    this_loop.run_until_complete(c.main())
    print(c.tmp_id)
    print(time() - t0)
