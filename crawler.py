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


class StopDownloading(Exception):
    pass


class Crawler:
    def __init__(self, start_url, rps=10, max_count=1000):
        self.start_url = start_url
        self.domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(self.start_url))
        self.index_name = ''.join([i for i in self.start_url
                                   if i not in ('[', '"', '*', '\\\\', '\\', '<', '|', ',', '>', '/', '?', ':')])
        self.rps = rps
        self.max_count = max_count
        self.sleep_time = 1 / self.rps
        self.set_links = set()
        self.links = asyncio.Queue()
        self.tmp_id = 0

    async def initialize_index(self):
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
            if await es.indices.exists(self.index_name):
                await es.indices.delete(self.index_name)

            await es.indices.create(index=self.index_name, ignore=400, body=settings)
            created = True
        except Exception as ex:
            print(str(ex))
        finally:
            await es.close()
            return created

    async def main(self):
        await self.initialize_index()
        await self.links.put(self.start_url)

        async with asyncpool.AsyncPool(asyncio.get_event_loop(), num_workers=10, name="CrawlerPool",
                                       logger=logging.getLogger("CrawlerPool"),
                                       worker_co=self.worker, max_task_time=300,
                                       log_every_n=10) as pool:
            link = await self.links.get()
            await pool.push(link)
            # сделал этот слип для того, чтобы дать время воркеру закинуть в очередь ссылки
            # иначе будет эксепшн (скинул внизу)
            # буду рад, если расскажешь, как убрать этот слип, чтоб все работало
            await asyncio.sleep(0.3)

            # даже с слипами начал быстро качать
            # 15.77086591720581

            # без пула было 49 секунд

            while True:
                # try:
                link = await self.links.get()
                # except:
                #     await asyncio.sleep(0.2)
                #     try:
                #         link = await self.links.get()
                #     except:
                #         break

                await pool.push(link)
                await asyncio.sleep(self.sleep_time)
                # await asyncio.sleep(0.5)

        pprint(self.links)

    async def worker(self, link):
        es = Elasticsearch()
        print(link)

        async with aiohttp.ClientSession() as session:
            async with session.get(link) as resp:
                new_links, soup = await self.get_links(await resp.text())
                self.set_links.add(link)

                for n in new_links:
                    if n not in self.set_links:
                        await self.links.put(n)
                        self.set_links.add(n)

                self.tmp_id += 1
                await es.create(index=self.index_name,
                                doc_type='crawler',
                                id=self.tmp_id,
                                body={'text': await self.clean_text(soup), 'url': link},
                                timeout=None)

            await es.close()

    @staticmethod
    async def clean_text(soup):
        [script.extract() for script in soup(["script", "style"])]
        await asyncio.sleep(0)
        text = soup.get_text()
        lines = [line.strip() for line in text.splitlines()]
        chunks = [phrase.strip() for line in lines for phrase in line.split("  ")]
        await asyncio.sleep(0)
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
    t0 = time()
    asyncio.run(Crawler(start_url=START_URL, rps=RPS).main())
    print(time() - t0)

# Traceback (most recent call last):
#   File "/Users/emirnavruzov/Documents/technopark/tp_crawler_api/crawler.py", line 146, in <module>
#     asyncio.run(Crawler(start_url=START_URL, rps=RPS).main())
#   File "/usr/local/Cellar/python/3.7.3/Frameworks/Python.framework/Versions/3.7/lib/python3.7/asyncio/runners.py", line 43, in run
#     return loop.run_until_complete(main)
#   File "/usr/local/Cellar/python/3.7.3/Frameworks/Python.framework/Versions/3.7/lib/python3.7/asyncio/base_events.py", line 584, in run_until_complete
#     return future.result()
#   File "/Users/emirnavruzov/Documents/technopark/tp_crawler_api/crawler.py", line 87, in main
#     link = await self.links.get()
#   File "/usr/local/Cellar/python/3.7.3/Frameworks/Python.framework/Versions/3.7/lib/python3.7/asyncio/queues.py", line 159, in get
#     await getter
# RuntimeError: Task <Task pending coro=<Crawler.main() running at /Users/emirnavruzov/Documents/technopark/tp_crawler_api/crawler.py:87> cb=[_run_until_complete_cb() at /usr/local/Cellar/python/3.7.3/Frameworks/Python.framework/Versions/3.7/lib/python3.7/asyncio/base_events.py:158]> got Future <Future pending> attached to a different loop
