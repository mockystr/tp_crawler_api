import asyncio
import aiohttp
import aioredis
from bs4 import BeautifulSoup
from settings import RPS, START_URL
from urllib.parse import urljoin, urlparse, urldefrag
from pprint import pprint
from aioelasticsearch import Elasticsearch


class Crawler:
    def __init__(self, start_url, rps=10, max_count=1000):
        self.start_url = start_url
        self.domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(self.start_url))
        self.rps = rps
        self.max_count = max_count
        self.links = [self.start_url]
        self.es = Elasticsearch()

    async def worker(self):
        async with aiohttp.ClientSession() as session:
            for i, link in enumerate(self.links):
                print(link)
                async with session.get(link) as resp:
                    new_links, soup = await self.get_links(await resp.text())

                    for n in new_links:
                        if n not in self.links:
                            self.links.append(n)

                    print(await self.es.ping())
                    ret = await self.es.create(index='emirloh',
                                               doc_type='tptest',
                                               id=i,
                                               body={'text': 'asdasdas', 'url': link},
                                               timeout=None)
                    assert ret['result'] == 'created'

                await asyncio.sleep(1 / self.rps)
            pprint(self.links)

    @staticmethod
    def clean_text(soup):
        [script.extract() for script in soup(["script", "style"])]
        # await asyncio.sleep(0)
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
    # with open('shit/index.html') as file:
    #     asyncio.run(Crawler(start_url=START_URL, rps=RPS).get_links(file))

    asyncio.run(Crawler(start_url=START_URL, rps=RPS).worker())


"""
/Users/emirnavruzov/.local/share/virtualenvs/tp_asyncio-qsY2M8-g/bin/python3.7 /Users/emirnavruzov/Documents/technopark/tp_asyncio/shit/es.py
Traceback (most recent call last):
  File "/Users/emirnavruzov/Documents/technopark/tp_asyncio/shit/es.py", line 22, in <module>
    asyncio.run(Tmp().go())
  File "/anaconda3/lib/python3.7/asyncio/runners.py", line 43, in run
    return loop.run_until_complete(main)
  File "/anaconda3/lib/python3.7/asyncio/base_events.py", line 584, in run_until_complete
    return future.result()
  File "/Users/emirnavruzov/Documents/technopark/tp_asyncio/shit/es.py", line 16, in go
    print(await self.es.search())
  File "/Users/emirnavruzov/.local/share/virtualenvs/tp_asyncio-qsY2M8-g/lib/python3.7/site-packages/aioelasticsearch/transport.py", line 338, in perform_request
    ignore=ignore, timeout=timeout, headers=headers,
  File "/Users/emirnavruzov/.local/share/virtualenvs/tp_asyncio-qsY2M8-g/lib/python3.7/site-packages/aioelasticsearch/transport.py", line 258, in _perform_request
    ignore=ignore, timeout=timeout, headers=headers,
  File "/Users/emirnavruzov/.local/share/virtualenvs/tp_asyncio-qsY2M8-g/lib/python3.7/site-packages/aioelasticsearch/connection.py", line 103, in perform_request
    timeout=timeout or self.timeout,
  File "/Users/emirnavruzov/.local/share/virtualenvs/tp_asyncio-qsY2M8-g/lib/python3.7/site-packages/aiohttp/client.py", line 417, in _request
    with timer:
  File "/Users/emirnavruzov/.local/share/virtualenvs/tp_asyncio-qsY2M8-g/lib/python3.7/site-packages/aiohttp/helpers.py", line 568, in __enter__
    raise RuntimeError('Timeout context manager should be used '
RuntimeError: Timeout context manager should be used inside a task
Unclosed client session
client_session: <aiohttp.client.ClientSession object at 0x10bc058d0>
"""