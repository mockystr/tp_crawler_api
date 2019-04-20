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
        self.index_name = ''.join([i for i in self.start_url
                                   if i not in ('[', '"', '*', '\\\\', '\\', '<', '|', ',', '>', '/', '?', ':')])
        self.rps = rps
        self.max_count = max_count
        self.links = [self.start_url]

    async def main(self):
        await self.initialize_index()
        await self.worker()

    async def initialize_index(self):
        es = Elasticsearch()
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
            if await es.indices.exists(self.index_name):
                await es.indices.delete(self.index_name)

            await es.indices.create(index=self.index_name, ignore=400, body=settings)
            print('Created Index')
            created = True
        except Exception as ex:
            print(str(ex))
        finally:
            await es.close()
            return created

    async def worker(self):
        es = Elasticsearch()

        async with aiohttp.ClientSession() as session:
            for i, link in enumerate(self.links):
                print(link)
                async with session.get(link) as resp:
                    new_links, soup = await self.get_links(await resp.text())

                    for n in new_links:
                        if n not in self.links:
                            self.links.append(n)

                    ret = await es.create(index=self.index_name,
                                          doc_type='crawler',
                                          id=i,
                                          body={'text': await self.clean_text(soup), 'url': link},
                                          timeout=None)
                    assert ret['result'] == 'created'

                if i > self.max_count:
                    break

                await asyncio.sleep(1 / self.rps)

            await es.close()
            pprint(self.links)

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
    asyncio.run(Crawler(start_url=START_URL, rps=RPS).main())
