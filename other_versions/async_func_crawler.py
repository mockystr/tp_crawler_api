import asyncio
import aiohttp
from bs4 import BeautifulSoup
from settings import RPS, START_URL
from urllib.parse import urljoin, urlparse, urldefrag
from pprint import pprint
from aioelasticsearch import Elasticsearch
from time import time


async def main():
    es_obj = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    await initialize_index(es_obj)
    await worker(es_obj)


async def initialize_index(es):
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
        if await es.indices.exists(index_name):
            await es.indices.delete(index_name)

        await es.indices.create(index=index_name, ignore=400, body=settings)
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        await es.close()
        return created


async def worker(es):
    async with aiohttp.ClientSession() as session:
        for i, link in enumerate(links):
            print(link)
            async with session.get(link) as resp:
                new_links, soup = await get_links(await resp.text())

                for n in new_links:
                    if n not in links:
                        links.append(n)

                ret = await es.create(index=index_name,
                                      doc_type='crawler',
                                      id=i * int(time() * 1_000_000),
                                      body={'text': await clean_text(soup), 'url': link},
                                      timeout=None)
                assert ret['result'] == 'created'

            if i > max_count:
                break

            await asyncio.sleep(sleep_time)

        await es.close()
        pprint(links)


async def clean_text(soup):
    [script.extract() for script in soup(["script", "style"])]
    await asyncio.sleep(0)
    text = soup.get_text()
    lines = [line.strip() for line in text.splitlines()]
    chunks = [phrase.strip() for line in lines for phrase in line.split("  ")]
    await asyncio.sleep(0)
    text = '\n'.join(chunk for chunk in chunks if chunk)
    return text


async def get_links(html):
    soup = BeautifulSoup(html, 'lxml')
    await asyncio.sleep(0)
    absolute_links = list(map(lambda x: x if x.startswith(('http://', 'https://')) else urljoin(start_url, x),
                              [i.get('href', '') for i in soup.find_all('a')]))
    links = [urldefrag(x)[0] for x in absolute_links if x.startswith(domain)]
    return links, soup


if __name__ == '__main__':
    start_url = START_URL
    domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(start_url))
    index_name = ''.join([i for i in start_url
                          if i not in ('[', '"', '*', '\\\\', '\\', '<', '|', ',', '>', '/', '?', ':')])
    rps = RPS
    max_count = 1000
    sleep_time = 1 / rps
    links = [start_url]

    t0 = time()
    asyncio.run(main())
    print(time() - t0)
    # es.close()
