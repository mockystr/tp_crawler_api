import asyncio
import aiohttp
import aioredis
from bs4 import BeautifulSoup
from settings import RPS, START_URL
from urllib.parse import urljoin, urlparse, urldefrag
from pprint import pprint


class Crawler:
    def __init__(self, start_url, rps=10, max_count=1000):
        self.start_url = start_url
        self.domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(self.start_url))
        print('domain ', self.domain)
        self.rps = rps
        self.max_count = max_count
        self.links = [self.start_url]

    async def main(self):
        # self.redis_conn = await aioredis.create_connection(('localhost', 6379))

        async with aiohttp.ClientSession() as session:
            for link in self.links:
                print(link)
                async with session.get(link) as resp:
                    new_links = await self.get_links(await resp.text())
                    # print(new_links)
                    for n in new_links:
                        if n not in self.links:
                            self.links.append(n)
                    await asyncio.sleep(1 / self.rps)
            pprint(self.links)

    async def get_links(self, html):
        soup = BeautifulSoup(html, 'lxml')
        await asyncio.sleep(0)
        absolute_links = list(map(lambda x: x if x.startswith(('http://', 'https://')) else urljoin(self.start_url, x),
                                  [i.get('href', '') for i in soup.find_all('a')]))
        links = [urldefrag(x)[0] for x in absolute_links if x.startswith(self.domain)]
        return links


if __name__ == '__main__':
    # with open('shit/index.html') as file:
    #     asyncio.run(Crawler(start_url=START_URL, rps=RPS).get_links(file))

    asyncio.run(Crawler(start_url=START_URL, rps=RPS).main())
