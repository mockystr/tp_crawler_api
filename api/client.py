import aiohttp
import asyncio
import time
import requests

url = 'http://0.0.0.0:8080/api/v1/search'


async def make_tests():
    async with aiohttp.ClientSession() as session:
        t0 = time.time()
        for _ in range(150):
            async with session.get(url) as resp:
                print(resp.status)
                print(await resp.text())
        print('finish for {}s'.format(time.time() - t0))
        # finish for 1.3171379566192627s


def make_tests_sync():
    t0 = time.time()
    for _ in range(150):
        r = requests.get(url)
        print(r.status_code)
        print(r.text)
    print('finish for {}s'.format(time.time() - t0))
    # finish for 1.8994920253753662s


if __name__ == '__main__':
    asyncio.run(make_tests())
    # make_tests_sync()
