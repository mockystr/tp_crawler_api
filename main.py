from settings import RPS, START_URL
import asyncio
from crawler import Crawler

if __name__ == '__main__':
    # with pool ~14s
    # best score was 11.5

    # without pool ~50s
    # sync 78s
    asyncio.run(Crawler(start_url=START_URL, rps=RPS).main())
