from settings import RPS, START_URL
import asyncio
from crawler import Crawler

if __name__ == '__main__':
    # asyncio.run(Crawler(start_url=START_URL, rps=RPS).main())
    asyncio.run(Crawler(start_url=START_URL, rps=RPS).main())
