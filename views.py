from aiohttp import web
import json
import aiopg
from utils import dsn
from pprint import pprint


async def index(request):
    r = {'status': 'success', 'text': 'Hello, im index handler'}
    return web.Response(text=json.dumps(r))


async def search_people(request):
    async with aiopg.create_pool(dsn) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                name = request._rel_url.query.get('q')
                limit = request._rel_url.query.get('limit')
                offset = request._rel_url.query.get('offset')

                if name:
                    select_query = 'SELECT * FROM "testing" WHERE "name"=%s LIMIT %s OFFSET %s'
                    await cur.execute(select_query, (name, limit or "null", offset or "null"))
                else:
                    select_query = 'SELECT * FROM "testing" LIMIT %s OFFSET %s'
                    await cur.execute(select_query, (limit, offset))

                r = await cur.fetchall()
                return web.Response(text=json.dumps(r))
