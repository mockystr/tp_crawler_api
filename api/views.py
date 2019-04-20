from aiohttp import web
import json
from utils import dsn
from pprint import pprint
from aioelasticsearch import Elasticsearch
from settings import index_name
from aioelasticsearch.helpers import Scan


async def index(request):
    r = {'status': 'success', 'text': 'Hello, im index handler'}
    return web.Response(text=json.dumps(r))


async def search(request):
    es = Elasticsearch()

    q = request._rel_url.query.get('q')
    limit = int(request._rel_url.query.get('limit', -1))
    offset = int(request._rel_url.query.get('offset', 0))

    body = {}
    if q:
        body['query'] = {'match': {'text': q}}

    async with Scan(es,
                    index=index_name,
                    doc_type='crawler',
                    query=body, ) as scan_res:
        res_formated, count = await format_search(scan_res, limit, offset)
        text = {'total_hits': count, 'count': len(res_formated), 'results': res_formated}
        return web.Response(text=json.dumps(text))


async def format_search(res, limit, offset):
    res_source = [i['_source'] async for i in res]
    count = len(res_source)
    return res_source[offset:min(limit + offset, count)], count
