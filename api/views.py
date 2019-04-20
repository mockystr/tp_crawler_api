from aiohttp import web
import json
import aiopg
from utils import dsn
from pprint import pprint
from aioelasticsearch import Elasticsearch


async def index(request):
    r = {'status': 'success', 'text': 'Hello, im index handler'}
    return web.Response(text=json.dumps(r))


async def search(request):
    es = Elasticsearch()

    q = request._rel_url.query.get('q')
    limit = request._rel_url.query.get('limit', -1)
    offset = request._rel_url.query.get('offset', 0)

    print(q)

    body = {'from': offset, 'size': limit}
    if q:
        body['query'] = {'match': {'text': q}}

    print('body', body)
    res = await es.search(index='qweqweqweqwewe', doc_type='tptest',
                          body=body)
    return web.Response(text=json.dumps(res))
