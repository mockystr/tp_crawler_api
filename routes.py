from views import index, search


def setup_routes(app):
    app.router.add_get('/', index, name='index')
    app.router.add_get('/api/v1/search', search, name='search')
