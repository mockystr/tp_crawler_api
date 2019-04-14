from views import index, search_people


def setup_routes(app):
    app.router.add_get('/', index, name='index')
    app.router.add_get('/v1/search', search_people, name='search')
