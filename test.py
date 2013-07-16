import logging
import tornado

import tornado.ioloop as io
from src import freebase

ioloop = io.IOLoop.instance()

__author__ = 'vukasin'
logging.basicConfig(level=logging.DEBUG)

@tornado.gen.engine
def d(callback):
    api_key = "AIzaSyAyg3M0vKzhc08TP8ehjlgzIUZhfpUbLtU"

    logging.debug("create freebase object")
    fb = freebase.Freebase(api_key, cache_path="freebase_cache")
    data = yield tornado.gen.Task(fb.mql, query={'id': None, 'type': [], 'name': 'SAP AG'})
    sap_ag = freebase.Object(fb, data)
    res = yield tornado.gen.Task(sap_ag.load_all)
    for t in sap_ag.get_rdf():
        print(t)
    callback(None)

ioloop.add_callback(d, print)
ioloop.start()
