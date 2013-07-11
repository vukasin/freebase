import logging

__author__ = 'vukasin'
import freebase
logging.basicConfig(level=logging.DEBUG)

api_key = "AIzaSyAyg3M0vKzhc08TP8ehjlgzIUZhfpUbLtU"

fb = freebase.Freebase(api_key, cache_path="freebase_cache")

sap_ag = freebase.Object(fb, fb.mql({'id': None, 'type': [], 'name': 'SAP AG'}))
sap_ag.load_all()

for t in sap_ag.get_rdf():
    print(t)