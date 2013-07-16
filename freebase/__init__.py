import shelve
import json
import rdflib
import urllib.request
import urllib.parse
import logging
import tornado
import tornado.httpclient

from tornado import gen

__author__ = 'vukasin'

__loaders__ = {
    '/type/datetime': lambda d: d.get('value'),
    '/type/int': lambda d: d.get('value'),
    '/type/float': lambda d: d.get('value'),
    '/type/boolean': lambda d: d.get('value'),
    '/type/text': lambda d: d.get('value'),
    '/type/rawstring': lambda d: d.get('value'),
    '/type/uri': lambda d: d.get('value'),
    '/type/key': lambda d: d.get('value'),
    '/type/id': lambda d: d.get('value')
}


class Reference(object):
    def __init__(self, connection, id, name=None, type=[]):
        self.freebase = connection
        self.id = id
        self.name = name
        self.type = type

    def get_rdf(self, ns='http://rdf.freebase.com#'):
        id = rdflib.URIRef(ns + self.id)
        for t in self.type:
            yield (id, rdflib.RDF.type, rdflib.URIRef(ns + t))
        if self.name is not None:
            yield (id, rdflib.RDFS.label, rdflib.Literal(self.name))

    @gen.engine
    def load(self, callback, *properties):
        if len(properties) == 0:
            properties = ["*"]
        res = yield Task(self.freebase.load_object, self.id, *properties)
        callback(res)


class Object(object):
    def __init__(self, connection, data={}):
        """
        Load an object from its Freebase representation

        :param data: dict representation of the object
        :type data:dict
        :return:
        """
        self.freebase = connection
        self.freebase_data = data
        self.type = []
        self.name = []
        self.id = None
        self.properties = {}
        self.__load_data(data)
        self.__properties = None

    def __generate_rdf(self, ns, subj, pred, obj):
        if isinstance(obj, list):
            for m in obj:
                yield from self.__generate_rdf(ns, subj, pred, m)
        elif isinstance(obj, Reference):
            if hasattr(obj, 'id'):
                yield (subj, pred, rdflib.URIRef(ns + obj.id))
                yield from obj.get_rdf(ns=ns)
            else:
                yield (subj, pred, rdflib.Literal(obj.freebase_data.get('value', None)))
        else:
            yield (subj, pred, rdflib.Literal(obj))

    @gen.engine
    def property_names(self, callback):
        if not self.__properties:
            props = set()
            for t in self.type:
                type_obj = yield gen.Task(self.freebase.load_type, type_id=t)
                for prop in type_obj.properties['/type/type/properties']:
                    props.add(prop.id)
            self.__properties = props
        callback(self.__properties)

    @gen.engine
    def load(self, callback, properties):
        properties = yield gen.Task(self.freebase.load_properties, id=self.id, properties=properties)
        callback(self.__load_data(properties))

    @gen.engine
    def load_all(self, callback):
        prop_names = yield gen.Task(self.property_names)
        res = yield gen.Task(self.load, properties=prop_names)
        callback(res)

    def __load_data(self, data: dict):
        def load_value(v):
            if isinstance(v, dict):
                t = v.get('type')
                if isinstance(t, str) and t in __loaders__:
                    return __loaders__[t](v)
                else:
                    return Reference(self.freebase, **v)
            elif isinstance(v, list):
                return [load_value(e) for e in v]
            else:
                return v

        for p, v in data.items():
            if p == 'type':
                self.type.extend(v)
            elif p == 'id':
                self.id = v
            elif p == 'name':
                self.name = v
            else:
                self.properties[p] = load_value(v)

    def get_rdf(self, ns='http://rdf.freebase.com'):
        id = rdflib.URIRef(ns + self.id)
        for t in self.type:
            yield (id, rdflib.RDF.type, rdflib.URIRef(ns + t))
        for t in self.name:
            yield (id, rdflib.RDFS.label, rdflib.Literal(t))
        for property_name, value in self.properties.items():
            purl = rdflib.URIRef(ns + property_name)
            yield from self.__generate_rdf(ns, id, purl, value)


class Freebase(object):
    def __init__(self,
                 api_key: str,
                 base_url: str="https://www.googleapis.com/freebase/v1/",
                 cache_path: str=None,
                 **params):
        """

        :type api_key: str
        :type base_url: str
        :param api_key:
        :param cache_path: location of the cache used to store type data
        :param base_url:
        """
        object.__init__(self)
        if cache_path is None:
            self.__cache = None
        else:
            self.__cache = shelve.open(cache_path)
        self.api_key = api_key
        self.base_url = base_url
        self.params = params
        self.http_client = tornado.httpclient.AsyncHTTPClient()

    def __fb2py(self, fb, obj):
        """

        :param fb:
        :param obj:
        :return:
        """
        if isinstance(obj, dict):
            return Object(fb, obj)
        elif isinstance(obj, str):
            return obj
        elif isinstance(obj, list):
            return [self.__fb2py(fb, d) for d in obj]
        else:
            return obj

    @gen.engine
    def load_properties(self, callback, id: str, properties):
        """

        :rtype : dict
        :param id:
        :param _properties:
        :param params:
        """
        if id in self.__cache:
            logging.debug("Using cached copy for %r", id)
            callback(self.__cache[id])
        else:
            q = dict()
            properties = list(properties)
            properties.sort()
            for offset in range(0, len(properties), 16):
                qtemp = [("id", id)] + [("type", [])] + [(properties[i], [{}]) for i in
                                                         range(offset, min(offset + 16, len(properties)))]
                query = dict(qtemp)
                tmp = yield gen.Task(self.mql, query=query)
                q.update(tmp)
            if not self.__cache is None:
                self.__cache[id] = q
            callback(q)

    @gen.engine
    def load_object(self, callback, id: str, properties):
        """
        :rtype : Object
        :param id:
        :param properties:
        :param params:
        """
        props = yield gen.Task(self.load_properties,
                               id=id, properties=properties)
        callback(self.__fb2py(self, props))

    @gen.engine
    def load_type(self, callback, type_id):
        """
        fetches basic information about a freebase type

        :param type_id: id of the type to be returned
        :return: a representation of the type
        :type type_id: str
        """
        props = yield gen.Task(self.load_properties,
                               id=type_id,
                               properties=[
                                   "/type/type/properties",
                                   "/type/type/domain",
                                   "/type/type/expected_by",
                                   "/type/type/default_property"
                               ])

        callback(self.__fb2py(self, props))

    def create_request_url(self, service_url, **additional_params) -> str:
        """

        :param service_url:
        :param api_key:
        :return:
        """
        params = dict(self.params)
        if self.api_key is not None:
            params['api_key'] = self.api_key
        params.update(additional_params)
        logging.debug("Call Freebase API %r", params)
        return self.base_url + service_url + '?' + urllib.parse.urlencode(params)

    @gen.engine
    def request(self, callback, service_url, **params):
        """

        :param service_url:
        :param params:
        :callback
        :return:
        """
        url = self.create_request_url(service_url, **params)
        logging.debug("fetch %s", url)
        response = yield gen.Task(self.http_client.fetch, url)
        callback(json.loads(response.body.decode('utf-8'))['result'])

    @gen.engine
    def mql(self, callback, query):
        """

        :param query:
        :param params:
        :type query: dict
        :return:
        """
        logging.debug("Running MQL %r", query)
        response = yield gen.Task(self.request, service_url="mqlread", query=json.dumps(query))
        res = dict(response)
        final = []
        for k, v in res.items():
            final.append((k, v))
        callback(dict(final))

    @gen.engine
    def search(self, callback, query):
        """

        :param query:
        :param filter:
        :param params:
        """
        callback(self.request, service_url='search', query=query)




