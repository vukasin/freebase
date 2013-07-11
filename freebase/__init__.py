import shelve
import json
import rdflib
import urllib.request
import urllib.parse
import logging

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

    def get_rdf(self, ns='http://rdf.freebase.com'):
        id = rdflib.URIRef(ns + self.id)
        for t in self.type:
            yield (id, rdflib.RDF.type, rdflib.URIRef(ns + t))
        if self.name is not None:
            yield (id, rdflib.RDFS.label, rdflib.Literal(self.name))

    def load(self, *properties):
        if len(properties) == 0:
            properties = ["*"]
        return self.freebase.load_object(self.id, *properties)


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

    @property
    def property_names(self):
        if not self.__properties:
            props = set()
            for t in self.type:
                type_obj = self.freebase.load_type(t)
                for prop in type_obj.properties['/type/type/properties']:
                    props.add(prop.id)
            self.__properties = props
        return self.__properties

    def load(self, *properties):
        self.__load_data(self.freebase.load_properties(self.id, *properties))

    def load_all(self):
        self.load(*self.property_names)

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

    def load_properties(self, id: str, *_properties) -> dict:
        """

        :rtype : dict
        :param id:
        :param _properties:
        :param params:
        """
        if id in self.__cache:
            logging.debug("Using cached copy for %r", id)
            return self.__cache[id]
        q = dict()
        properties = list(_properties)
        properties.sort()
        for offset in range(0, len(properties), 16):
            qtemp = [("id", id)] + [("type", [])] + [(properties[i], [{}]) for i in range(offset, min(offset + 16, len(properties)))]
            query = dict(qtemp)
            tmp = self.mql(query=query)
            q.update(tmp)
        if not self.__cache is None:
            self.__cache[id] = q
        return q

    def load_object(self, id: str, *properties) -> Object:
        """
        :rtype : Object
        :param id:
        :param properties:
        :param params:
        """
        return self.__fb2py(self, self.load_properties(id, *properties))

    def load_type(self, type_id) -> Object:
        """
        fetches basic information about a freebase type

        :param type_id: id of the type to be returned
        :return: a representation of the type
        :type type_id: str
        """
        return self.__fb2py(self, self.load_properties(type_id,
                                                        "/type/type/properties",
                                                        "/type/type/domain",
                                                        "/type/type/expected_by",
                                                        "/type/type/default_property"))

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

    def request(self, service_url, **params):
        """

        :param service_url:
        :param params:
        :return:
        """
        url = self.create_request_url(service_url, **params)
        response = json.loads(urllib.request.urlopen(url).read().decode('utf-8'))
        return response['result']

    def mql(self, query):
        """

        :param query:
        :param params:
        :type query: dict
        :return:
        """
        logging.debug("Running MQL %r", query)
        res = dict(self.request("mqlread", query=json.dumps(query)))
        final = []
        for k, v in res.items():
            final.append((k, v))
        return dict(final)

    def search(self, query):
        """

        :param query:
        :param filter:
        :param params:
        """
        return self.request(service_url='search', query=query)




