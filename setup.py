__author__ = 'Vukasin Toroman'

from distutils.core import setup

setup(
    name='freebase_rdf',
    version='0.0.1',
    packages=['freebase'],
    url='https://bitbucket.org/vtoroman/freebase-rdf',
    license='All Rights Reserved',
    author='Vukasin Toroman',
    author_email='vtoroman@attensity.com',
    #package_dir={'rdfrete': 'rules'},
    requires=["rdflib", "tornado"],
    description='A simple rdf client for freebase'
)
