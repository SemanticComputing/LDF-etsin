#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Create Etsin API calls from LDF.fi dataset descriptions
"""

import argparse

import requests
from SPARQLWrapper import JSON, SPARQLWrapper

ENDPOINT = 'http://ldf.fi/service-descriptions/sparql'

argparser = argparse.ArgumentParser(description="Create an Etsin API call from an LDF.fi dataset")
argparser.add_argument("--dataset", default="", help="URI of the LDF.fi dataset")
args = argparser.parse_args()

sparql = SPARQLWrapper(ENDPOINT)
sparql.setQuery('PREFIX sd: <http://www.w3.org/ns/sparql-service-description#> SELECT * WHERE { ?data a sd:Dataset . }')
sparql.setReturnFormat(JSON)

results = {}
try:
    results = sparql.query().convert()
except ValueError:
    print('Error while parsing SPARQL results.')
    quit()

for result in results["results"]["bindings"]:
    dataset_uri = result['data']['value']

