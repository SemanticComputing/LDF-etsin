#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Create Etsin API calls from LDF.fi dataset descriptions
"""

from collections import defaultdict

import requests
from SPARQLWrapper import JSON, SPARQLWrapper

ENDPOINT = 'http://ldf.fi/service-descriptions/sparql'

DATA_QUERY = '''PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX sd: <http://www.w3.org/ns/sparql-service-description#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX sf: <http://ldf.fi/functions#>

SELECT ?data
    ?title 
    #(GROUP_CONCAT(?licenses ; separator=" , ") as ?license) 
    ?contact_name 
    ?contact_email 
    ?contact_URL 
    ?contact_phone 
    ?direct_download_URL
    #(GROUP_CONCAT(?author_organisations ; separator=" , ") as ?author) 
    ?agent2_role 
    ?agent2_organisation 
    ?langdis 
    ?fin
    ?eng 
WHERE {
  ?data a sd:Dataset .
  # TODO: version
  OPTIONAL {?data dct:title ?title }
  OPTIONAL {?data dct:license ?licenses }
  # TODO: owner_org
  
  OPTIONAL {?data dct:publisher/sf:preferredLanguageLiteral ( skos:prefLabel 'fi' 'en' '' ?contact_name ) }
  OPTIONAL {?data dct:publisher/foaf:mbox ?contact_email } 
  OPTIONAL {?data dct:publisher/foaf:homepage ?contact_URL . }
  OPTIONAL {?data dct:publisher/foaf:phone ?contact_phone . }
  OPTIONAL {?data foaf:homepage ?direct_download_URL . }
  
  OPTIONAL {?data dct:creator ?author_organisations . }
  BIND( "owner" as ?agent2_role ) .
  OPTIONAL {?data dct:rightsHolder/skos:prefLabel ?agent2_organisation . }
  
  BIND( False as ?langdis ) .
  BIND (EXISTS { ?data ?p ?lit . FILTER ( lang(?lit) = "fi" ) . } as ?fin)
  BIND (EXISTS { ?data ?p ?lit . FILTER ( lang(?lit) = "en" ) . } as ?eng)
   
} GROUP BY ?data ?title ?license ?contact_name ?contact_email ?contact_URL ?contact_phone ?direct_download_URL ?agent1_role ?agent1_organisation ?agent2_role ?agent2_organisation ?langdis ?fin ?eng
ORDER BY ?title
'''


def format_sparql_results(sparql_res):
    """
    Format SPARQL results to a dict of lists grouped by dataset URI

    :param sparql_res: SPARQL results in JSON format
    :return: dict of lists
    """
    datasets = defaultdict(lambda: defaultdict(list))

    # Loop through result rows
    for result in sparql_res["results"]["bindings"]:
        dataset_key = result['data']['value']
        dataset = datasets[dataset_key]

        for (key, value) in result.items():
            value = value['value']
            if value not in dataset[key]:
                dataset[key].append(value)

        datasets[dataset_key].update(dataset)

    return datasets

sparql = SPARQLWrapper(ENDPOINT)
sparql.setQuery(DATA_QUERY)
sparql.setReturnFormat(JSON)

results = {}
try:
    results = sparql.query().convert()
except ValueError:
    print('Error while parsing SPARQL results.')
    quit()

datasets = format_sparql_results(results)

