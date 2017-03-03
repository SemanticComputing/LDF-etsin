#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Create Etsin API calls from LDF.fi dataset descriptions
"""

from collections import defaultdict
import json

from SPARQLWrapper import JSON, SPARQLWrapper

ENDPOINT = 'http://ldf.fi/service-descriptions/sparql'


def format_sparql_results(sparql_res):
    """
    Format SPARQL results to a dict of lists grouped by dataset URI

    :param sparql_res: SPARQL results in JSON format
    :return: dict of lists
    """
    datasets = defaultdict(lambda: defaultdict(list))

    # Loop through result rows
    for result in sparql_res["results"]["bindings"]:
        dataset_key = result['pids___id']['value']
        dataset = datasets[dataset_key]

        for (key, value) in result.items():
            value = value['value']
            if value not in dataset[key]:
                dataset[key].append(value)

        datasets[dataset_key].update(dataset)

    return datasets


def format_dataset_for_api(dataset):
    # TODO: Format according to the API documentation
    return json.dumps(dataset)


sparql = SPARQLWrapper(ENDPOINT)
with open('get_datasets.sparql', 'r') as f:
    sparql.setQuery(f.read())  # TODO: Handle all literal language related stuff in SPARQL

sparql.setReturnFormat(JSON)

results = {}
try:
    results = sparql.query().convert()
except ValueError:
    print('Error while parsing SPARQL results.')
    quit()

datasets = format_sparql_results(results)

print()
print(datasets['http://ldf.fi/warsa'])
print()
print(format_dataset_for_api(datasets['http://ldf.fi/warsa']))
