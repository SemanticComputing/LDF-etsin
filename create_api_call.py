#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Create Etsin API calls from LDF.fi dataset descriptions
"""
import argparse
import copy
from collections import defaultdict
import json
from pprint import pprint

import requests
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
    """
    Format a dataset dictionary
    :param dataset:
    :return:
    """
    formatted = defaultdict(list)

    # BASIC PROCESSING TO LISTS OF DICTS STRUCTURE

    for k, v in dataset.items():
        keys = k.split('___')
        if len(keys) == 1:
            formatted[keys[0]] = v[0] if type(v) == list and len(v) == 1 else v
        else:
            if keys[0][-1] in '0123456789':
                index = int(keys[0][-1])
                keys[0] = keys[0][:-1]
            else:
                index = 0

            if len(formatted[keys[0]]) > index:
                formatted[keys[0]][index].update({keys[1]: v})
            else:
                formatted[keys[0]].append({keys[1]: v})

    # POST PROCESS INNER DICTS WITH MULTIPLE NAMES, SPLIT THEM TO NEW DICTS

    for k, v in copy.deepcopy(formatted).items():
        if type(v) == list and type(v[0]) == dict:
            for idict_index, idict in enumerate(v):
                for ik, iv in idict.items():
                    if len(iv) > 1:
                        for d_index, duplicate in enumerate(iv[1:]):

                            new_dict = copy.deepcopy(formatted[k][idict_index])
                            new_dict[ik].pop(0)

                            formatted[k][idict_index][ik].pop(d_index + 1)
                            formatted[k].append(new_dict)

    # POST PROCESS INNER LISTS OF ONE ITEM TO JUST THE ITEMS

    for k, v in formatted.items():
        if type(v) == list and type(v[0]) == dict:
            for idict_index, idict in enumerate(v):
                for ik, iv in idict.items():
                    # Assuming iv is a list
                    formatted[k][idict_index][ik] = iv[0]
                    if len(iv) > 1:
                        print('Found a list with length > 1 when there should be only one item')

    # ADDITIONAL POST PROCESSING

    formatted['version'] = formatted['version'][:19]

    if 'accept_terms' in formatted:
        formatted['accept-terms'] = formatted.pop('accept_terms')

    tags = formatted.get('tag_string')
    if tags:
        if type(tags) == list:
            formatted['tag_string'] = ", ".join(tags)

    # TRANSFORM TITLE AND NOTES INTO STRINGS

    formatted['title'] = str(formatted['title'][0])
    formatted['notes'] = str(formatted['notes'][0])

    licenses = requests.get('https://etsin.avointiede.fi/licenses.json')
    licenses = licenses.json()

    lic_urls = formatted.get('license_URL')
    if type(lic_urls) == str:
        lic_urls = [lic_urls]

    found = False
    if lic_urls:
        for (index, lic_url) in enumerate(lic_urls):
            for lic in licenses:
                if lic['url'] in [lic_url, lic_url.replace('http:', 'https:')]:
                    formatted['license_id'] = lic['id']
                    if len(lic_urls) > 1:
                        formatted['license_URL'] = lic_urls.copy()
                        formatted['license_URL'].pop(index)
                    else:
                        formatted.pop('license_URL')
                    found = True
                    break
            if found:
                break

    return json.dumps(formatted).replace("'", '\\"')


##############################

# if __name__ == '__main__':

argparser = argparse.ArgumentParser(description="Convert LDF.fi dataset metadata to Etsin format")
argparser.add_argument("dataset", help="Dataset URI, or all for all datasets")
args = argparser.parse_args()

sparql = SPARQLWrapper(ENDPOINT)
with open('get_datasets.sparql', 'r') as f:
    sparql.setQuery(f.read())

sparql.setReturnFormat(JSON)

results = {}
try:
    results = sparql.query().convert()
except ValueError:
    print('Error while parsing SPARQL results.')
    quit()

datasets = format_sparql_results(results)

if args.dataset == 'all':
    used_datasets = datasets
else:
    used_datasets = {args.dataset: datasets.get(args.dataset)}

for (uri, dataset) in used_datasets.items():
    final_dataset = format_dataset_for_api(dataset)

    print('\n\n')
    print('DATASET {uri}:'.format(uri=uri))
    pprint(json.loads(final_dataset))
    print()
    print('API CALL:')
    print('curl "https://etsin.avointiede.fi/api/3/action/package_create" -d \'{dataset}\' -H "Authorization: MY_PRIVATE_KEY"'
          .format(dataset=final_dataset))
