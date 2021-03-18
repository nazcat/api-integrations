#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests

url = 'http://now.core.api.espn.com/allsports/apis/v1/now?content=story&enable=authors,headshots,peers&insider=only'

r = requests.get(url)
json = r.json().get('feed')

articles = []

for x in json:
    if(x.get('premium') == True):
       print(x.get('headline'))
       print(x.get('byline'))
       print("League: "+x.get('section'))
       print(x.get('links').get('web').get('href'))
       print("\n")
       print(x.get('description'))
       print(x.get('categories')[0].get('league').get('description'))
       print(\"---\") 
       
       articles.append({'League':x.get('section'), \
                        'Headline':x.get('headline'),\
                        'Byline':x.get('byline'),\
                        'Description':x.get('description'),\
                        'Link':x.get('links').get('web').get('href')})

articles

import csv
keys = articles[0].keys()
with open('eplus.csv', 'w', newline='')  as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(articles)
