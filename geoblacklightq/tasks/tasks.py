#from celery.task import task
from celery import Celery
import celeryconfig
from subprocess import call, STDOUT
import requests
import json
import os
from requests import exceptions

app = Celery()
app.config_from_object(celeryconfig)

# Default base directory
# basedir="/data/static/"
#os.getenv('WRKSPACE', "geocolorado")
solr_index = os.getenv('SOLR_INDEX', 'geoblacklight')
solr_connection = os.getenv(
    'GEO_SOLR_URL', "http://geoblacklight_solr:8983/solr")
#solr_connection = "http://geoblacklight_solr:8983/solr"

# Example task
@app.task()
def getUserData():
    """ 
    Example task to return User information within task
    """
    user_data=getUserData.request.options
    return user_data


@app.task()
def solrIndexSampleData(catalog_collection='geoblacklight', solr_index=solr_index):
    """
    Provide a list of JSON items to be index within the
    Geoportal Solr index.
    args: items - List of json objects
    """
    data = open('geoblacklight-documents.json', 'r').read()
    #data_url= requests.get(data_url)
    #data = r.json()
    headers = {'Content-Type': 'application/json'}
    url = "{0}/{1}/update?commit=true".format(solr_connection, solr_index)
    sr = requests.post(url, data=data, headers=headers)
    return {"status": sr.status_code, "url": url, "response": sr.json()}
    #solr = pysolr.Solr(solr_connection, timeout=10)


@app.task()
def solrDeleteIndex(solr_index=solr_index):
    """
    Delete Solr Index:
    kwargs: solr_index='geoblacklight'
    """
    headers = {'Content-Type': 'text/xml'}
    url = "{0}/{1}/update?commit=true".format(solr_connection, solr_index)
    data = '<delete><query>*:*</query></delete>'
    sr = requests.post(url, data, headers=headers)
    return {"status": sr.status_code, "url": url, "response": sr.text}


@app.task()
def solrIndexItems(items, solr_index=solr_index):
    """
    Index items to GeoPortal
    """
    headers = {'Content-Type': 'application/json'}
    url = "{0}/{1}/update?commit=true".format(solr_connection, solr_index)
    #results =[]
    # for itm in items:
    sr = requests.post(url, json=items, headers=headers)
    #results.append({"status":sr.status_code,"url":url,"response": sr.json()})
    return {"status": sr.status_code, "url": url, "response": sr.json()}


@app.task()
def solrSearch(query, solr_index=solr_index):
    headers = {'Content-Type': 'application/json'}
    url = "{0}/{1}/select?q={2}".format(solr_connection, solr_index, query)
    sr = requests.get(url, headers=headers)
    return sr.json()
