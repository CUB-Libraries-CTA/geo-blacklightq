from celery.task import task
from subprocess import call, STDOUT
import requests
import os
from requests import exceptions
from .tasks import solrDeleteIndex, solrIndexSampleData, solrIndexItems
from .geotransmeta import unzip, geoBoundsMetadata, determineTypeBounds
from .geotransmeta import configureGeoData, crossWalkGeoBlacklight
from .geoservertasks import dataLoadGeoserver
import json

wwwdir = "/data/static"
# No slash at end of API URL
cybercom_api_url = os.getenv(
    "CYBERCOM_API_URL", "https://geo.colorado.edu/api")


@task()
def resetSolrIndex(items=None):
    """
    Delete current solr index and indexs items sent in Args
    Args:
        items (list of objects) defaults to index all  if items not provided.
    returns:
        acknowledgement of workflow submitted.
        Children chain: solrDeleteIndex --> solrIndexItems
    """
    if not items:
        headers = {'Content-Type': 'application/json'}
        query = 'query={"filter":{"status":"indexed"},"projection":{"_id":0,"style":0,"status":0}}'
        url = '{0}/catalog/data/catalog/geoportal.json?{1}'.format(
            cybercom_api_url, query)
        sr = requests.get(url, headers=headers)
        data = sr.json()
        items = data['results']
    queuename = resetSolrIndex.request.delivery_info['routing_key']
    workflow = (solrDeleteIndex.si().set(queue=queuename) |
                solrIndexItems.si(items).set(queue=queuename))()
    return "Succefully Workflow Submitted: children workflow chain: solrDeleteIndex --> solrIndexItems"

@task()
def geoDataImport(s3_file_location, force=True):
    """
    Workflow to handle import of zipfile
    """
    print(s3_file_location)
 

@task()
def geoLibraryLoader(local_file, request_data, force=True):
    """
    Workflow to handle initial import of zipfile:
    --> Unzip
    --> Identify file type(shapefile, image, iiif)
    --> return Bounds
    --> Check for xml metadata file
    --> Initial cross walk of xml to geoblacklight schema
    Return data to applicaiton to display (Human interaction)

    Workflow is called from /upload with form that has taskname

    force (boolean): If data already uploaded will delete and replace.
    """
    task_id = str(geoLibraryLoader.request.id)
    resultDir = os.path.join(wwwdir, 'geo_tasks', task_id)
    os.makedirs(resultDir)
    if 'force' in request_data:
        force = request_data['force']
    queuename = geoLibraryLoader.request.delivery_info['routing_key']
    workflow = (unzip.s(local_file).set(queue=queuename) |
                determineTypeBounds.s().set(queue=queuename) |
                dataLoadGeoserver.s().set(queue=queuename) |
                configureGeoData.s(resultDir).set(queue=queuename) |
                crossWalkGeoBlacklight.s().set(queue=queuename))()
    return "Succefully submitted geoLibrary initial workflow"
