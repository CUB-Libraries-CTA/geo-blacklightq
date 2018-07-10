from celery.task import task
from subprocess import call,STDOUT
import requests, os
from requests import exceptions
from tasks import solrDeleteIndex, solrIndexSampleData
from geotransmeta import unzip,geoBoundsMetadata, determineTypeBounds
from geotransmeta import configureGeoData
import json

wwwdir = "/data/static"

@task()
def resetSolrIndex(local_file,request_data):
    queuename = resetSolrIndex.request.delivery_info['routing_key']
    workflow = (solrDeleteIndex.si().set(queue=queuename) |
                solrIndexSampleData.si().set(queue=queuename))()
    return "Succefully Reset Solr Index Workflow(local_file='{0}',request_data='{1}')".format(local_file,request_data)

@task()
def geoLibraryLoader(local_file,request_data,force=False):
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
    queuename = geoLibraryLoader.request.delivery_info['routing_key']
    workflow = (unzip.s(local_file).set(queue=queuename) |
                determineTypeBounds.s().set(queue=queuename) |
                configureGeoData.s(resultDir).set(queue=queuename))()
    return "Succefully submitted geoLibrary initial workflow"
