from celery.task import task
from subprocess import call,STDOUT
import requests
from requests import exceptions
from tasks import solrDeleteIndex, solrIndexSampleData
from geotransmeta import unzip,geoBoundsMetadata, determineTypeBounds
import json

@task()
def resetSolrIndex(local_file,request_data):
    queuename = resetSolrIndex.request.delivery_info['routing_key']
    workflow = (solrDeleteIndex.si().set(queue=queuename) |
                solrIndexSampleData.si().set(queue=queuename))()
    return "Succefully Reset Solr Index Workflow(local_file='{0}',request_data='{1}')".format(local_file,request_data)

@task()
def geoLibraryLoader(localfile,request_data):
    queuename = geoLibraryLoader.request.delivery_info['routing_key']
    workflow = (unzip.s().set(queue=queuename) |
                determineTypeBounds.s().set(queue=queuename))()
    return "Succefully submitted geoLibrary initial workflow(local_file='{0}',request_data='{1}')".format(local_file,request_data)
