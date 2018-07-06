from celery.task import task
from subprocess import call,STDOUT
import requests
from requests import exceptions
from tasks import solrDeleteIndex, solrIndexSampleData
import json

@task()
def resetSolrIndex(local_file,request_data):
    queuename = resetSolrIndex.request.delivery_info['routing_key']
    workflow = (solrDeleteIndex.si().set(queue=queuename) |
                solrIndexSampleData.si().set(queue=queuename))()
    return "Succefully reset Solr Index Workflow(local_file='{0}',request_data='{1}')".format(local_file,request_data)
