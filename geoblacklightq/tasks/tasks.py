from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
import requests
from requests import exceptions

#Default base directory
#basedir="/data/static/"

solr_connection="http://geoblacklight_solr:8983/solr"
#Example task
@task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result

@task()
def solr_index(catalog_collection='geoblacklight',solr_index='geoblacklight'):
    """
    Provide a list of JSON items to be index within the
    Geoportal Solr index.
    args: items - List of json objects
    """
    data = open('geoblacklight-documents.json','r').read()
    #data_url= requests.get(data_url)
    #data = r.json()
    headers = {'Content-Type':'application/json'}
    url = "{0}/{1}/update".format(solr_connection,solr_index)
    sr = requests.post(url,data,headers=headers)
    return sr.text
    #solr = pysolr.Solr(solr_connection, timeout=10)

@task()
def solr_index_delete_all(solr_index='geoblacklight'):
    headers = {'Content-Type':'text/xml'}
    url = "{0}/{1}/update?commit=true".format(solr_connection,solr_index)
    data ='<delete><query>*:*</query></delete>'
    sr = requests.post(url,data,headers=headers)
    return sr.text
