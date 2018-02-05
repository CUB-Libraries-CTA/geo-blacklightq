from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
import requests

#Default base directory
#basedir="/data/static/"

solr_connection="http://geoblacklight_solr:8983/solr/"
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
def solr_index(items):
    """
    Provide a list of JSON items to be index within the
    Geoportal Solr index.
    args: items - List of json objects
    """
    solr = pysolr.Solr(solr_connection, timeout=10)
