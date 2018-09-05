from celery.task import task
from subprocess import call,STDOUT
import requests, os,json, xmltodict


workspace = os.getenv('WRKSPACE',"geocolorado")
geoserver_connection="https://geo.colorado.edu/geoserver"

def getBoundingBox(owsBBox):
    solr_geom = 'ENVELOPE({0},{1},{2},{3})'
    lc=owsBBox["ows:LowerCorner"].split(' ')
    uc=owsBBox["ows:UpperCorner"].split(' ')
    return solr_geom.format(lc[0],uc[0],uc[1],lc[1])

@task()
def geoserverGetWorkspaceMetadata(workspace=workspace):
    """
    Task returns a list of all layers within workspace
    args:
        None
    Kwargs:
        workspace(string): default 'geocolorado'
    Return:
        List of objects:
            name,title,crs,boundbox
    """
    url= "{0}/{1}/ows?SERVICE=WFS&REQUEST=GetCapabilities".format(geoserver_connection,workspace)
    r=requests.get(url)
    doc = xmltodict.parse(r.text)
    ftdata=json.loads(json.dumps(doc['wfs:WFS_Capabilities']['FeatureTypeList']["FeatureType"]))
    results=[]
    for itm in ftdata:
        data={"name":itm['Name']}
        data["title"]=itm['Title']
        data['crs']= itm['DefaultCRS']
        data['boundbox']= getBoundingBox(itm['ows:WGS84BoundingBox'])
        results.append(data)
    return results
