from celery.task import task
from subprocess import call,STDOUT
#from geoserver import catalog
#import Catalog
#from geoserver.util import shapefile_and_friends
import requests, os, json, xmltodict

workspace = os.getenv('WRKSPACE',"geocolorado")
geoserver_connection="https://geo.colorado.edu/geoserver"
geoserver_username=os.getenv('GEOSVR_USER',"admin")
geoserver_password= os.getenv('GEOSRV_PASS')


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

@task()
def createDataStore(name,filename, format="shapefile"):
    from geoserver import catalog
    from geoserver.util import shapefile_and_friends
    cat = catalog.Catalog("{0}/rest/".format(geoserver_connection),geoserver_username,geoserver_password)
    ws = cat.get_workspace(workspace)
    if format == "shapefile":
        shapefile=shapefile_and_friends(filename)
        ft = cat.create_featurestore(name, shapefile, workspace)
        resource=cat.get_resource(name)
        resource.projection='EPSG:4326'
        resource.projection_policy='REPROJECT_TO_DECLARED'
        cat.save(resource)
    elif format == "image":
        newcs= cat.create_coveragestore2(name,ws)
        newcs.type="GeoTIFF"
        url="something"

#@task()
#def getBoundingBox(name)

@task()
def deleteGeoserverStore(storeName,workspace=workspace, purge=None, recurse=True):
    """
    Delete Geoserver Data Store.
    Args:
        storeName
    Kwargs (Default):
        workspace(geocolorado)
        purge(None) - if purge is True. Data files will be deleted.
        recurse (True) - Store and all metadata items will be deleted.
    """
    from geoserver import catalog
    cat = catalog.Catalog("{0}/rest/".format(geoserver_connection),geoserver_username,geoserver_password)
    ws = cat.get_workspace(workspace)
    ds=cat.get_store(storeName,workspace=ws)
    cat.delete(ds,purge=purge,recurse=recurse)
    msg= "metadata and data files removed." if purge else "only metadata items removed."
    return "DataStore: {0} deleted from geoServer with {1}".format(storeName,msg)
