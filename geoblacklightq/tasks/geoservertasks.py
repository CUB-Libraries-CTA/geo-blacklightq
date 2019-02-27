from celery.task import task
from subprocess import call, STDOUT
from geoserver.catalog import Catalog, ConflictingDataError, FailedRequestError
from geoserver.util import shapefile_and_friends
from requests.auth import HTTPBasicAuth
import requests
import os
import json
import xmltodict

workspace = os.getenv('WRKSPACE', "geocolorado")
geoserver_connection = "https://geo.colorado.edu/geoserver"
geoserver_username = os.getenv('GEOSVR_USER', "admin")
geoserver_password = os.getenv('GEOSRV_PASS')


def getBoundingBox(owsBBox):
    solr_geom = 'ENVELOPE({0},{1},{2},{3})'
    lc = owsBBox["ows:LowerCorner"].split(' ')
    uc = owsBBox["ows:UpperCorner"].split(' ')
    return solr_geom.format(lc[0], uc[0], uc[1], lc[1])


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
    url = "{0}/{1}/ows?SERVICE=WFS&REQUEST=GetCapabilities".format(
        geoserver_connection, workspace)
    r = requests.get(url)
    doc = xmltodict.parse(r.text)
    ftdata = json.loads(json.dumps(
        doc['wfs:WFS_Capabilities']['FeatureTypeList']["FeatureType"]))
    results = []
    for itm in ftdata:
        data = {"name": itm['Name']}
        data["title"] = itm['Title']
        data['crs'] = itm['DefaultCRS']
        data['boundbox'] = getBoundingBox(itm['ows:WGS84BoundingBox'])
        results.append(data)
    return results


@task()
def dataLoadGeoserver(data):
    geoserverStoreName = data["folder"].split('/')[-1]
    if data['type'] == 'shapefile':
        shapefileName = os.path.splitext(os.path.basename(data['file']))[0]
        filename = "{0}/{1}".format(data['folder'], shapefileName)
        bbox = createDataStore(
            geoserverStoreName, filename, format=data['type'])
        data["msg"] = "{0} {1}".format(data["msg"], bbox["msg"])
        data["bounds"] = bbox["solr_geom"]
        data["resource_type"] = bbox["resource_type"]
    elif data['type'] == 'image':
        fileUrl = "file:{0}".format(data['file'][1:].replace(
            'geoserver-data', 'geoportal_data', 1))
        bbox = createDataStore(
            geoserverStoreName, fileUrl, format=data['type'])
        data["msg"] = "{0} {1}".format(data["msg"], bbox["msg"])
        data["bounds"] = bbox["solr_geom"]
        data["resource_type"] = bbox["resource_type"]
    else:
        data["msg"] = "{0} {1}".format(
            data["msg"], "Data element not georeferenced. IIIF server not implemented.")
    data['geoserverStoreName'] = geoserverStoreName
    return data


@task()
def createDataStore(name, filename, format="shapefile"):
    cat = Catalog("{0}/rest/".format(geoserver_connection),
                  geoserver_username, geoserver_password)
    ws = cat.get_workspace(workspace)
    msg = ""
    if format == "shapefile":
        shapefile = shapefile_and_friends(filename)
        try:
            cat.create_featurestore(name, shapefile, workspace)
        except ConflictingDataError as inst:
            msg = str(inst)
        except:
            raise
        resource = cat.get_resource(name, workspace=ws)
        resource.projection = 'EPSG:4326'
        cat.save(resource)
        resource.projection_policy = 'REPROJECT_TO_DECLARED'
        cat.save(resource)
        resource.refresh()
        bbox = resource.latlon_bbox[:4]
        solr_geom = 'ENVELOPE({0},{1},{2},{3})'.format(
            bbox[0], bbox[1], bbox[3], bbox[2])
        return {"solr_geom": solr_geom, "msg": msg, "resource_type": resource.resource_type}
    elif format == "image":
        if cat.get_store(name):
            newcs = cat.get_store(name, workspace=ws)
            msg = "Geoserver datastore already existed. Update existing datastore."
        else:
            newcs = cat.create_coveragestore2(name, ws)
        newcs.type = "GeoTIFF"
        newcs.url = filename
        cat.save(newcs)
        # add coverage
        url = "{0}/rest/workspaces/{1}/coveragestores/{2}/coverages.json"
        url = url.format(geoserver_connection, ws.name, name)
        headers = {"Content-Type": "application/json"}
        coverageName = os.path.splitext(os.path.basename(filename))[0]
        postdata = {"coverage": {"nativeCoverageName": coverageName, "name": coverageName,
                                 'projectionPolicy': 'REPROJECT_TO_DECLARED', 'srs': 'EPSG:4326'}}
        requests.post(url, json.dumps(postdata), headers=headers,
                      auth=(geoserver_username, geoserver_password))
        # Reproject
        resource = cat.get_resource(name, workspace=ws)
        url = "{0}/rest/workspaces/{1}/coveragestores/{2}/coverages/{2}?{3}"
        parameters = "recalculate=nativebbox,latlonbbox"
        url = url.format(geoserver_connection, ws.name, name, parameters)
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        requests.post(url, headers=headers, auth=(
            geoserver_username, geoserver_password))
        # resource.refresh()
        bbox = resource.latlon_bbox[:4]
        solr_geom = 'ENVELOPE({0},{1},{2},{3})'.format(
            bbox[0], bbox[1], bbox[3], bbox[2])
        return {"solr_geom": solr_geom, "msg": msg, "resource_type": resource.resource_type}
    return True


@task()
def getstyles():
    """
    Returns a list of available Geoserver Styles
    """
    url = "{0}/rest/styles.json"
    url = url.format(geoserver_connection)
    headers = {"Content-Type": "application/json"}
    result = requests.get(url, headers=headers, auth=(
        geoserver_username, geoserver_password))
    data = result.json()
    return data['styles']['style']


@task()
def setLayerDefaultStyle(layername, stylename):
    """
    Set layer default style
    args:
        layername (string)
        stylename (string)
    """
    url = "{0}/rest/layers/{1}.json"
    url = url.format(geoserver_connection, layername)
    headers = {"Content-Type": "application/json"}
    data = {"layer": {"defaultStyle": stylename}}
    result = requests.put(url, data=json.dumps(data), headers=headers, auth=(
        geoserver_username, geoserver_password))
    if not result.text:
        msg = "Geoserver Accepted Default Style"
    else:
        msg = result.text
    return {"method": "put", "status_code": result.status_code, "msg": msg}


@task()
def deleteGeoserverStore(storeName, workspace=workspace, purge=None, recurse=True):
    """
    Delete Geoserver Data Store.
    Args:
        storeName
    Kwargs (Default):
        workspace(geocolorado)
        purge(None) - if purge is True. Data files will be deleted.
        recurse (True) - Store and all metadata items will be deleted.
    """

    cat = Catalog("{0}/rest/".format(geoserver_connection),
                  geoserver_username, geoserver_password)
    ws = cat.get_workspace(workspace)
    ds = cat.get_store(storeName, workspace=ws)
    cat.delete(ds, purge=purge, recurse=recurse)
    msg = "metadata and data files removed." if purge else "only metadata items removed."
    return "DataStore: {0} deleted from geoServer with {1}".format(storeName, msg)
