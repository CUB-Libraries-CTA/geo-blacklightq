from celery.task import task
from subprocess import call,STDOUT
from requests import exceptions
from glob import iglob
import re, fnmatch
import requests, zipfile, fiona, shutil
import os, tempfile, rasterio,xmltodict

#set tmp direcotry. Assign a specific directory with environmental variable
tmpdir = os.getenv('TMPDIR',tempfile.gettempdir())
resulturl= "https://geo.colorado.edu/apps/geo_tasks/"

def findfiles(patterns, where='.'):
    '''Returns list of filenames from `where` path matched by 'which'
       shell pattern. Matching is case-insensitive. return de-duped list just
       in case multiple patterns hit the same filename
    '''
    result=[]
    for pattern in patterns:
        # TODO: recursive param with walk() filtering
        rule = re.compile(fnmatch.translate(pattern), re.IGNORECASE)
        result = result +  [name for name in os.listdir(where) if rule.match(name)]
    return list(set(result))

def deep_get(_dict, keys, default=None):
    keys=keys.split('.')
    def _reducer(d, key):
        if isinstance(d, dict):
            return d.get(key, default)
        return default
    return reduce(_reducer, keys, _dict)

@task()
def unzip(filename,destination=None,force=False):
    """
    This task unzips content into directory.

    Signature:
    <blockquote>unzip(filename,destination=None)</blockquote>
    Args:
        filename (string): location of zipfile
    kwargs
        destination (string): directory name - default assigns the zipfile name.
            Do not include path with the destination name.
    Returns:
        (string): path to the unziped directory
    """
    if not destination:
        destination=os.path.splitext(os.path.basename(filename))[0]
    destination = os.path.join(tmpdir,destination)
    if os.path.exists(destination):
        if force:
            shutil.rmtree(destination)
        else:
            return {"folder": destination,"zipdata":False}
    zip_ref = zipfile.ZipFile(filename,'r')
    zip_ref.extractall(destination)
    return {"folder": destination,"zipdata":True}

@task()
def determineTypeBounds(data):
    folder= data["folder"]
    msg = "Initial upload"
    if not data["zipdata"]:
        msg="Data uploaded previously - Use force to remove and reload."
    type = None
    file = None
    bounds= None
    shapefiles = findfiles(['*.shp'],where=folder)
    if shapefiles:
        type="shapefile"
        file = os.path.join(folder,shapefiles[0])
        bounds=geoBoundsMetadata(file)
    else:
        try:
            imgfiles=findfiles(['*.jpg','*.tif','*.tiff','*.png'],where=folder)
            if imgfiles:
                file = os.path.join(folder,imgfiles[0])
                bounds=geoBoundsMetadata(file,format="image")
                type="image"
        except:
            type="iiif"
            bounds=None
    return {"file":file,"folder":folder,"bounds":bounds,"type":type,"msg":msg}

@task()
def configureGeoData(data,resultDir):
    xmlfiles = findfiles(['*.xml'],where=data["folder"])
    xmlurls=[]
    fgdclist=[]
    for xml in xmlfiles:
        shutil.copy(os.path.join(data['folder'],xml),resultDir)
        xmlurls.append(os.path.join(resulturl,resultDir.split('/')[-1],xml))
        import xmltodict

        with open(os.path.join(data['folder'],xml)) as fd:
            stringxml = fd.read()
            if 'FGDC' in stringxml.upper():
                fgdc={}
                fgdc['url']=os.path.join(resulturl,resultDir.split('/')[-1],xml)
                doc = xmltodict.parse(stringxml)
                fgdc['data']=doc
                fgdclist.append(fgdc)
    data['xml']={"urls":xmlurls,"fgdc":fgdclist}
    return data

@task()
def geoBoundsMetadata(filename,format="shapfile"):
    """
    This task finds bounding box of georeferenced shapefile or raster.

    Signature:
        geoBoundsMetadata(filename,format="shapfile")
    Args:
        filename (string): location of georeferenced file
    kwargs
        format (string): default="shapefile" if not shapefile defaults to raster
            Only choice is to leave as shapefile or 'raster' for image.
    Returns:
        (string): with bounding box.
            path to the unziped directory
    """
    if format=="shapfile":
        with fiona.open(filename, 'r') as c:
            bnd= c.bounds
            bnd=(bnd[0],bnd[2],bnd[3],bnd[1])
            return "ENVELOPE{0}".format(bnd)

    else:
        with rasterio.open(filename,'r') as c:
            bnd= c.bounds
            bnd=(bnd[0],bnd[2],bnd[3],bnd[1])
            return "ENVELOPE{0}".format(bnd)
