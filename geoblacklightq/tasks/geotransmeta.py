from celery.task import task
from subprocess import call,STDOUT
from requests import exceptions
from glob import iglob
import re, fnmatch, jinja2, json
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
    data['xmlurls']=xmlurls
    data['xml']={"urls":xmlurls,"fgdc":fgdclist}
    return data

@task()
def crossWalkGeoBlacklight(data, templatename='geoblacklightSchema.tmpl',type='FGDC'):
    """
    Crosswalk
    """
    # load template
    templateLoader = jinja2.FileSystemLoader( searchpath=os.path.dirname(os.path.realpath(__file__)) )
    templateEnv = jinja2.Environment( loader=templateLoader )
    template = templateEnv.get_template("templates/{0}".format(templatename))
    crosswalkData = template.render(assignMetaDataComponents(data))
    data['geoblacklight-schema']=json.loads(crosswalkData)
    return data

def assignMetaDataComponents(data,type='fgdc'):
    dataJsonObj=deep_get(data,"xml.fgdc",[])
    if len (dataJsonObj)>0:
        dataJsonObj=deep_get(dataJsonObj[0],"data",{})
    else:
        dataJsonObj={}
    gblight={}
    gblight['uuid']= "DO NOT SET"
    gblight['dc_identifier_s'] = "DO NOT SET"
    gblight['dc_title_s'] = deep_get(dataJsonObj,"metadata.idinfo.citation.citeinfo.title","")
    gblight['dc_description_s'] = deep_get(dataJsonObj,"metadata.idinfo.descript.abstract","")
    gblight['dc_rights_s'] = "Public"
    gblight['dct_provenance_s'] = "University of Colorado Boulder"
    gblight['dct_references_s'] = "DO NOT SET"
    gblight['layer_id_s'] = "DO NOT SET"
    gblight['layer_slug_s'] = "DO NOT SET"
    gblight['layer_geom_type_s'] = ""
    gblight['dc_format_s'] =deep_get(dataJsonObj,"metadata.distInfo.distFormat.formatName.#text","")
    gblight['dc_language_s'] = "English"
    gblight['dc_type_s'] = "Dataset"
    gblight['dc_publisher_s'] = deep_get(dataJsonObj,"metadata.idinfo.citation.citeinfo.origin","")
    gblight['dc_creator_sm'] = [deep_get(dataJsonObj,"metadata.idinfo.citation.citeinfo.origin","")]
    subjects = deep_get(dataJsonObj,"metadata.idinfo.keywords.theme",[])
    subs=[]
    for itm in subjects:
        temp=deep_get(itm,"themekey",[])
        if not isinstance(temp, list):
            temp=[temp]
        subs = subs + temp
    gblight['dc_subject_sm'] = json.dumps(subs)
    pubdate=deep_get(dataJsonObj,"metadata.idinfo.citation.citeinfo.pubdate","")
    gblight['dct_issued_s'] = json.dumps(pubdate)
    gblight['dct_temporal_sm'] = [json.dumps(pubdate)]
    place =deep_get(dataJsonObj,"metadata.idinfo.keywords.place.placekey",[])
    if not isinstance(place, list):
        place=[place]
    gblight['dct_spatial_sm'] = json.dumps(place)
    gblight['solr_geom'] = "DO NOT SET"
    return gblight

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
