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
            zipname = filename.split('/')[-1]
            zip_url = "https://geo.colorado.edu/apps/geolibrary/datasets/{0}".format(zipname)
            if not os.path.isfile("/data/static/geolibrary/datasets/{0}".format(zipname)):
                shutil.copy(filename,"/data/static/geolibrary/datasets/{0}".format(zipname))
            os.remove(filename)
            return {"folder": destination,"zipdata":False,"zipurl":zip_url}
    zip_ref = zipfile.ZipFile(filename,'r')
    zip_ref.extractall(destination)
    zipname = filename.split('/')[-1]
    shutil.copy(filename,"/data/static/geolibrary/datasets/{0}".format(zipname))
    zip_url = "https://geo.colorado.edu/apps/geolibrary/datasets/{0}".format(zipname)
    os.remove(filename)
    return {"folder": destination,"zipdata":True,"zipurl":zip_url}

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
            else:
                raise Exception("No suitable georeferenced or scanned image file found")
        except:
            type="iiif"
            bounds=None
    return {"file":file,"folder":folder,"bounds":bounds,"type":type,"msg":msg,"zipurl":data["zipurl"]}

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
                doc = xmltodict.parse(stringxml,cdata_key='text',attr_prefix='',dict_constructor=dict)
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
    gblight = json.loads(crosswalkData, strict=False)
    gblight['solr_geom']=data['bounds']
    data['geoblacklightschema']=gblight
    return data

def findSubject(subjects,keyword):
    subs=[]
    try:
        for itm in subjects:
            temp=deep_get(itm,keyword,[])
            if not isinstance(temp, list):
                temp=[temp]
            subs = subs + temp
    except Exception as inst:
        subs.append(str(inst))
    return subs

def assignMetaDataComponents(data,type='fgdc'):
    dataJsonObj=deep_get(data,"xml.fgdc",[])
    if len (dataJsonObj)>0:
        dataJsonObj=deep_get(dataJsonObj[0],"data",{})
    else:
        dataJsonObj={}
    gblight={}
    layername=os.path.splitext(os.path.basename(data['file']))[0]
    gblight['uuid']= "https://geo.colorado.edu/{0}".format(layername)
    gblight['dc_identifier_s'] = "https://geo.colorado.edu/{0}".format(layername)
    gblight['dc_title_s'] = deep_get(dataJsonObj,"metadata.idinfo.citation.citeinfo.title",
                deep_get(dataJsonObj,"metadata.dataIdInfo.idCitation.resTitle",""))
    gblight['dc_description_s'] = deep_get(dataJsonObj,"metadata.idinfo.descript.abstract",
                re.sub('<[^<]+>', "", deep_get(dataJsonObj,"metadata.dataIdInfo.idAbs","")))
    gblight['dc_rights_s'] = "Public"
    gblight['dct_provenance_s'] = "University of Colorado Boulder"
    gblight['dct_references_s'] =  json.dumps({"http://schema.org/downloadUrl":data["zipurl"],"http://www.opengis.net/def/serviceType/ogc/wfs":"https://geo.colorado.edu/geoserver/geocolorado/wfs","http://www.opengis.net/def/serviceType/ogc/wms":"https://geo.colorado.edu/geoserver/geocolorado/wms"})
    gblight['layer_id_s'] = layername
    gblight['layer_slug_s'] = "cub:{0}".format(layername)
    if data["resource_type"]=='coverage':
        gblight['layer_geom_type_s'] = "Polygon"
    else:
        gblight['layer_geom_type_s'] = "Raster"
    gblight['dc_format_s'] =deep_get(dataJsonObj,"metadata.distInfo.distFormat.formatName.#text","")
    gblight['dc_language_s'] = "English"
    gblight['dc_type_s'] = "Dataset"
    creator= deep_get(dataJsonObj,"metadata.idinfo.citation.citeinfo.origin",
                deep_get(dataJsonObj,"metadata.dataIdInfo.idCredit",""))
    gblight['dc_publisher_s'] = creator
    gblight['dc_creator_sm'] = '["{0}"]'.format(creator)
    subjects = deep_get(dataJsonObj,"metadata.idinfo.keywords.theme",
                deep_get(dataJsonObj,"metadata.dataIdInfo.searchKeys",[]))
    subs=findSubject(subjects,"themekey")
    if not subs:
        subs=findSubject(subjects,"keyword")
    gblight['dc_subject_sm'] = json.dumps(subs)
    pubdate=deep_get(dataJsonObj,"metadata.idinfo.citation.citeinfo.pubdate",
            deep_get(dataJsonObj,"metadata.mdDateSt",""))
    gblight['dct_issued_s'] = pubdate
    gblight['dct_temporal_sm'] = '["{0}"]'.format(pubdate)
    place =deep_get(dataJsonObj,"metadata.idinfo.keywords.place.placekey",[])
    if not isinstance(place, list):
        place=[place]
    gblight['dct_spatial_sm'] = json.dumps(place)
    gblight['solr_geom'] = data["bounds"]
    return gblight

@task()
def geoBoundsMetadata(filename,format="shapefile"):
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
    if format=="shapefile":
        with fiona.open(filename, 'r') as c:
            bnd= c.bounds
            bnd=(bnd[0],bnd[2],bnd[3],bnd[1])
            return "ENVELOPE{0}".format(bnd)

    else:
        with rasterio.open(filename,'r') as c:
            bnd= c.bounds
            bnd=(bnd[0],bnd[2],bnd[3],bnd[1])
            return "ENVELOPE{0}".format(bnd)
