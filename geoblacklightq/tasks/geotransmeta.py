from celery.task import task
from subprocess import call, STDOUT
from requests import exceptions
from glob import iglob
from .geoservertasks import determineFeatureGeometry, getGeoServerBoundingBox, getLayerDefaultStyle
from functools import reduce
import re
import fnmatch
import jinja2
import json
import ast
import requests
import zipfile
#import fiona
import shutil
import os
import tempfile
#import rasterio
import xmltodict
import nested_lookup
# set tmp direcotry. Assign a specific directory with environmental variable
tmpdir = os.getenv('TMPDIR', tempfile.gettempdir())
zipurl = os.getenv(
    'ZIP_URL', "https://geo.colorado.edu/apps/geolibrary/datasets")
resulturl = os.getenv('RESULT_URL', "https://geo.colorado.edu/apps/geo_tasks/")
arkurl = os.getenv('ARK_URL', "https://test-ark.colorado.edu/ark:/")
geoserver_url = os.getenv('GEOSERVER_CONNECTION',
                          "https://geo.colorado.edu/geoserver")
arktoken = os.getenv('ARK_TOKEN', '')


def findfiles(patterns, where='.'):
    '''Returns list of filenames from `where` path matched by 'which'
       shell pattern. Matching is case-insensitive. return de-duped list just
       in case multiple patterns hit the same filename
    '''
    result = []
    for pattern in patterns:
        # TODO: recursive param with walk() filtering
        rule = re.compile(fnmatch.translate(pattern), re.IGNORECASE)
        result = result + \
            [name for name in os.listdir(where) if rule.match(name)]
    return list(set(result))


def deep_get(_dict, keys, default=None):
    """
    Deep get on python dictionary. Key is in dot notation.
    Returns value if found. Default returned if not found.

    Default can be set to another deep_get function.
    """
    keys = keys.split('.')

    def _reducer(d, key):
        if isinstance(d, dict):
            return d.get(key, default)
        return default
    return reduce(_reducer, keys, _dict)


@task()
def setModsXML(url, filename, basefolder='/data/static/geolibrary/metadata/'):
    req = requests.get(url)
    with open(os.path.join(basefolder, filename), 'w') as f1:
        f1.write(req.text)
    url = zipurl.replace('/datasets', '')
    url = "{0}/metadata/{1}".format(url, filename)
    return url


@task()
def unzip(filename, destination=None, force=True):
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
        destination = os.path.splitext(os.path.basename(filename))[0]
    destination = os.path.join(tmpdir, destination)
    if os.path.exists(destination):
        if force:
            shutil.rmtree(destination)
        else:
            zipname = filename.split('/')[-1]
            zip_url = "{0}/{1}".format(zipurl, zipname)
            if not os.path.isfile("/data/static/geolibrary/datasets/{0}".format(zipname)):
                shutil.copy(
                    filename, "/data/static/geolibrary/datasets/{0}".format(zipname))
            os.remove(filename)
            return {"folder": destination, "zipdata": False, "zipurl": zip_url}
    zip_ref = zipfile.ZipFile(filename, 'r')
    zip_ref.extractall(destination)
    zipname = filename.split('/')[-1]
    shutil.copy(
        filename, "/data/static/geolibrary/datasets/{0}".format(zipname))
    zip_url = "{0}/{1}".format(zipurl, zipname)
    os.remove(filename)
    return {"folder": destination, "zipdata": True, "zipurl": zip_url}


@task()
def determineTypeBounds(data):
    """
    Determine if a shapefile, image, or non georeferenced iiif. Then determines bounds within original projection.
    """
    folder = data["folder"]
    msg = "Initial upload"
    if not data["zipdata"]:
        msg = "Data uploaded previously - Use force to remove and reload."
    type = None
    file = None
    bounds = None
    shapefiles = findfiles(['*.shp'], where=folder)
    if shapefiles:
        type = "shapefile"
        file = os.path.join(folder, shapefiles[0])
        bounds = geoBoundsMetadata(file)
    else:
        try:
            imgfiles = findfiles(
                ['*.jpg', '*.tif', '*.tiff', '*.png'], where=folder)
            if imgfiles:
                file = os.path.join(folder, imgfiles[0])
                bounds = geoBoundsMetadata(file, format="image")
                type = "image"
            else:
                raise Exception(
                    "No suitable georeferenced or scanned image file found")
        except:
            type = "iiif"
            bounds = None
    return {"file": file, "folder": folder, "bounds": bounds, "type": type, "msg": msg, "zipurl": data["zipurl"]}


@task()
def configureGeoData(data, resultDir):
    """
    Finds all xml files within upload dataset. Reads, parses, and converts to python dictionary.
    """
    xmlfiles = findfiles(['*.xml'], where=data["folder"])
    xmlurls = []
    fgdclist = []
    # xmlselect=[]
    for xml in xmlfiles:
        shutil.copy(os.path.join(data['folder'], xml), resultDir)
        xmlurls.append(os.path.join(resulturl, resultDir.split('/')[-1], xml))
        #import xmltodict
        localfilename = os.path.join(data['folder'], xml)
        # xmlselect.append({"file":localfilename,"url":os.path.join(resulturl,resultDir.split('/')[-1],xml)})
        with open(os.path.join(data['folder'], xml)) as fd:
            stringxml = fd.read()
            # if 'FGDC' in stringxml.upper():
            fgdc = {}
            fgdc['url'] = os.path.join(
                resulturl, resultDir.split('/')[-1], xml)
            doc = xmltodict.parse(
                stringxml, cdata_key='text', attr_prefix='', dict_constructor=dict)
            fgdc['data'] = doc
            fgdc['file'] = localfilename
            fgdclist.append(fgdc)
    data['xmlurls'] = xmlurls
    data['xml'] = {"urls": xmlurls, "fgdc": fgdclist, "files": xmlfiles}
    return data


def xml2dict(xmlfile):
    with open(xmlfile) as fd:
        stringxml = fd.read()
        doc = xmltodict.parse(stringxml, cdata_key='text',
                              attr_prefix='', dict_constructor=dict)
    return doc


def convertStringList(obj):
    if type(obj) == str:
        return ast.literal_eval(obj)
    return obj


@task()
def singleCrossWalkGeoBlacklight(filename, layername, geoserver_layername, resource_type, zipurl, mod_url):
    """
    Single XML file crosswalk to GeoBlacklight schema

    """
    doc = {}
    with open(filename) as fd:
        stringxml = fd.read()
        doc = xmltodict.parse(stringxml, cdata_key='text',
                              attr_prefix='', dict_constructor=dict)
    gblight = assignMetaDataComponents(
        doc, layername, geoserver_layername, resource_type)
    gblight['solr_geom'] = getGeoServerBoundingBox(geoserver_layername)
    gblight['dct_references_s'] = json.dumps({"http://schema.org/downloadUrl": zipurl,
                                              "http://www.opengis.net/def/serviceType/ogc/wfs": "{0}/geocolorado/wfs".format(geoserver_url),
                                              "http://www.opengis.net/def/serviceType/ogc/wms": "{0}/geocolorado/wms".format(geoserver_url),
                                              "http://www.loc.gov/mods/v3": mod_url})
    return gblight


@task()
def crossWalkGeoBlacklight(data):
    """
    Workflow Crosswalk to GeoBlacklight schema
    """

    dataJsonObj = deep_get(data, "xml.fgdc", [])
    if len(dataJsonObj) > 0:
        dataJsonObj = deep_get(dataJsonObj[0], "data", {})
    else:
        dataJsonObj = {}
    layername = os.path.splitext(os.path.basename(data['file']))[0]
    geoserver_layername = data['geoserverStoreName']
    gblight = assignMetaDataComponents(
        dataJsonObj, layername, geoserver_layername, data["resource_type"])
    gblight['solr_geom'] = data['bounds']
    # Set dct_references
    mod_url = zipurl.replace('/datasets', '')
    mod_url = "{0}/metadata/{1}".format(mod_url, 'blankMODS.xml')
    mod_url = setModsXML(mod_url, "{0}.xml".format(gblight['layer_slug_s']))
    gblight['dct_references_s'] = json.dumps({"http://schema.org/downloadUrl": data['zipurl'],
                                              "http://www.opengis.net/def/serviceType/ogc/wfs": "{0}/geocolorado/wfs".format(geoserver_url),
                                              "http://www.opengis.net/def/serviceType/ogc/wms": "{0}/geocolorado/wms".format(geoserver_url),
                                              "http://www.loc.gov/mods/v3": mod_url})

    data['geoblacklightschema'] = gblight
    return data


def findSubject(subjects, keyword):
    subs = []
    try:
        for itm in subjects:
            temp = deep_get(itm, keyword, [])
            if not isinstance(temp, list):
                temp = [temp]
            subs = subs + temp
    except Exception as inst:
        subs.append(str(inst))
    return subs


def findTitle(dataJsonObj):
    title = deep_get(dataJsonObj, "mods:mods.mods:titleInfo.mods:title",
                     deep_get(dataJsonObj, "metadata.idinfo.citation.citeinfo.title",
                              deep_get(dataJsonObj, "metadata.dataIdInfo.idCitation.resTitle",
                                       deep_get(dataJsonObj, "gmi:MI_Metadata.gmd:parentIdentifier.gco:CharacterString", ""))))
    if type(title) == dict:
        try:
            title = title['text']
        except:
            pass
    return u'{0}'.format(title)


def findDataIssued(dataJsonObj):
    pubdate = deep_get(dataJsonObj, "mods:mods.mods:originInfo.mods:dateIssued",
                       deep_get(dataJsonObj, "metadata.idinfo.citation.citeinfo.pubdate",
                                deep_get(dataJsonObj, "metadata.mdDateSt", "")))
    if type(pubdate) == dict:
        try:
            pubdate = pubdate['text']
        except:
            pass
    return u'{0}'.format(pubdate)


def findDataCreated(dataJsonObj):
    createDate = deep_get(dataJsonObj, "mods:mods.mods:originInfo.mods:dateCreated",
                          deep_get(dataJsonObj, "metadata.idinfo.citation.citeinfo.pubdate",
                                   deep_get(dataJsonObj, "metadata.mdDateSt", "")))
    if type(createDate) == dict:
        try:
            createDate = createDate['text']
        except:
            pass
    return u'{0}'.format(createDate)


def findCreators(dataJsonObj):
    if 'mods:mods' in dataJsonObj:
        def flatten(l): return [item for sublist in l for item in sublist]
        creators = []
        name_tags = nested_lookup(key='mods:name', document=dataJsonObj)[0]
        for name_tag in name_tags:
            roleterms = flatten(nested_lookup(
                key='mods:roleTerm', document=name_tag))
            for roleterm in roleterms:
                if roleterm['type'] == 'text' and roleterm['text'] == 'creator':
                    creators.append(name_tag['mods:namePart'])
        return u'{0}'.format(creators)
    else:
        creator = deep_get(dataJsonObj, "metadata.idinfo.citation.citeinfo.pubinfo.publish",
                           deep_get(dataJsonObj, "metadata.dataIdInfo.idCitation.citResParty.rpOrgName", []))
        return u'{0}'.format(creators)


def findPublishers(dataJsonObj):
    if 'mods:mods' in dataJsonObj:
        def flatten(l): return [item for sublist in l for item in sublist]
        publishers = []
        name_tags = nested_lookup(
            key='mods:publisher', document=dataJsonObj)[0]
        publishers.append(name_tags)
        return u'{0}'.format(publishers)
    else:
        publishers = deep_get(dataJsonObj, "metadata.idinfo.citation.citeinfo.pubinfo.publish",
                              deep_get(dataJsonObj, "metadata.dataIdInfo.idCredit", ""))
        return u'{0}'.format(publishers)


def setARKSlug(gblight, ark, ark_url=arkurl, naan='47540'):

    # double check that arkurl ends with /
    ark_url = ark_url.strip()
    if not ark_url.endswith('/'):
        ark_url = ark_url + '/'

    if ark:
        gblight['uuid'] = "{0}{1}".format(arkurl, ark)
        gblight['dc_identifier_s'] = "{0}{1}".format(arkurl, ark)
        gblight['layer_slug_s'] = ark.replace('/', '-')
    else:
        headers = {"Content-Type": "application/json",
                   "Authorization": "Token {0}".format(arktoken)}
        resolve_url = resulturl.replace('apps/geo_tasks/', 'catalog/')
        data = {"resolve_url": resolve_url, "metadata": {"mods": {"titleInfo": [{"title": gblight['dc_title_s']}],
                                                                  "typeOfResource": "", "identifier": "", "accessCondition": ""}}}
        req = requests.post("{0}?format=json".format(
            arkurl), data=json.dumps(data), headers=headers)
        data = req.json()["results"][0]
        url = data["ark-detail"]
        if "ark-detail" in data:
            del data["ark-detail"]
        ark = data["ark"]
        data["resolve_url"]
        gblight['uuid'] = "{0}{1}".format(arkurl, ark)
        gblight['dc_identifier_s'] = "{0}{1}".format(arkurl, ark)
        gblight['layer_slug_s'] = ark.replace('/', '-')
        data["resolve_url"] = "{0}{1}".format(
            resolve_url, gblight['layer_slug_s'])
        data["metadata"]["mods"]["identifier"] = "{0}{1}".format(arkurl, ark)
        req = requests.put(url, data=json.dumps(data), headers=headers)
        if req.status_code >= 400:
            raise Exception(req.text)
    return gblight


def assignMetaDataComponents(dataJsonObj, layername, geoserver_layername, resource_type, ark=None):
    """
    Geoblacklight crosswalk for metadata
    """
    gblight = {}
    gblight['dc_title_s'] = findTitle(dataJsonObj)
    gblight = setARKSlug(gblight, ark)
    # gblight['uuid'] = "https://ark.colorado.edu/ark:47540/"
    # gblight['dc_identifier_s'] = "https://ark.colorado.edu/ark:47540/"
    # gblight['layer_slug_s'] = "47540-"
    gblight['dc_description_s'] = deep_get(dataJsonObj, "mods:mods.mods:abstract", deep_get(dataJsonObj, "metadata.idinfo.descript.abstract",
                                                                                            re.sub('<[^<]+>', "", deep_get(dataJsonObj, "metadata.dataIdInfo.idAbs",
                                                                                                                           deep_get(dataJsonObj, "gmi:MI_Metadata.gmd:identificationInfo.gmd:MD_DataIdentification.gmd:abstract.gco:CharacterString", "")))))
    gblight['dc_rights_s'] = "Public"
    cub_rights_metadata_s = "The organization that has made the Item available believes that the Item is in the Public Domain under the laws of the United States."
    gblight['cub_rights_metadata_s'] = cub_rights_metadata_s
    gblight['dct_provenance_s'] = "University of Colorado Boulder"
    gblight['dct_references_s'] = "DO NOT SET"
    gblight['layer_id_s'] = geoserver_layername

    if resource_type == 'coverage':
        gblight['layer_geom_type_s'] = "Raster"
        gblight['dc_format_s'] = "GeoTiff"
    else:
        gblight['layer_geom_type_s'] = determineFeatureGeometry(
            geoserver_layername)
        gblight['dc_format_s'] = "Shapefile"
    gblight['dc_language_s'] = "English"
    gblight['dc_type_s'] = "Dataset"
    # creator = deep_get(dataJsonObj, "metadata.idinfo.citation.citeinfo.origin",
    #                    deep_get(dataJsonObj, "metadata.dataIdInfo.idCredit", ""))
    gblight['dc_publisher_s'] = findPublishers(dataJsonObj)  # creator
    # cleanBlanksFromList([u"{0}".format(creator)])
    gblight['dc_creator_sm'] = findCreator(dataJsonObj)
    subjects = deep_get(dataJsonObj, "mods:mods.mods:subject.mods:topic", deep_get(dataJsonObj, "metadata.idinfo.keywords.theme",
                                                                                   deep_get(dataJsonObj, "metadata.dataIdInfo.searchKeys", [])))
    subs = findSubject(subjects, "themekey")
    if not subs:
        subs = findSubject(subjects, "keyword")
    gblight['dc_subject_sm'] = cleanBlanksFromList(subs)
    pubdate = findDataIssued(dataJsonObj)
    gblight['dct_issued_s'] = pubdate
    gblight['dct_created_s'] = findDataCreated(dataJsonObj)
    # Remove pubdate from dct_temporal_sm leaving clean for possible update to another field
    gblight['dct_temporal_sm'] = cleanBlanksFromList([])
    place = deep_get(dataJsonObj, "mods:mods.mods:subject.mods:geographic", deep_get(
        dataJsonObj, "metadata.idinfo.keywords.place.placekey", []))
    if not isinstance(place, list):
        place = [place]
    gblight['dct_spatial_sm'] = cleanBlanksFromList(place)
    gblight['status'] = "indexed"
    gblight['style'] = getLayerDefaultStyle(geoserver_layername)
    gblight['mod_import_url'] = ""
    return gblight


def cleanBlanksFromList(datalist):
    return [x for x in datalist if x]


@task()
def geoBoundsMetadata(filename, format="shapefile"):
    """
    **** Removing getting Bounds - Done later in work workflow with GeoServer. 


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
    # default with entire colorado
    return "ENVELOPE(-109.27619724342406,-101.91572412775933,41.036591647196474,36.93298568144766)"
    # if format == "shapefile":
    #     with fiona.open(filename, 'r') as c:
    #         bnd = c.bounds
    #         bnd = (bnd[0], bnd[2], bnd[3], bnd[1])
    #         return "ENVELOPE{0}".format(bnd)

    # else:
    #     with rasterio.open(filename, 'r') as c:
    #         bnd = c.bounds
    #         bnd = (bnd[0], bnd[2], bnd[3], bnd[1])
    #         return "ENVELOPE{0}".format(bnd)
