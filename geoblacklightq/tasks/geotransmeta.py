from celery.task import task
from subprocess import call,STDOUT
from requests import exceptions
import requests, zipfile, fiona
import os, tempfile, rasterio

#set tmp direcotry. Assign a specific directory with environmental variable
tmpdir = os.getenv('TMPDIR',tempfile.gettempdir())
@task()
def unzip(filename,destination=None):
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
    zip_ref = zipfile.ZipFile(filename,'r')
    zip_ref.extractall(destination)
    return destination

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
            bnd=(bnd[0],bnd[2],bnd[3],bnd[1])
            return "ENVELOPE{0}".format(bnd)
