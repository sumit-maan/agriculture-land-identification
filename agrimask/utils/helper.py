import os
import warnings
from osgeo import ogr
from subprocess import call
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import shapely.wkt
from shapely.geometry import box
import pkg_resources

warnings.filterwarnings('ignore')
wd = str(os.getcwd())

stream = pkg_resources.resource_stream('agrimask', 'satellite/satellite_tiles/s2_tile.shp')
s2_tile_shp = str(stream).split("'")[1]


def get_tile_shape(aoi_shp, bbox):
    aoi_tile_shp = os.path.join(wd, 'sentinel2_bbox_tile.shp')
    try:
        os.remove(aoi_tile_shp)
    except OSError:
        pass
    if aoi_shp:
        call(str('ogr2ogr -clipsrc ' + aoi_shp + ' ' + aoi_tile_shp + ' ' + s2_tile_shp), shell=True)
    else:
        min_x, min_y, max_x, max_y = bbox[0], bbox[1], bbox[2], bbox[3]
        call(
            str('ogr2ogr -f "ESRI Shapefile" ' + aoi_tile_shp + ' ' + s2_tile_shp
                + ' -clipsrc ' + str(min_x) + ' ' + str(min_y) + ' ' + str(max_x) + ' ' + str(max_y)), shell=True)
    return aoi_tile_shp


def shape_to_tiles(aoi_shp=None, bbox=None):
    aoi_tile_shp = get_tile_shape(aoi_shp, bbox)
    driver = ogr.GetDriverByName('ESRI Shapefile')
    ds = driver.Open(aoi_tile_shp)
    layer = ds.GetLayer(0)
    tile_list = []
    for feat in layer:
        tile_list.append(feat.GetField('tile'))
    f_name, ext = os.path.splitext(aoi_tile_shp)
    for item in ['.dbf', '.prj', '.shp', '.shx']:
        try:
            os.remove(str(f_name) + str(item))
        except:
            pass
    del ds
    del layer
    return tile_list


def validate_date(original_date, start_date, end_date):
    final_date = datetime.strptime(str(original_date), '%Y-%m-%d')
    s_date = datetime.strptime(str(start_date), '%Y-%m-%d')
    e_date = datetime.strptime(str(end_date), '%Y-%m-%d')
    return s_date <= final_date <= e_date


def shape_to_polygon(shp_file=None, bbox=None):
    if shp_file:
        ds = ogr.Open(shp_file)
        layer = ds.GetLayer(0)
        for feat in layer:
            wkt_poly = feat.geometry().ExportToWkt()
        shape_poly = shapely.wkt.loads(wkt_poly)
        return shape_poly
    poly = box(*bbox, ccw=True)
    return poly


def date_dif(date1, date2):
    d0 = datetime.strptime(str(date1), '%Y-%m-%d').date()
    d1 = datetime.strptime(str(date2), '%Y-%m-%d').date()
    delta = abs(d1 - d0).days
    return delta


def datetime_iterator(start_date=None, end_date=None):
    if not end_date:
        end_date = datetime.today().date()
    if not start_date:
        start_date = end_date - timedelta(30)
    start_date = datetime.strptime(str(start_date), '%Y-%m-%d').date()
    start_date = start_date.replace(day=1)
    end_date = datetime.strptime(str(end_date), '%Y-%m-%d').date()
    while start_date <= end_date:
        yield start_date
        start_date = start_date + relativedelta(months=1)


def data_difference_days(product_dictionary, days_interval):
    from datetime import datetime as dt
    prev_date = None
    _dates = list(sorted(product_dictionary.keys()))
    n = len(_dates)
    for i in range(n - 1):
        if not prev_date:
            prev_date = _dates[i]
        date = _dates[i + 1]
        date_diff = dt.strptime(date, "%Y-%m-%d") - dt.strptime(prev_date, "%Y-%m-%d")
        days_diff = date_diff.days
        if days_diff < days_interval:
            product_dictionary.pop(date)
        else:
            prev_date = None
    return product_dictionary
