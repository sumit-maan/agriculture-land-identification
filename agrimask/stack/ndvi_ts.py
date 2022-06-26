import os 
import errno
from collections import defaultdict
from osgeo import gdal

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from agrimask.satellite import sentinel2
from agrimask.utils.helper import *
from agrimask.utils.raster import *

wd = str(os.getcwd())

s2 = sentinel2.Sentinel2()

s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
s3_client = boto3.client('s3')


class indice:
    def __init__(self):
        gdal.SetConfigOption('AWS_NO_SIGN_REQUEST', 'YES')
        self.source_s3_bucket = 'sentinel-cogs'
        self.source_s3_dir = 'sentinel-s2-l2a-cogs'
        self.s2_tile_shp = 'satellite_tiles/s2_tile.shp'
        self.bands = ['B04' 'B08']
        self.indices = 'ndvi'
        self.crs = 'EPSG:4326'
        self.sat_name = 'sentinel2'
        self.sat_id = 'S2'
        self.home_dir = os.path.join(wd, 'cc',
                                     str(datetime.today().strftime('%Y-%m-%d-%H-%M-%S')).split('.')[0].replace(' ', ''))
        self.home_dir = self.home_dir.replace('\\', '/')

    def pid_to_path(self, prod_id, band):
        lst = prod_id.split('_')
        tile = lst[1]
        utm_zone, lat_band, grid_sq = str(tile)[:2], str(tile)[2], str(tile)[3:5]
        _year, _month, _day = lst[2][0:4], lst[2][4:6], lst[2][6:8]
        vsi_ext = '/vsis3/'
        band_tif = os.path.join(vsi_ext, self.source_s3_bucket, self.source_s3_dir, str(utm_zone), str(lat_band),
                                str(grid_sq),str(_year), str(int(_month)), str(prod_id), str(band) + '.tif')
        band_tif = band_tif.replace('\\', '/')
        return band_tif

    def layer_stack(self, start_date, end_date, cloud_threshold, data_days_interval,
                    shp_file=None, bbox=None):
        if shp_file:
            shp_file = shp_file.replace('\\', '/')
        pids = s2.get_product_ids(start_date, end_date, cloud_threshold, data_days_interval, shp_file, bbox)
        ndvi_list = []
        _dates = sorted(pids.keys())
        print(f'Dates found (in ascending order) :{_dates}')
        count = 0
        n = len(_dates)
        for _key in _dates:
            pid_list = pids[_key]
            bands_dict = defaultdict(list)
            for item in pid_list:
                for band in self.bands:
                    band_tif = self.pid_to_path(prod_id=item, band=band)
                    bands_dict[band].append(band_tif)

            merged_file_dir = os.path.join(self.home_dir, str(_key), 'merged')
            try:
                os.makedirs(merged_file_dir)
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
                pass
            merged_file_dir = merged_file_dir.replace('\\', '/')
            merged_b8 = merge_clip_raster(bands_dict['B08'], str(merged_file_dir) + '/B08.tif',
                                          shp_file, bbox)
            merged_b4 = merge_clip_raster(bands_dict['B04'], str(merged_file_dir) + '/B04.tif',
                                          shp_file, bbox)
            b8 = raster_to_array(merged_b8)
            b4 = raster_to_array(merged_b4)
            del merged_b4
            ndvi_arr = self.ndvi(b4, b8)
            del b8
            del b4
            ndvi_file_path = os.path.join(self.home_dir, str(_key), 'ndvi.tif')
            ndvi_file_path = ndvi_file_path.replace('\\', '/')
            write_raster(merged_b8, ndvi_arr, ndvi_file_path, gdal.GDT_Float32)
            del merged_b8
            del ndvi_arr
            ndvi_list.append(ndvi_file_path)
            count += 1
            print(f'Progress :   {100 * count // n}% completed')
        stack_path = os.path.join(self.home_dir, 'stack')
        stack_path = stack_path.replace('\\', '/')
        try:
            os.makedirs(stack_path)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
            pass
        file_path = os.path.join(stack_path, 'ndvi.tif')
        raster_mosaic(ndvi_list, file_path)
        return file_path

    @staticmethod
    def ndvi(red, nir):
        arr = (nir - red) / (nir + red)
        arr[arr > 1] = 0
        arr[arr < -1] = 0
        return arr

    @staticmethod
    def fcc(green, red, nir, output_vrt):
        return raster_mosaic([green, red, nir], output_vrt)
