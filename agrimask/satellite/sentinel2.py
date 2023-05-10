import json
from shapely.geometry import Polygon
from collections import defaultdict
from datetime import datetime as dt


from agrimask.utils.helper import *
from agrimask.utils.raster import *

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from osgeo import gdal

s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))


class Sentinel2:
    def __init__(self):
        gdal.SetConfigOption('AWS_NO_SIGN_REQUEST', 'YES')
        self.source_bucket = 'sentinel-cogs'
        self.source_s3_folder = 'sentinel-s2-l2a-cogs'
        self.bands = ['B04', 'B08']
        self.crs = 'EPSG:4326'
        self.sat_name = 'sentinel2'
        self.sat_id = 'S2'
        self.cloud = 10

    def get_product_ids(self, start_date, end_date, cloud_threshold, data_days_interval,
                        shape_file=None, bbox=None):
        poly = None
        percent_area_present = None
        tiles = shape_to_tiles(aoi_shp=shape_file, bbox=bbox)
        poly = shape_to_polygon(shp_file=shape_file, bbox=bbox)
        print(f'Tiles found for the given AOI : {tiles}')
        final_dict = {'single_tile': {}, 'merge_tile': {}}
        for tile in tiles:
            utm_zone, lat_band, grid_square = str(tile)[:2], str(tile)[2], str(tile)[3:5]
            for _date in datetime_iterator(start_date, end_date):
                _year = int(_date.year)
                _month = int(_date.month)
                _PREFIX = os.path.join(self.source_s3_folder, str(utm_zone), str(lat_band), str(grid_square),
                                       str(_year), str(_month), '')
                _PREFIX = _PREFIX.replace('\\', '/')
                response = s3_client.list_objects(Bucket=self.source_bucket, Prefix=_PREFIX)
                for content in response.get('Contents', []):
                    key = content['Key']
                    if key.endswith('L2A.json'):
                        product_id = str(key).split('/')[-2]
                        pid_date = '-'.join([str(_year), str(_month).zfill(2), str(product_id[16:18]).zfill(2)])
                        if not validate_date(pid_date, start_date, end_date):
                            continue
                        result = s3.Object(self.source_bucket, key)
                        data = json.load(result.get()['Body'])
                        tile_coord = data['geometry']['coordinates']
                        tile_cloud = data['properties']['eo:cloud_cover']
                        tile_timestamp = data['properties']['datetime']
                        tile_poly = Polygon(tile_coord[0])
                        if not percent_area_present:
                            percent_area = poly.intersection(tile_poly).area / poly.area
                        if percent_area <= 0.05:
                            continue
                        elif percent_area >= 0.99:
                            if not final_dict['single_tile'].get(str(pid_date)):
                                final_dict['single_tile'][str(pid_date)] = {}
                            final_dict['single_tile'][str(pid_date)][str('pids')] = [product_id]
                            final_dict['single_tile'][str(pid_date)][str('cloud_percentages')] = tile_cloud
                            final_dict['single_tile'][str(pid_date)][str('tile_timestamp')] = tile_timestamp
                        else:
                            if not final_dict['merge_tile'].get(str(pid_date)):
                                final_dict['merge_tile'][str(pid_date)] = defaultdict(list)
                            final_dict['merge_tile'][str(pid_date)][str('pids')].append(product_id)
                            final_dict['merge_tile'][str(pid_date)][str('tile_ids')].append(tile)
                            final_dict['merge_tile'][str(pid_date)][str('cloud_percentages')].append(tile_cloud)
                            final_dict['merge_tile'][str(pid_date)][str('percent_areas')].append(percent_area)
                            final_dict['merge_tile'][str(pid_date)][str('tile_timestamp')].append(tile_timestamp)
                        print(f'Date: {pid_date}   Tile:{tile}   Cloud: {tile_cloud}   Area AOI/Tile: {percent_area}')

        diff = date_dif(start_date, end_date)
        diff = diff // data_days_interval
        diff = diff // 2  # Data interval Buffer for user's input
        all_pids = {}
        if len(final_dict['single_tile']) >= diff:
            data = final_dict['single_tile']
            _dates = list(sorted(data.keys()))
            for date in _dates:
                # print(f'Single Tile Metadata: {data[date]}')
                tile_cloud = data[date]['cloud_percentages']
                if tile_cloud > cloud_threshold:
                    continue
                else:
                    # all_pids[str(date)] = data[date]['pids']
                    all_pids[str(date)] = {}
                    all_pids[str(date)]['pids'] = data[date]['pids']
                    all_pids[str(date)]['tile_timestamp'] = data[date]['tile_timestamp']
        else:
            data = final_dict['merge_tile']
            prev_date = None
            skip_one = False
            _dates = list(sorted(data.keys()))
            for date in _dates:
                if not prev_date:
                    prev_date = date
                    continue
                if skip_one:
                    prev_date = date
                    skip_one = False
                    continue
                date_diff = dt.strptime(str(date), "%Y-%m-%d") - dt.strptime(str(prev_date), "%Y-%m-%d")
                days_diff = date_diff.days
                weighted_sum = 0
                sum_area_percents = 0
                for i in range(len(data[prev_date]['tile_ids'])):
                    weighted_sum += data[prev_date]['cloud_percentages'][i] * data[prev_date]['percent_areas'][i]
                    sum_area_percents += data[prev_date]['percent_areas'][i]

                for i in range(len(data[date]['tile_ids'])):
                    weighted_sum += data[date]['cloud_percentages'][i] * data[date]['percent_areas'][i]
                    sum_area_percents += data[date]['percent_areas'][i]

                avg_cloud_percent = weighted_sum / sum_area_percents
                if avg_cloud_percent > cloud_threshold:
                    prev_date = date
                    continue
                if days_diff >= 5:
                    all_pids[str(prev_date)] = {}
                    all_pids[str(prev_date)]['pids'] = data[prev_date]['pids']
                    all_pids[str(prev_date)]['tile_timestamp'] = data[prev_date]['tile_timestamp']
                elif days_diff < 5:
                    all_pids[str(date)] = {}
                    all_pids[str(date)]['tile_timestamp'] = data[date]['tile_timestamp']
                    all_pids[str(date)] = data[prev_date]['pids'] + data[date]['pids']
                    skip_one = True
                prev_date = date
        final_pids = data_difference_days(all_pids, data_days_interval)
        return final_pids
