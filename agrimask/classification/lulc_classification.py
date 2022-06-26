import os.path

import faiss
import numpy as np
from osgeo import gdal
from unsupcc.utils.raster import write_raster


class Classifier:
    def __init__(self):
        self.n_init = 10
        self.max_iter = 300

    def crop_cluster(self, layer_stack_path, date_band_ids, number_of_cluster=None, number_of_crop=None):
        layer_stack_path = layer_stack_path.replace('//', '/')
        out_dir = '/'.join(os.path.splitext(layer_stack_path)[0].split('/')[:-2])
        if not number_of_cluster and number_of_crop:
            number_of_cluster = int(number_of_crop) + 3
        samples, original_layer_shape = self.multi_band_raster_to_array(layer_stack_path)
        sub_samples = [samples[:, i - 1] for i in date_band_ids]
        train_samples = np.column_stack(sub_samples)

        clf = faiss.Kmeans(d=train_samples.shape[1], k=number_of_cluster,
                           niter=self.max_iter,
                           nredo=self.n_init)
        clf.train(train_samples)

        labels = clf.index.search(train_samples, 1)[1]
        label_arr = labels.reshape(original_layer_shape)

        del labels
        outfile = os.path.join(out_dir, 'crop_cluster.tif')
        outfile = outfile.replace('\\', '/')
        write_raster(layer_stack_path, label_arr, outfile, gdal.GDT_UInt16)
        del label_arr
        del sub_samples
        return outfile

    @staticmethod
    def multi_band_raster_to_array(raster_stack_file_path):
        ds = gdal.Open(raster_stack_file_path)
        layers = ds.RasterCount
        lst = []
        for i in range(layers):
            band = ds.GetRasterBand(i + 1)
            arr = band.ReadAsArray()
            arr[np.isnan(arr)] = 0
            lst.append(arr.flatten())
        original_shape = arr.shape
        samples = np.column_stack(lst)
        return samples, original_shape
