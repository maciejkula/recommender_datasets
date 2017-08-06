import array
import os
import zipfile

import h5py
import numpy as np

from recommender_datasets import _common


SEPARATOR = ','


def _array_from_dtype(dtype):

    if dtype == np.float32:
        return array.array('f')
    else:
        return array.array('i')


def _to_numpy(data, dtypes):

    arrays = tuple(_array_from_dtype(x)
                   for x in dtypes)

    for row in data:
        for (arr, elem) in zip(arrays, row):
            arr.append(elem)

    return tuple(np.array(arr, dtype=dtype)
                 for (arr, dtype) in
                 zip(arrays, dtypes))


def _serialize(row):

    row = [str(x) for x in row]

    return (SEPARATOR.join(row) + '\n').encode('utf-8')


def write_csv_data(filename, data,
                   header=('user_id', 'item_id', 'rating', 'timestamp')):

    output_dir = os.path.join(_common.get_data_home(), 'output')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    path = os.path.join(output_dir, filename) + '.zip'

    with zipfile.ZipFile(path, mode='w') as archive:
        with archive.open('data.csv', mode='w') as output_file:
            output_file.write(_serialize(header))

            for row in data:
                output_file.write(_serialize(row))


def write_hdf5_data(filename, data,
                    header=('user_id',
                            'item_id',
                            'rating',
                            'timestamp'),
                    dtype=(np.int32,
                           np.int32,
                           np.float32,
                           np.int32)):

    if not all(isinstance(x, np.ndarray) for x in data):
        arrays = _to_numpy(data, dtype)
    else:
        arrays = data

    output_dir = os.path.join(_common.get_data_home(), 'output')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    path = os.path.join(output_dir, filename + '.hdf5')

    with h5py.File(path, "w") as archive:
        for (arr, name) in zip(arrays, header):
            archive.create_dataset(name, data=arr, compression='gzip')
