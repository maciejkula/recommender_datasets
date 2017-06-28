import os
import zipfile

import h5py
import numpy as np
import pandas as pd


from recommender_datasets import _common


def _read_csv(fname, columns=('user_id', 'item_id', 'rating', 'timestamp')):

    with zipfile.ZipFile(fname, mode='r') as archive:
        with archive.open('data.csv') as datafile:
            df = pd.read_csv(datafile)
            data = df[list(columns)]

    return data


def _read_hdf5(fname, columns=('user_id', 'item_id', 'rating', 'timestamp')):

    with h5py.File(fname, 'r') as data:
        return {column: data['/{}'.format(column)][:]
                for column in columns}


def verify(path, columns=('user_id', 'item_id', 'rating', 'timestamp')):

    output_dir = os.path.join(_common.get_data_home(), 'output')
    path = os.path.join(output_dir, path)

    csv_data = _read_csv(path + '.zip', columns)
    hdf5data = _read_hdf5(path + '.hdf5', columns)

    return all(
        np.all(csv_data[column] == hdf5data[column])
        for column in columns
    )
