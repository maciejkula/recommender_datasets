import os
import zipfile

import h5py
import numpy as np
import pandas as pd


from recommender_datasets import _common


def _read_csv(fname):

    with zipfile.ZipFile(fname, mode='r') as archive:
        with archive.open('data.csv') as datafile:
            data = pd.read_csv(datafile)

    return data


def _read_hdf5(fname):

    with h5py.File(fname, 'r') as data:
        return (data['/user_id'][:],
                data['/item_id'][:],
                data['/rating'][:],
                data['/timestamp'][:])


def verify(path):

    output_dir = os.path.join(_common.get_data_home(), 'output')
    path = os.path.join(output_dir, path)

    csv_data = _read_csv(path + '.zip')
    user_ids, item_ids, ratings, timestamps = _read_hdf5(path + '.hdf5')

    return (
        np.all(csv_data['user_id'] == user_ids) and
        np.all(csv_data['item_id'] == item_ids) and
        np.all(csv_data['rating'] == ratings) and
        np.all(csv_data['timestamp'] == timestamps)
    )
