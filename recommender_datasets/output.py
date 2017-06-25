import array
import os
import zipfile

import h5py
import numpy as np

from recommender_datasets import _common


SEPARATOR = ','


def _to_numpy(data):

    uids = array.array('i')
    iids = array.array('i')
    ratings = array.array('f')
    timestamps = array.array('i')

    for uid, iid, rating, timestamp in data:
        uids.append(uid)
        iids.append(iid)
        ratings.append(rating)
        timestamps.append(timestamp)

    return (np.array(uids, dtype=np.int32),
            np.array(iids, dtype=np.int32),
            np.array(ratings, dtype=np.float32),
            np.array(timestamps, dtype=np.int32))


def _serialize(row):

    row = [str(x) for x in row]

    return (SEPARATOR.join(row) + '\n').encode('utf-8')


def write_csv_data(filename, data,
                   header=('uid', 'iid', 'rating', 'timestamp')):

    output_dir = os.path.join(_common.get_data_home(), 'output')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    path = os.path.join(output_dir, filename)

    with zipfile.ZipFile(path, mode='w') as archive:
        with archive.open('data.csv', mode='w') as output_file:
            output_file.write(_serialize(header))

            for row in data:
                output_file.write(_serialize(row))


def write_hdf5_data(filename, data,
                    header=('uid', 'iid', 'rating', 'timestamp')):

    uids, iids, ratings, timestamps = _to_numpy(data)

    output_dir = os.path.join(_common.get_data_home(), 'output')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    path = os.path.join(output_dir, filename + '.hdf5')

    with h5py.File(path, "w") as archive:
        archive.create_dataset('user_ids', data=uids, compression='gzip')
        archive.create_dataset('item_ids', data=iids, compression='gzip')
        archive.create_dataset('ratings', data=ratings, compression='gzip')
        archive.create_dataset('timestamps', data=timestamps,
                               compression='gzip')
