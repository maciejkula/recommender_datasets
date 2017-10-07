import array

import numpy as np

from recommender_datasets import _common


BASE_URL = 'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/'

VARIANTS = ('books', 'electronics')


def _to_numpy(data):

    uid_map = {}
    iid_map = {}

    uids = array.array('i')
    iids = array.array('i')
    ratings = array.array('f')
    timestamps = array.array('f')

    for uid, iid, rating, timestamp in data:
        uid = uid_map.setdefault(uid, len(uid_map) + 1)
        iid = iid_map.setdefault(iid, len(iid_map) + 1)

        uids.append(uid)
        iids.append(iid)
        ratings.append(rating)
        timestamps.append(timestamp)

    return (np.array(uids, dtype=np.int32),
            np.array(iids, dtype=np.int32),
            np.array(ratings, dtype=np.float32),
            np.array(timestamps, dtype=np.float32))


def _read_data(variant):

    file_path = _common.get_data(BASE_URL +
                                 'ratings_{}.csv'.format(variant.title()),
                                 'amazon',
                                 'ratings_{}.csv'.format(variant))

    with open(file_path, 'r') as datafile:
        for line in datafile:
            uid, iid, rating, timestamp = line.split(',')

            yield uid, iid, float(rating), float(timestamp)


def read_amazon(variant):

    data = _read_data(variant)

    uids, iids, ratings, timestamps = _to_numpy(data)

    return uids, iids, ratings, timestamps
