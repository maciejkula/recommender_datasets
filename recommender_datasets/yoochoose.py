import array
import datetime
import os
import subprocess
import time

import numpy as np

from recommender_datasets import _common


def _to_numpy(data):

    uid_map = {}
    iid_map = {}

    uids = array.array('i')
    iids = array.array('i')
    timestamps = array.array('f')

    for uid, iid, timestamp in data:
        uid = uid_map.setdefault(uid, len(uid_map) + 1)
        iid = iid_map.setdefault(iid, len(iid_map) + 1)

        uids.append(uid)
        iids.append(iid)
        timestamps.append(timestamp)

    return (np.array(uids, dtype=np.int32),
            np.array(iids, dtype=np.int32),
            np.array(timestamps, dtype=np.float32))


def _read_data(variant):

    zip_path = _common.get_data('https://s3-eu-west-1.amazonaws.com/'
                                'yc-rdata/yoochoose-data.7z',
                                'yoochoose',
                                'yoochoose.7z')

    dest_dir = os.path.dirname(zip_path)

    for suffix in ('buys', 'clicks'):
        if not os.path.exists(os.path.join(
                dest_dir, 'yoochoose-{}.dat'.format(suffix))):
            subprocess.check_call(['7z',
                                   '-o{}'.format(dest_dir),
                                   'x',
                                   zip_path])

    fname = os.path.join(dest_dir, 'yoochoose-{}.dat'.format(variant))
    with open(fname, 'r') as datafile:
        for line in datafile:
            uid, timestamp, iid = line.split(',')[:3]

            timestamp = time.mktime(
                datetime.datetime.strptime(timestamp,
                                           '%Y-%m-%dT%H:%M:%S.%fZ')
                .timetuple())

            yield int(uid), int(iid), timestamp


def read_yoochoose(variant):

    data = _read_data(variant)

    uids, iids, timestamps = _to_numpy(data)

    return uids, iids, timestamps
