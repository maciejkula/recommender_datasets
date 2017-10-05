import datetime
import itertools
import os
import subprocess
import time

from recommender_datasets import _common


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
        for line in itertools.islice(datafile, 0, 100):
            uid, timestamp, iid = line.split(',')[:3]

            timestamp = time.mktime(
                datetime.datetime.strptime(timestamp,
                                           '%Y-%m-%dT%H:%M:%S.%fZ')
                .timetuple())

            yield int(uid), timestamp, int(iid)


def read_yoochoose(variant):

    


if __name__ == '__main__':
    data = list(read_yoochoose('buys'))
