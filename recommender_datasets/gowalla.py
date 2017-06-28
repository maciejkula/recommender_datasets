import datetime
import gzip


from recommender_datasets import _common

URL = 'https://snap.stanford.edu/data/loc-gowalla_totalCheckins.txt.gz'


def _read_data(path):

    with gzip.GzipFile(path) as datafile:
        for line in datafile:
            (user_id, time, lat,
             lon, locaton_id) = line.decode('utf-8').split('\t')

            yield (int(user_id),
                   int(datetime.datetime.strptime(time,
                                                  "%Y-%m-%dT%H:%M:%SZ")
                       .timestamp()),
                   float(lat),
                   float(lon),
                   int(locaton_id))


def read_gowalla():

    zip_path = _common.get_data(URL,
                                'gowalla',
                                'gowalla.txt.gz')

    for line in _read_data(zip_path):
        yield line
