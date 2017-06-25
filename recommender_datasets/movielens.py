import itertools
import os
import zipfile

from recommender_datasets import _common


URL_PREFIX = 'http://files.grouplens.org/datasets/movielens/'
URL_100K = 'ml-100k.zip'
URL_1M = 'ml-1m.zip'
URL_10M = 'ml-10m.zip'
URL_20M = 'ml-20m.zip'


def _read_data(path, archive_path):

    with zipfile.ZipFile(path) as archive:
        with archive.open(archive_path) as datafile:
            for line in datafile:
                yield line.decode('utf-8')


def _parse_line(line, separator='::'):

    uid, iid, rating, timestamp = line.split(separator)

    return (int(uid), int(iid), float(rating), int(timestamp))


def read_movielens_100K():

    zip_path = _common.get_data(URL_PREFIX + URL_100K,
                                'movielens',
                                'movielens_100k.zip')

    archive_path = os.path.join('ml-100k', 'u.data')

    for line in _read_data(zip_path, archive_path):
        yield _parse_line(line, separator='\t')


def read_movielens_1M():

    zip_path = _common.get_data(URL_PREFIX + URL_1M,
                                'movielens',
                                'movielens_1M.zip')

    archive_path = os.path.join('ml-1m', 'ratings.dat')

    for line in _read_data(zip_path, archive_path):
        yield _parse_line(line, separator='::')


def read_movielens_10M():

    zip_path = _common.get_data(URL_PREFIX + URL_10M,
                                'movielens',
                                'movielens_10M.zip')

    archive_path = os.path.join('ml-10M100K', 'ratings.dat')

    for line in _read_data(zip_path, archive_path):
        yield _parse_line(line, separator='::')


def read_movielens_20M():

    zip_path = _common.get_data(URL_PREFIX + URL_20M,
                                'movielens',
                                'movielens_20M.zip')

    archive_path = os.path.join('ml-20m', 'ratings.csv')

    for line in itertools.islice(_read_data(zip_path, archive_path), 1, None):
        yield _parse_line(line, separator=',')
