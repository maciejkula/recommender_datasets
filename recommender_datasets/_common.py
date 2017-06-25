import os

import requests


def get_data_home():

    return os.path.join(os.path.expanduser('~'),
                        '.recommender_datasets')


def create_data_dir(path):

    if not os.path.isdir(path):
        os.makedirs(path)


def download(url, dest_path):

    req = requests.get(url, stream=True)
    req.raise_for_status()

    with open(dest_path, 'wb') as fd:
        for chunk in req.iter_content(chunk_size=2**20):
            fd.write(chunk)


def get_data(url, dest_subdir, dest_filename, download_if_missing=True):

    data_home = get_data_home()
    data_dir = os.path.join(os.path.abspath(data_home), dest_subdir)

    create_data_dir(data_dir)

    dest_path = os.path.join(data_dir, dest_filename)

    if not os.path.isfile(dest_path):
        if download_if_missing:
            download(url, dest_path)
        else:
            raise IOError('Dataset missing.')

    return dest_path
