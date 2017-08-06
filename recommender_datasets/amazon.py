import array
import datetime
import gzip
from itertools import islice
import time

import numpy as np

from recommender_datasets import _common


def _read_blocks(path):

    with gzip.open(path, 'r') as source_file:

        block = []

        for line in islice(source_file, 3, None):
            line = line.decode('utf-8')
            if line.startswith('Id:'):
                if block:
                    yield block
                block = [line.replace('\r\n', '')]
                continue
            else:
                block.append(line.replace('\r\n', ''))


def _parse_category(cat_string):

    start = cat_string.find('[')
    stop = cat_string.find(']')

    return int(cat_string[start + 1:stop])


def _parse_categories(lines):

    num_categories = int(lines[0].split(':')[-1].strip())

    categories = []

    for category_line in lines[1:1 + num_categories]:

        cat_strings = category_line.strip().split('|')
        cat_ids = [_parse_category(x) for x in cat_strings if x]

        categories += cat_ids

    return categories


def _parse_reviews(lines):

    customer_ids = []
    ratings = []
    dates = []

    for line in lines[1:]:

        if not line:
            continue

        date_stop = line.find('cutomer')
        year, month, day = line[:date_stop].split('-')
        dates.append(datetime.date(year=int(year),
                                   month=int(month),
                                   day=int(day)))

        rating_start = line.find('rating')
        rating_stop = line.find('votes')
        rating = int(line[rating_start:rating_stop].split(':')[1])
        ratings.append(rating)

        customer_ids.append(line[date_stop:rating_start].split(':')[1].strip())

    return customer_ids, ratings, dates


def _parse_block(lines):

    ITEM_ID_LINE = 0

    item_id = int(lines[ITEM_ID_LINE].split(':')[1]) + 1
    categories = []
    user_ids = []
    ratings = []
    dates = []

    for line_num, line in enumerate(lines):
        if 'categories' in line:
            categories = _parse_categories(lines[line_num:])

        if 'reviews:' in line:
            (user_ids,
             ratings,
             dates) = _parse_reviews(lines[line_num:])

    return (item_id,
            categories,
            user_ids,
            ratings,
            dates)


def read_amazon_co_purchasing():

    path = _common.get_data('https://snap.stanford.edu/data/bigdata/amazon/amazon-meta.txt.gz',
                            'amazon',
                            'amazon_co_purchasing.gz')

    user_dict = {}
    feature_dict = {}

    interaction_user_ids = array.array('i')
    interaction_item_ids = array.array('i')
    interaction_ratings = array.array('f')
    interaction_timestamps = array.array('f')

    feature_item_ids = array.array('i')
    feature_ids = array.array('i')

    failed_parses = []
    total_parses = 0

    for block in _read_blocks(path):
        total_parses += 1
        try:
            (item_id,
             categories,
             user_ids,
             ratings,
             dates) = _parse_block(block)
        except Exception as e:
            print('Parse failed')
            failed_parses.append((e, block))

        user_ids = [user_dict.setdefault(x, len(user_dict))
                    for x in user_ids]

        interaction_user_ids.extend(user_ids)
        interaction_item_ids.extend([item_id] * len(user_ids))
        interaction_ratings.extend(ratings)
        interaction_timestamps.extend([int(time.mktime(x.timetuple()))
                                       for x in dates])

        categories = [feature_dict.setdefault(x, len(feature_dict))
                      for x in categories]

        feature_item_ids.extend([item_id] * len(categories))
        feature_ids.extend(categories)

    print('Num of failed parses: {} (out of {})'.format(len(failed_parses),
                                                        total_parses))

    return (np.array(interaction_user_ids),
            np.array(interaction_item_ids),
            np.array(interaction_ratings),
            np.array(interaction_timestamps),
            np.array(feature_item_ids),
            np.array(feature_ids))
