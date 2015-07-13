from datetime import date, datetime
import json
import os
import logging

import pandas as pd

BASE_DIR = os.path.dirname(__file__)
logger = logging.getLogger(__file__)


def redistrict(df, kind, start=None, end=None, drop=False, splits=True):
    if isinstance(start, int):
        start = date(start, 1, 1)
    elif isinstance(start, datetime):
        start = start.date()
    if isinstance(end, int):
        end = date(end, 12, 31)
    elif isinstance(end, datetime):
        end = end.date()

    path = os.path.join(BASE_DIR, 'data', kind + '.json')
    data_dict = json.load(open(path))
    return apply_changes(df, data_dict['changes'], start=start, end=end,
                         drop=drop, splits=splits)


def apply_changes(df, changes, start=None, end=None, **kwargs):
    for change in changes:
        change_date = datetime.strptime(change['date'], '%Y-%m-%d').date()
        if start is not None and start < change_date:
            continue
        if end is not None and end > change_date:
            continue
        df = apply_change(df, change, **kwargs)
    return df


def apply_change(df, change, splits=True, **kwargs):
    if 'mergers' in change:
        df = apply_mergers(df, change['mergers'], **kwargs)
    if splits and 'splits' in change:
        df = apply_splits(df, change['splits'], **kwargs)

    if df.index.duplicated().any():
        logger.info('Summing up duplicate index.')
        df = df.groupby(df.index).sum()
        logger.info('Result: %s' % df)
    return df


def merge_series(df, series, index_id):
    df = df.append(pd.DataFrame([series], index=[index_id]))
    return df


def apply_mergers(df, mergers, drop=False):
    for merger in mergers:
        sentinels = merger['old_ids']
        series = df.ix[sentinels].apply(sum)
        if len(df.ix[sentinels]):
            logger.info('Merging %s to %s', sentinels, merger['id'])
            df = merge_series(df, series, merger['id'])
            logger.info('Results: %s', df)
            if drop:
                logger.info('Dropping %s', sentinels)
                df = df.drop(sentinels, errors='ignore')
    return df


def apply_splits(df, splits, drop=False):
    for split in splits:
        old_id = split['old_id']
        try:
            row = df.ix[old_id]
        except KeyError:
            continue
        for to in split['to']:
            ratio = to['ratio']
            series = row.copy() * ratio
            logger.info('Splitting %s to %s at %s', old_id, to['id'], to['ratio'])
            df = merge_series(df, series, to['id'])
            logger.info('Result: %s', df)
        if drop:
            logger.info('Dropping %s', old_id)
            df = df.drop(old_id, errors='ignore')
    return df
