from datetime import date, datetime
import json
import os
import logging

import pandas as pd

BASE_DIR = os.path.dirname(__file__)
logger = logging.getLogger(__file__)


def redistrict(df, kind, start=None, end=None, drop=False, splits=True):
    """Redistrict dataframe indexed by district

    Args:
        df (pandas.DataFrame): input dataframe
        kind (string): identifier of redistrict info (e.g. de/kreise)
        start (int, datetime or date, optional):
                Use only redistrict information beginningwith this year.
        end (int, datetime or date, optional):
                Use only redistrict information up to and including this year.
        drop (bool, default: False): Drop rows with old identifiers.
        splits (bool, default: True): Apply district splitting operations

    Returns:
        pandas.DataFrame: Redistricted dataframe
    """
    if isinstance(start, int):
        start = date(start, 1, 1)
    elif isinstance(start, datetime):
        start = start.date()
    if isinstance(end, int):
        end = date(end, 12, 31)
    elif isinstance(end, datetime):
        end = end.date()
    data_dict = get_data_dict(kind)
    return apply_changes(df, data_dict['changes'], start=start, end=end,
                         drop=drop, splits=splits)


def redistrict_grouped(df, kind, group_cols, district_col=None,
                       value_cols=None, **kwargs):
    """Redistrict dataframe by groups

    Args:
        df (pandas.DataFrame): input dataframe
        kind (string): identifier of redistrict info (e.g. de/kreise)
        group_cols (list): List of column names to group by
        district_col (string): Name of district column
        value_cols (list): List of column names with values to operate on
        **kwargs: see redistrict function

    Returns:
        pandas.Dataframe: Redistricted dataframe
    """
    return pd.concat(redistrict_grouped_dataframe(df, kind, group_cols,
                     district_col=district_col, value_cols=value_cols,
                     **kwargs))


def redistrict_rows(df, kind, **kwargs):
    data_dict = get_data_dict(kind)
    affected_ids = list(get_affected(data_dict['changes'], **kwargs))
    filtered_df = df[df[kwargs['district_col']].isin(affected_ids)]
    row_generator = redistrict_rows_generator(filtered_df.iterrows(),
                                              kind, **kwargs)

    rows = list(row_generator)
    index, rows = [list(t) for t in zip(rows)]
    df = pd.DataFrame(rows, index=index)
    return df


def redistrict_rows_generator(row_iterator, kind, **kwargs):
    for i, row in row_iterator:
        for new_i, new_row in redistrict_row(i, row, kind, **kwargs).iterrows():
            yield i, new_row


def redistrict_row(index, row, kind, **kwargs):
    district_col = kwargs.pop('district_col')
    value_cols = kwargs.pop('value_cols')
    other_cols = list(set(row.index) - set([district_col] + value_cols))
    df = row.to_frame().T.set_index(district_col)[value_cols]
    df[value_cols] = df[value_cols].convert_objects(convert_numeric=True)
    new_df = redistrict(df, kind, **kwargs)
    new_df = new_df.dropna()
    new_df = new_df.reset_index()
    for o in other_cols:
        new_df[o] = row[o]
    return new_df


def get_affected(changes, **kwargs):
    for change in changes:
        if not is_change_affected(change, kwargs.get('start'), kwargs.get('end')):
            continue
        for m in change['mergers']:
            for o in m['old_ids']:
                yield o
        for s in change.get('splits', []):
            yield s['old_id']


def is_change_affected(change, start=None, end=None):
    change_date = datetime.strptime(change['date'], '%Y-%m-%d').date()
    if start is not None and start > change_date:
        return False
    if end is not None and end < change_date:
        return False
    return True


def get_data_dict(kind):
    path = os.path.join(BASE_DIR, 'data', kind + '.json')
    return json.load(open(path))


def redistrict_grouped_dataframe(df, kind, group_cols, district_col=None,
                                 value_cols=None, **kwargs):
    grouped = df.groupby(group_cols)
    for k, gdf in grouped:
        if not isinstance(k, tuple):
            k = tuple([k])
        if callable(kwargs.get('start', None)):
            kwargs['start'] = kwargs['start'](dict(zip(group_cols, k)))
        if callable(kwargs.get('end', None)):
            kwargs['end'] = kwargs['end'](dict(zip(group_cols, k)))
        gdf_r = redistrict(gdf.set_index(district_col)[value_cols], kind, **kwargs)
        gdf_r = gdf_r.reset_index()
        for n, v in zip(group_cols, k):
            gdf_r[n] = v
        yield gdf_r


def apply_changes(df, changes, start=None, end=None, **kwargs):
    for change in changes:
        if not is_change_affected(change, start, end):
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
        if len(df.ix[sentinels].dropna(how='all')):
            series = df.ix[sentinels].fillna(0).apply(sum)
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
