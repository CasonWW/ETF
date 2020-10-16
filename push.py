from api import f, dc
from database import get_etf_list_by_query, get_etf_data
import pandas as pd
import os.path
from datetime import datetime, timedelta
import numpy as np


def daily():
    ark_etf_list = list(get_etf_list_by_query({'agency': 'ARK'}))

    path = 'ark_daily_change.csv'
    if os.path.isfile(path):
        os.remove(path)

    etf_data = []

    for ark_etf in ark_etf_list:
        ark_data_list = list(
            get_etf_data(
                {'fund': ark_etf['fund'], 'date': ark_etf['last_updated_date'],
                 'change': {'$ne': 0}}))
        if len(ark_data_list) is not 0:
            etf_data = etf_data + ark_data_list

    df = pd.DataFrame(etf_data)
    df = df.drop(columns=['_id']).sort_values(by=['ticker', 'date'])
    df.to_csv(path, index=False)
    dc(path)

    df['shares'] = df['shares'].astype(np.int64).astype(str)
    df['date'] = df['date'][0].strftime('%Y-%m-%d')
    df = df.reset_index(drop='index')
    md = df.to_markdown()
    # f(md)

    print('pushed daily change')


def weekly():
    ark_etf_list = list(get_etf_list_by_query({'agency': 'ARK'}))
    path = 'ark_weekly_change.csv'
    if os.path.isfile(path):
        os.remove(path)

    etf_data = []

    for ark_etf in ark_etf_list:
        ark_data_list = list(
            get_etf_data(
                {'fund': ark_etf['fund'], 'date': {'$gte': ark_etf['last_updated_date'] + timedelta(days=-5),
                                                   '$lte': ark_etf['last_updated_date']},
                 'change': {'$ne': 0}}))
        if len(ark_data_list) is not 0:
            etf_data = etf_data + ark_data_list

    df = pd.DataFrame(etf_data)
    df = df.drop(columns=['_id']).sort_values(by=['ticker', 'date'])
    df.to_csv(path, index=False)

    dc(path)
    print('pushed weekly change')


def push(t):
    if t is 'd':
        daily()
    elif t is 'w':
        weekly()


if __name__ == '__main__':
    push('d')
