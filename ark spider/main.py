from database import get_etf_list, dataframe_to_mongo, update_date, get_old_etf_data_by_fund
import time
import schedule
import pandas as pd
import requests
from datetime import datetime
import io


def cal_change(fund, etf, last_updated_date, date):
    old_df = pd.DataFrame(get_old_etf_data_by_fund(fund, last_updated_date))
    last_change = []

    for index, row in etf.iterrows():
        if row[0] in old_df['ticker'].values:
            change = row[1] - (old_df[old_df['ticker'] == row[0]]['shares'].values[0])
            last_change.append(change)
        else:
            last_change.append(row[1])

    etf['change'] = last_change

    for index, row in old_df.iterrows():
        if row[0] not in etf['ticker'].values:
            etf.append({'ticker': row[0], 'shares': 0, 'date': date, 'change': -row[1]},
                       ignore_index=True)

    return etf


def spider(url, last_updated_date, fund):
    req = requests.get(url)
    df = pd.read_csv(io.StringIO(req.content.decode('utf-8')))
    date = datetime.strptime(df['date'][0], '%m/%d/%Y')

    if last_updated_date < date:
        df = df[df["ticker"].notnull()]
        df = df.rename({'weight(%)': 'weight'},
                       axis='columns')
        df.loc[:, 'date'] = date
        df.loc[:, 'fund'] = fund

        return df[['ticker', 'shares', 'weight', 'date', 'fund']]


def main(fund, url, last_updated_date):
    etf_df = spider(url, last_updated_date, fund)
    if etf_df is not None:
        date = etf_df['date'][0]
        etf_df = cal_change(fund, etf_df, last_updated_date, date)
        print(etf_df)
        dataframe_to_mongo(etf_df)
        update_date(fund, date)
        print(fund, "|", date)


def start():
    ETFs = get_etf_list()
    print('Fund', "|", 'Last Updated Date', "|", 'Agency')
    for ETF in ETFs:
        try:
            fund = ETF['fund']
            url = ETF['url']
            agency = ETF['agency']

            last_updated_date = ETF['last_updated_date']

            print(fund, "|", last_updated_date, "|", agency)

            main(fund, url, last_updated_date)
            time.sleep(5)

        except Exception as Argument:
            print('ETF', ETF['fund'], ':\nThis is the Argument:', Argument)

    print('Done! ')


if __name__ == '__main__':
    schedule.every().tuesday.at("09:00").do(start)
    schedule.every().wednesday.at("09:00").do(start)
    schedule.every().thursday.at("09:00").do(start)
    schedule.every().friday.at("09:00").do(start)
    schedule.every().saturday.at("09:00").do(start)

    while True:
        schedule.run_pending()
        time.sleep(1)
    # start()
