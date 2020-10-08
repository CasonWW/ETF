import pandas as pd
import requests
from datetime import datetime
import io
import numpy as np
import re


def ark(url, last_updated_date, fund):
    req = requests.get(url)
    df = pd.read_csv(io.StringIO(req.content.decode('utf-8')))
    date = datetime.strptime(df['date'][0], '%m/%d/%Y')

    if last_updated_date < date:
        df = df[df["ticker"].notnull()]
        df.loc[:, 'date'] = date
        df.loc[:, 'fund'] = fund

        return df[['ticker', 'shares', 'date','fund']]


def invesco(url, last_updated_date, fund):
    req = requests.get(url)
    df = pd.read_csv(io.StringIO(req.content.decode('utf-8')))
    date = datetime.strptime(df['Date'][0], '%m/%d/%Y')

    if last_updated_date < date:
        df = df.rename({'Holding Ticker': 'ticker', 'Shares/Par Value': 'shares', 'Date': 'date'},
                       axis='columns')

        df = df[df['ticker'].notnull()]
        df.loc[:, 'date'] = date
        df.loc[:, 'fund'] = fund
        df = df[~df['ticker'].str.contains('-')]
        df['ticker'] = df['ticker'].str.replace(' ', '').astype(str)
        df['shares'] = df['shares'].str.replace(',', '').astype(float)

        return df[['ticker', 'shares', 'date', 'fund']]


def pro_shares(url, last_updated_date, fund):
    req = requests.get(url)
    df = pd.read_csv(io.StringIO(req.content.decode('utf-8')), sep="\t", header=None)

    date = datetime.strptime(str(df.iat[1, 0])[6:15], '%m/%d/%Y')

    if last_updated_date < date:
        df = df.drop([0, 1, 2])

        data = df.to_numpy()
        data[0][0] = re.compile(' ').sub('', data[0][0])

        func = np.vectorize(lambda x: x.replace('"', "").split(','))

        nData = [func(data[0][0])]
        nData[0] = np.append(nData[0], '')

        for x in data[1:-1]:
            nData.append(func(x[0]))

        df = pd.DataFrame(nData[1:-1], columns=nData[0])

        df.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
        df = df[df['SecurityTicker'].notnull()]
        df = df.rename({'SecurityTicker': 'ticker', 'Shares/Contracts': 'shares'}, axis='columns')
        df.loc[:, 'date'] = date
        df.loc[:, 'fund'] = fund

        df['shares'] = df['shares'].str.replace(',', '').astype(float)
        df = df.reset_index()

        return df[['ticker', 'shares', 'date', 'fund']]


def spdr(url, last_updated_date, fund):
    df = pd.read_excel(url, header=None)
    date = datetime.strptime(str(df.iat[2, 1])[6:], '%d-%b-%Y')

    if last_updated_date < date:
        df = df.drop([0, 1, 2, 3])

        data = df.to_numpy()
        for i in range(len(data[0])):
            data[0][i] = re.compile(' ').sub('', str(data[0][i]))

        df = pd.DataFrame(data[1:-1], columns=data[0])

        df = df.rename({'Ticker': 'ticker', 'SharesHeld': 'shares'}, axis='columns')
        df = df[df['ticker'].notnull()]

        if 'LocalCurrency' in df.columns:
            df = df[df['LocalCurrency'] == 'USD']
            df = df[df['SEDOL'] != 'Unassigned']

        df.loc[:, 'date'] = date
        df.loc[:, 'fund'] = fund

        df['ticker'] = df['ticker'].str.replace(' US', '')
        df['shares'] = df['shares'].str.replace(',', '').astype(float)
        df = df.reset_index()

        return df[['ticker', 'shares', 'date', 'fund']]


def i_shares(url, last_updated_date, fund):
    req = requests.get(url)

    df = pd.read_csv(io.StringIO(req.content.decode('utf-8')), sep="\t", header=None)
    date = datetime.strptime(str(df.iat[1, 0].split('"')[1]), '%b %d, %Y')

    if last_updated_date < date:
        df = df.drop([0, 1, 2, 3, 4, 5, 6, 7, 8])

        data = df.to_numpy()

        data[0][0] = re.compile(' ').sub('', data[0][0])

        func_columns = np.vectorize(lambda x: x.strip('\"').split(','))
        func = np.vectorize(lambda x: x.split('"'))

        new_data = [func_columns(data[0][0])]
        new_data[0] = np.append(new_data[0], '')

        for x in data[1:-1]:
            new_data.append([i for i in func(x[0]) if i != ','])

        df = pd.DataFrame(new_data[1:-1], columns=new_data[0])

        df['Ticker'] = df['Ticker'].str.replace(',', '').astype(str)
        df['Shares'] = df['Shares'].str.replace(',', '').astype(float)
        df = df[(df['Ticker'] != '-')]
        df = df[(df['Exchange'] == 'New York Stock Exchange Inc.') | (df['Exchange'] == 'NASDAQ')]

        df = df.rename({'Ticker': 'ticker', 'Shares': 'shares'}, axis='columns')
        df.loc[:, 'date'] = date
        df.loc[:, 'fund'] = fund
        df = df.reset_index()

        return df[['ticker', 'shares', 'date', 'fund']]


def gs(url, last_updated_date, fund):
    df = pd.read_excel(url, header=None)

    date = df[0][3]

    if last_updated_date < date:
        df = df.drop([0, 1])

        data = df.to_numpy()

        for i in range(len(data[0])):
            data[0][i] = re.compile(' ').sub('', str(data[0][i]))

        df = pd.DataFrame(data[1:-1], columns=data[0])

        df = df.rename({'Ticker': 'ticker', 'NumberofShares': 'shares', 'Date': 'date'},
                       axis='columns')
        df = df[df['ticker'].notnull()]
        df = df[df['Cusip'] != '--']
        df = df.reset_index()

        df.loc[:, 'date'] = date
        df.loc[:, 'fund'] = fund

        df['shares'] = df['shares'].str.replace(',', '').astype(float)

        return df[['ticker', 'shares', 'date', 'fund']]


def jpm(url, last_updated_date, fund):
    df = pd.read_excel(url, header=None)

    date = datetime.strptime((df[7][5]).split(':')[1].strip(' '), '%m/%d/%Y')

    if last_updated_date < date:
        df = df.drop([0, 1, 2, 3, 4, 5, 6])

        data = df.to_numpy()

        for i in range(len(data[0])):
            data[0][i] = re.compile(' ').sub('', str(data[0][i]))

        df = pd.DataFrame(data[1:-1], columns=data[0])
        df = df.rename({'Ticker': 'ticker', 'Shares/Par': 'shares'}, axis='columns')
        df = df[df['ticker'].notnull()]

        df = df[df['Currency'] == 'USD']

        df.loc[:, 'date'] = date
        df.loc[:, 'fund'] = fund

        df['shares'] = df['shares'].str.replace(',', '').astype(float)
        df = df.reset_index()

        return df[['ticker', 'shares', 'date', 'fund']]


def main():
    etf = gs(
        'https://www.gsam.com/content/dam/gsam/xls/us/en/etf/Goldman%20Sachs%20ActiveBeta%20U.S.%20Small%20Cap%20Equity%20ETF_9107.xlsx',
        datetime.strptime('01/01/1990', '%d/%m/%Y'), 'gssc')

    # for e in list(etf['ticker']):
    #     print(e, ':', len(e))
    print(etf)


if __name__ == '__main__':
    main()
