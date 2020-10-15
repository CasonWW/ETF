from database import get_etf_list, dataframe_to_mongo, update_date, get_old_etf_data_by_fund
import time
from spider import ark, invesco, pro_shares, spdr, i_shares, gs, jpm
import pandas as pd
import logging
import schedule
from push import push as Push
from datetime import datetime

# init logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M',
                    handlers=[logging.FileHandler('error.log', 'w', 'utf-8'), ])
ark_c = 0


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


def main(fund, url, agency, last_updated_date, ark_u):
    def _etf(agency):
        return {
            'ARK': lambda x, y, z: ark(x, y, z),
            'Invesco': lambda x, y, z: invesco(x, y, z),
            'Pro Shares': lambda x, y, z: pro_shares(x, y, z),
            'SPDR': lambda x, y, z: spdr(x, y, z),
            'iShare': lambda x, y, z: i_shares(x, y, z),
            'GS': lambda x, y, z: gs(x, y, z),
            'JPM': lambda x, y, z: jpm(x, y, z)
        }[agency](url, last_updated_date, fund)

    etf_df = _etf(agency=agency)
    if etf_df is not None:
        date = etf_df['date'][0]
        etf_df = cal_change(fund, etf_df, last_updated_date, date)
        print(etf_df)
        dataframe_to_mongo(etf_df)
        update_date(fund, date)
        print(fund, "|", date, "|", agency)
        if agency in 'ARK':
            return ark_u + 1
        else:
            return ark_u


def start():
    ark_u = 0
    ETFs = get_etf_list()
    print('Fund', "|", 'Last Updated Date', "|", 'Agency')
    for ETF in ETFs:
        try:
            fund = ETF['fund']
            url = ETF['url']
            agency = ETF['agency']

            last_updated_date = ETF['last_updated_date']

            print(fund, "|", last_updated_date, "|", agency)

            # if last_updated_date.weekday() != 4:
            # if 'Invesco' in ETF['agency']:
            ark_u = main(fund, url, agency, last_updated_date, ark_u)
            time.sleep(5)
            # 10 16 20



        except Exception as Argument:
            logging.error('ETF', ETF['fund'], ':\nThis is the Argument:', Argument)

    if ark_u is not 0:
        print(ark_u)
        Push('d')
    elif ark_u is not 0 and datetime.today().weekday() is 5:
        Push('w')

    print('Done! ')


if __name__ == '__main__':
    # schedule.every().monday.at("12:00").do(start)
    # schedule.every().monday.at("20:00").do(start)
    # schedule.every().tuesday.at("09:00").do(start)
    # schedule.every().tuesday.at("12:00").do(start)
    # schedule.every().tuesday.at("18:00").do(start)
    # schedule.every().tuesday.at("21:20").do(start)
    # schedule.every().wednesday.at("09:00").do(start)
    # schedule.every().wednesday.at("12:00").do(start)
    # schedule.every().wednesday.at("18:00").do(start)
    # schedule.every().wednesday.at("21:20").do(start)
    # schedule.every().thursday.at("09:00").do(start)
    # schedule.every().thursday.at("12:00").do(start)
    # schedule.every().thursday.at("18:00").do(start)
    # schedule.every().thursday.at("21:20").do(start)
    # schedule.every().friday.at("09:00").do(start)
    # schedule.every().friday.at("12:00").do(start)
    # schedule.every().friday.at("18:00").do(start)
    # schedule.every().friday.at("21:20").do(start)
    # schedule.every().saturday.at("09:00").do(start)
    # schedule.every().saturday.at("18:00").do(start)
    #
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
    start()
