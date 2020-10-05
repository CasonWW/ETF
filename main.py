from database import get_etf_list, dataframe_to_mongo, update_date, get_old_etf_data_by_fund
import time
from spider import ark, invesco, pro_shares, spdr, i_shares, gs, jpm
from datetime import datetime
import pandas as pd
import logging

# init logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M',
                    handlers=[logging.FileHandler('error.log', 'w', 'utf-8'), ])


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
            etf.append({'ticker': row[0], 'shares': 0, 'weight': '0', 'date': date, 'change': -row[1]},
                       ignore_index=True)

    return etf


def main(fund, url, agency, last_updated_date):
    def _etf(agency):
        return {
            'ARK': lambda x, y: ark(x, y),
            'Invesco': lambda x, y: invesco(x, y),
            'Pro Shares': lambda x, y: pro_shares(x, y),
            'SPDR': lambda x, y: spdr(x, y),
            'iShare': lambda x, y: i_shares(x, y),
            'GS': lambda x, y: gs(x, y),
            'JPM': lambda x, y: jpm(x, y)

        }[agency](url, last_updated_date)

    etf_df = _etf(agency=agency)
    if etf_df is not None:
        date = etf_df['date'][0]
        etf_df = cal_change(fund, etf_df, last_updated_date.strftime('%d/%m/%Y'), date)
        print(etf_df)
        dataframe_to_mongo(fund, etf_df)
        update_date(fund, date)
        print(fund, "|", date, "|", agency)


if __name__ == '__main__':
    ETFs = get_etf_list()
    print('Fund', "|", 'Last Updated Date', "|", 'Agency')
    for ETF in ETFs:
        try:
            fund = ETF['fund']
            url = ETF['url']
            agency = ETF['agency']

            last_updated_date = datetime.strptime(ETF['last_updated_date'], '%d/%m/%Y')

            print(fund, "|", ETF['last_updated_date'], "|", agency)

            if last_updated_date.weekday() != 4:
                #if 'GS' in ETF['agency']:
                main(fund, url, agency, last_updated_date)
                time.sleep(5)

        except Exception as Argument:
            logging.error('ETF', ETF['fund'], ':\nThis is the Argument:', Argument)

    print('Done! ')
