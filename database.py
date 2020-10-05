import pymongo
from config import MongoDB

client = pymongo.MongoClient(MongoDB)
database = client["ETF"]


def dataframe_to_mongo(fund, df):
    collection = database[fund]

    records = df.to_dict('records')
    collection.insert_many(records)


def get_etf_list():
    collection = database['etf_list']

    return list(collection.find({}, {"_id": 0}))


def update_date(fund, date):
    collection = database['etf_list']

    collection.update_one({'fund': fund}, {"$set": {'last_updated_date': date}})


def test_connection():
    maxSevSelDelay = 1
    try:
        c = pymongo.MongoClient(MongoDB, serverSelectionTimeoutMS=maxSevSelDelay)
        c.server_info()
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(err)


def get_old_etf_data_by_fund(fund, last_updated_date):
    collection = database[fund]

    return list(collection.find({'date': last_updated_date}, {'_id': 0, 'weight': 0}))


def main():
    test_connection()


if __name__ == "__main__":
    main()
