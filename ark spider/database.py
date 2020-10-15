import pymongo
from config import MongoDB

client = pymongo.MongoClient(MongoDB)
database = client["ARK"]


def dataframe_to_mongo(df):
    collection = database['etf_data']

    records = df.to_dict('records')
    collection.insert_many(records)


def get_etf_list():
    collection = database['etf_list']

    return list(collection.find({}, {"_id": 0}))


def get_etf_list_by_query(query):
    collection = database['etf_list']

    return list(collection.find(query, {"_id": 0}).limit(5))


def get_etf_data(query):
    collection = database['etf_data']

    return list(collection.find(query))


def update_date(fund, date):
    collection = database['etf_list']

    collection.update_one({'fund': fund}, {"$set": {'last_updated_date': date}})


def get_old_etf_data_by_fund(fund, last_updated_date):
    collection = database['etf_data']

    return list(collection.find({'fund': fund, 'date': last_updated_date}, {'_id': 0, 'weight': 0}))


def main():
    get_etf_list()


if __name__ == "__main__":
    main()
