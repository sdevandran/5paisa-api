from py5paisa import FivePaisaClient
import configparser
from pymongo import MongoClient
from datetime import date, timedelta

config = configparser.RawConfigParser()
config_file = 'keys.conf'
config.read(config_file)


class Database:
    def __init__(self):
        mclient = MongoClient()
        self.db = mclient.five_paisa

    def select(self, query, collection_name):
        cn = self.db[collection_name]
        data = cn.find_one(query)
        print("Fetched data")
        return data

    def insert_one(self, data, collection_name):
        cn = self.db[collection_name]
        cn.insert_one(data)
        print("one record inserted")

    def insert_all(self, df, collection_name):
        cn = self.db[collection_name]
        cn.insert_many(df.to_dict('records'), ordered=False)
        print("all records inserted")

    def update(self, query, update_query, collection_name):
        cn = self.db[collection_name]
        data = cn.find_one(query)
        if data:
            new_data = {'$set': update_query}
            cn.update_one(data, new_data)
            print("updated")

    def delete(self, query, collection_name):
        cn = self.db[collection_name]
        data = cn.find_one(query)
        if data:
            cn.delete_one(query)
            print("Deleted")


class FivePaisa:
    def __init__(self, db_obj):
        self.client = FivePaisaClient(email="Sm.nazarr@gmail.com", passwd="Login@123456", dob="19871116",
                                      cred=dict((k.upper(), v) for k, v in config.items('KEYS')))
        # remove later
        self.client.session.verify = False
        self.client.login()
        self.yesterday = date.today() - timedelta(days=1)
        self.last_1yr_start = self.yesterday - timedelta(days=365)
        self.data = None
        self.db_obj = db_obj

    def historical_data(self, scrip_code):
        self.data = self.client.historical_data('N', 'C', scrip_code, '1d', str(self.last_1yr_start),
                                                str(self.yesterday))

    def to_csv(self, file_name):
        self.data.to_csv(file_name + ".csv", index=False)
        print("Data stored as csv")

    def insert_to_db(self, collection_name):
        if not self.data.empty:
            self.db_obj.insert_all(self.data, collection_name)
            print("All Data inserted to Database")

    def live_data(self, scrip_code, collection_name):
        req_list = [{"Exch": "N", "ExchType": "C", "ScripCode": scrip_code}]
        req_data = self.client.Request_Feed('mf', 's', req_list)

        def on_receive(ws, message):
            data = eval("{" + message[2:-2] + "}")
            self.db_obj.insert_one(data, collection_name)
        self.client.connect(req_data)
        self.client.receive_data(on_receive)


def prob1():
    db_obj = Database()
    fp = FivePaisa(db_obj)

    stocks = {'NIFTY': '1660', 'RELIANCE': '2885', 'SBIN': '2885', 'MRF': '2277', 'JSWSTEEL': '11723'}

    for stock_name, scrip_code in stocks.items():
        print(stock_name)
        fp.historical_data(scrip_code)
        fp.to_csv(stock_name)
        fp.insert_to_db(stock_name)

    print(db_obj.select({'High': 554.6}, 'JSWSTEEL'))


def prob2():
    db_obj = Database()
    fp = FivePaisa(db_obj)
    scrip_code = '2885'
    collection_name = 'SBIN'
    fp.live_data(scrip_code, collection_name)


prob1()
prob2()
