# utf-8
import time
import threading
from QUANTAXIS.QAARP.QAAccount import QA_Account
from QUANTAXIS.QAUtil.QALogs import QA_util_log_info
from QUANTAXIS.QAUtil.QAParameter import (AMOUNT_MODEL, FREQUENCE, MARKET_TYPE,
                                          ORDER_DIRECTION, ORDER_MODEL)
import os
import pandas as pd
from influxdb import InfluxDBClient
os.environ['http_proxy'] = '10.144.1.10:8080'
os.environ['https_proxy'] = '10.144.1.10:8080'
stock_hold = {}

class OpsInflux:
    def __init__(self,
                 host='10.70.62.66', # replace with influxdb
                 port=8086,
                 db_name='stockbase',
                 proxies={'http_proxy': '10.144.1.10:8080', 'https_proxy': '10.144.1.10:8080'}):
        self.client = InfluxDBClient(host=host, port=port, proxies=proxies)
        self.client.switch_database(db_name)

    def save_from_pandas(self,
                         df,
                         time_col,
                         tag_col,
                         feilds_col_list,
                         measurement_name):
        save_df = df.dropna(how='any')
        time_values = save_df[time_col].values
        tag_dict = None if tag_col is None else save_df[[tag_col]].to_dict('records')
        feilds_dict = save_df[feilds_col_list].to_dict('records')

        records = []
        for i in range(len(save_df)):
            sample = dict()
            sample['measurement'] = measurement_name
            sample['time'] = time_values[i]
            if tag_dict is not None:
                sample['tags'] = tag_dict[i]
            sample['fields'] = feilds_dict[i]
            records.append(sample)
        self.client.write_points(records, batch_size=200)

    def is_database_exist(self, db_name):
        return any(d.get('name', None) == db_name for d in self.client.get_list_database())

    def create_database(self, db_name):
        if not self.is_database_exist(db_name):
            self.client.create_database(db_name)

    def query(self, query):
        return self.client.query(query)

    def query_pandas(self, query):
        result_set = self.query(query)
        return pd.DataFrame(list(result_set)[0])

import json
from influxdb import InfluxDBClient
from pymongo import MongoClient
import pandas as pd


class OpsMongo:
    def __init__(self,
                 host='localhost',  # replace with mongo
                 port=27017,
                 db_name='quantaxis'):
        self.client = MongoClient('mongodb://{}'.format(host),
                                  port)
        self.db = self.client[db_name]

    def save_from_pandas(self, df, collection_name):
        collection = self.db[collection_name]
        records = json.loads(df.to_json(orient='records'))
        collection.insert_many(records)

    def query(self, collection_name, query={}):
        collection = self.db[collection_name]
        return collection.find(query)

    def query_list(self, collection_name, query={}):
        return list(self.query(collection_name, query=query))

    def query_pandas(self,
                     collection_name,
                     query={},
                     date_col=None,
                     ascending=True):
        df = pd.DataFrame(self.query_list(collection_name, query=query))
        if date_col is not None:
            df = df.sort_values(by='trade_date', ascending=ascending) \
                   .reset_index(drop=True)
        return df

ops_mongo = OpsMongo()
market = ops_mongo.query_pandas('index_day', query={'code': '000001'})
market['date'] = pd.to_datetime(market['date'], format='%Y-%m-%d')
trade_day = market.sort_values(by='date', ascending=True).reset_index(drop=True).date
print(trade_day)
ops_influx = OpsInflux()
df = ops_influx.query_pandas('select * from prediction')
prediction_df = df[['time',
                    'assetCode',
                    'prediction_returnsOpenNextMktres10',
                    'returnsOpenNextMktres10']]
prediction_df['code'] = prediction_df['assetCode'].apply(lambda x: x[:-3])
prediction_df['datetime'] = pd.to_datetime(prediction_df['time'], format='%Y-%m-%dT%H:%M:%SZ')


class MAStrategy(QA_Account):
    def __init__(self, user_cookie, portfolio_cookie, account_cookie,  init_cash=100000, init_hold={}):
        super().__init__(user_cookie=user_cookie, portfolio_cookie=portfolio_cookie, account_cookie= account_cookie,
                         init_cash=init_cash, init_hold=init_hold)
        self.frequence = FREQUENCE.DAY
        self.market_type = MARKET_TYPE.STOCK_CN
        self.commission_coeff = 0.00015
        self.tax_coeff = 0.0001
        self.reset_assets(100000)  # 这是第二种修改办法

    def on_bar(self, event):
        #print(threading.enumerate())

        sellavailable = self.sell_available
        #print('JBH DEBUG')
        #print(event.market_data.data)
        #print(event.market_data.data.columns)
        real = event.market_data.data.loc[(str(self.current_time)[:10], '000001')]

        #print(real)
        try:
            for item in event.market_data.code:
                print('JBH real: ' + str(item))
                current_index = trade_day.loc[trade_day == self.current_time].index[0]
                next_day = trade_day.loc[current_index + 1]
                print(next_day)
                # adapt for new train/predict strategy.
                #predict = prediction_df.loc[prediction_df['datetime'] == next_day]\
                #                       .loc[prediction_df['code'] == item]
                predict = prediction_df.loc[prediction_df['datetime'] == self.current_time]\
                                       .loc[prediction_df['code'] == item]
                real = event.market_data.data.loc[(str(self.current_time)[:10], '000001')]
                open_price = real['open']


                print(open_price)
                returnsOpenNextMktres10 = predict['prediction_returnsOpenNextMktres10'].values[0]
                print(returnsOpenNextMktres10)
                #print(returnsOpenNextMktres10.values)
                print(stock_hold)
                if sellavailable.get(item, 0) > 0 and stock_hold[item] >= 9:
                    print('sell ' + str(item))
                    del stock_hold[item]
                    event.send_order(account_cookie=self.account_cookie,
                                     amount=sellavailable[item], amount_model=AMOUNT_MODEL.BY_AMOUNT,
                                     time=self.current_time, code=item, price=0,
                                     order_model=ORDER_MODEL.MARKET, towards=ORDER_DIRECTION.SELL,
                                     market_type=self.market_type, frequence=self.frequence,
                                     broker_name=self.broker
                                     )
                elif returnsOpenNextMktres10 > 0 and item not in stock_hold:
                    print('buy ' + str(item))
                    stock_hold[item] = 0
                    event.send_order(account_cookie=self.account_cookie,
                                     amount=500, amount_model=AMOUNT_MODEL.BY_AMOUNT,
                                     time=self.current_time, code=item, price=0,
                                     order_model=ORDER_MODEL.MARKET, towards=ORDER_DIRECTION.BUY,
                                     market_type=self.market_type, frequence=self.frequence,
                                     broker_name=self.broker)
                elif item in stock_hold:
                    stock_hold[item] = stock_hold[item] + 1

        except Exception as e:
            print(e)
