import os
from time import time

import pandas as pd

from producer import ConnectedJsonProducer

class CSVStreamer():
    def __init__(
        self,
        server,
        topic,
        fname,
        offset=0,
        limit=-1
        ):
        # input arguments
        self.server = server
        self.topic = topic
        self.fname = fname
        self.offset = offset
        self.limit = limit

        # read the csv
        rootpath = os.path.dirname(os.path.dirname(__file__))
        self.datapath = os.path.join(rootpath, 'data')
        self.csvpath = os.path.join(self.datapath, self.fname)
        self.df = self.get_df()

        # get the producer
        self.producer = ConnectedJsonProducer(
            server=self.server
           ).create_producer()
        print(f"initialized CSVStreamer for data file: {self.csvpath}")
    
    def send_all(self):
        print(f'Sending all {len(self.df)} rows to the streamer')
        t0 = time()
        for index, row in self.df.iterrows():
            message = row.to_dict()
            # message['event_timestamp'] = time() * 1000
            self.producer.send(
                topic=self.topic,
                value=message
            )
            print(f'sent row: {index}', end='\r')
            # time.sleep(0.05)
        self.producer.flush()
        t1 = time()
        print(f'Done. Elapsed time: {(t1 - t0):.2f} seconds')

    def get_df(self):
        df = pd.read_csv(self.csvpath)
        df['row_index'] = range(df.shape[0])
        df = df[[
            'row_index',
            'lpep_pickup_datetime',
            'lpep_dropoff_datetime',
            'PULocationID',
            'DOLocationID',
            'passenger_count',
            'trip_distance',
            'tip_amount']]
        # df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        # df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
        # df['passenger_count'] = df['passenger_count'].astype(int)
        if self.limit != -1:
            df_sel = df.iloc[self.offset:min(self.offset + self.limit,df.shape[0]+1)]
        else:
            df_sel = df.iloc[self.offset:]
        print(f'Loaded {df_sel.shape[0]} rows out of {df.shape[0]} from {self.csvpath} with fields:\n{df.dtypes}')
        return df_sel


if __name__ == "__main__":
    csv_streamer = CSVStreamer(
        server='localhost:9092',
        topic='green-trips',
        fname='green_tripdata_2019-10.csv.gz',
        offset=100,
        limit=100)
    csv_streamer.send_all()
