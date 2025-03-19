import os
from time import time

import pandas as pd

from producer import ConnectedJsonProducer

class CSVStreamer():
    def __init__(
        self,
        server,
        topic,
        fname
        ):
        self.topic = topic
        rootpath = os.path.dirname(os.path.dirname(__file__))
        self.datapath = os.path.join(rootpath, 'data')
        self.csvpath = os.path.join(self.datapath, fname)
        self.df = self.get_df()

        self.producer = ConnectedJsonProducer(
            server=server
           ).create_producer()
        print(f"initialized CSVStreamer for data file: {self.csvpath}")
    
    def send_all(self):
        print(f'Sending all {len(self.df)} rows to the streamer')
        t0 = time()
        for index, row in self.df.iterrows():
            message = row.to_dict()
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
        df = df[[
            'lpep_pickup_datetime',
            'lpep_dropoff_datetime',
            'PULocationID',
            'DOLocationID',
            'passenger_count',
            'trip_distance',
            'tip_amount']]
        return df


if __name__ == "__main__":
    csv_streamer = CSVStreamer(
        server='localhost:9092',
        topic='green-trips',
        fname='green_tripdata_2019-10.csv.gz')
    csv_streamer.send_all()
