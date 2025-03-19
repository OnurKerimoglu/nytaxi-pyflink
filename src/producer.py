import json
from kafka import KafkaProducer

class ConnectedJsonProducer():
    def __init__(
            self,
            server='localhost:9092',
            test=False
            ):
        self.server = server
        
        if test:
            producer = self.create_producer()

    def create_producer(self):
        producer = KafkaProducer(
            bootstrap_servers=[self.server],
            value_serializer=self.json_serializer
            )
        if producer.bootstrap_connected():
            print(f"Created the  kafka producer and connected to streamer at {self.server}")
        else:
            raise Exception(f"Failed to create the producer and connect to streamer at {self.server}")
        
        return producer

    def json_serializer(self, data):
        return json.dumps(data).encode('utf-8')


if __name__ == "__main__":
    producer = ConnectedJsonProducer(test=True)