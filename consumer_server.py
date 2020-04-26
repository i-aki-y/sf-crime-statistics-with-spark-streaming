from kafka import KafkaConsumer
import json
import time
from kafka_server import TOPIC_NAME

class ConsumerServer(KafkaConsumer):

    def __init__(self, topics, **kwargs):
        super().__init__(*topics, **kwargs)


if __name__ == "__main__":

    consumer = ConsumerServer(topics=[TOPIC_NAME],
                              group_id="consumer-test-1",
                              auto_offset_reset='earliest',
                              enable_auto_commit=False,
                              value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for message in consumer:
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))

        time.sleep(0.1)
