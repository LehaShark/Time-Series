from config import config
from kafka import KafkaConsumer
from kafka import TopicPartition
from json import load
import json

from config.config import bootstrap_server1, bootstrap_server2, bootstrap_server3

if __name__ == '__main__':
    TOPIC = 'trade'
    sasl_mechanism = "PLAIN"
    username = "forecast"
    password = "o$aPN26utpR@8ndcWU72"
    security_protocol = "SASL_SSL"

    consumer = KafkaConsumer(#TOPIC,
                             bootstrap_servers=bootstrap_server1,
                             api_version=(0, 10),
                             # security_protocol = security_protocol,
                             # ssl_check_hostname = True,
                             sasl_mechanism=sasl_mechanism,
                             sasl_plain_username=username,
                             sasl_plain_password=password
                             )
    #print(consumer.topics())
    print('ok1')
    print(type(consumer))
    topic_partition = TopicPartition(TOPIC, 2)
    assigned_topic = [topic_partition]
    consumer.assign(assigned_topic)

    # consumer.poll()
    # consumer.seek_to_end(topic_partition)

    #partitions = consumer.partitions_for_topic(TOPIC)
    consumer.seek_to_beginning(topic_partition)
    #msg = next(consumer)
    #print (msg)
    for message in next(consumer):
        print('ok')
        print(message.value)
