from config import config
from kafka import KafkaConsumer
from json import load
import json

from config.config import bootstrap_server1, bootstrap_server2, bootstrap_server3

if __name__ == '__main__':
    topic = 'trade'
    sasl_mechanism = "PLAIN"
    username = "forecast"
    password = "o$aPN26utpR@8ndcWU72"
    security_protocol = "SASL_SSL"

    consumer = KafkaConsumer(topic,
                             bootstrap_servers=bootstrap_server3,
                             api_version=(0, 10),
                             # security_protocol = security_protocol,
                             ssl_check_hostname = True,
                             sasl_mechanism=sasl_mechanism,
                             sasl_plain_username=username,
                             sasl_plain_password=password
                             )
    print('ok')
    print(type(consumer))
    for message in consumer:
        print(message.value)
        print('ok')
