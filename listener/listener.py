from kafka import KafkaConsumer
from json import load
import json



topic = 'trade'
sasl_mechanism = "PLAIN"
username = "username"
password = "password"
security_protocol = "SASL_SSL"

consumer = KafkaConsumer('trade',
                         bootstrap_servers=['rc1c-894cbo6uhb90srcg.mdb.yandexcloud.net:9091'],
                         api_version = (0, 10),
                         # security_protocol = security_protocol,
                         # ssl_check_hostname = True,
                         sasl_mechanism = sasl_mechanism,
                         sasl_plain_username = username,
                         sasl_plain_password = password
                         )
for message in consumer:
    print(message.value)