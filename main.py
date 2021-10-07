from kafka import KafkaConsumer
from kafka import TopicPartition


from config.config import bootstrap_servers

if __name__ == '__main__':
    TOPIC = 'alpaca_trade'
    sasl_mechanism = "SCRAM-SHA-512"
    username = "user1"
    password = "gBNk$ccku$iZ*7$5NaUa"
    security_protocol = "SASL_SSL"
    sertificate_path = "/usr/local/share/ca-certificates/Yandex/YandexCA.crt"

    consumer = KafkaConsumer(#TOPIC,
                             bootstrap_servers=bootstrap_servers,
                             api_version=(0, 10),
                             security_protocol = security_protocol,
                             # ssl_check_hostname = True,
                             sasl_mechanism=sasl_mechanism,
                             sasl_plain_username=username,
                             sasl_plain_password=password,
                             ssl_cafile=sertificate_path
                             )

    # читать старые данные
    topic_partition = TopicPartition(TOPIC, 2)

    assigned_topic = [topic_partition]
    consumer.assign(assigned_topic)
    consumer.seek_to_beginning(topic_partition)
    # consumer.seek_to_end(topic_partition)

    for message in next(consumer):
        print(message.value)
