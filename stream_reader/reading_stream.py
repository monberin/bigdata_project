from time import sleep
from json import dumps
from kafka import KafkaProducer

import requests
from requests.exceptions import ChunkedEncodingError

producer = KafkaProducer(bootstrap_servers=['kafka-server:9092'],
                         api_version=(2,0,2),
                         value_serializer=lambda x:
                         dumps(x).encode('ascii'))


def get_stream_data(topic):
    s = requests.Session()

    while True:
            try:
                with s.get('https://stream.wikimedia.org/v2/stream/page-create', headers=None, stream=True) as resp:
                    for line in resp.iter_lines(decode_unicode=True):
                        if line and line.split(':')[0] == "data":
                            print(line)

                            producer.send(topic, line)
            except ChunkedEncodingError:
                print('bad read, skip')

def main():
    get_stream_data('wiki-data')


if __name__ == '__main__':
    main()
