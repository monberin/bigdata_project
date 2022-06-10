import json
from json import loads
from kafka import KafkaConsumer


consumer = KafkaConsumer('wiki-data',
                         bootstrap_servers=['kafka-server:9092'],
                         api_version=(2,0,2),
                         value_deserializer=lambda x:
                         loads(x.decode('ascii')))

class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None
        self.row=0

    def connect(self):
        from cassandra.cluster import Cluster
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)

    def execute(self, query):
        self.session.execute(query)

    def close(self):
        self.session.shutdown()

    # def select1(self, uid):
    #     """
    #     Return all transactions for uid that were fraudulent
    #     :param uid: str
    #     :return: dict
    #     """
    #     query = f"SELECT * from uid_fraud WHERE uid = '{uid}' AND isFraud = 1;"
    #     return list(self.execute(query))
    #
    # def select2(self, uid):
    #     """
    #     Return 3 biggest transactions for uid
    #     :param uid: str
    #     :return: dict
    #     """
    #     query = f"SELECT * from uid_big WHERE uid = '{uid}';"
    #     return list(sorted(self.execute(query), reverse=True))[:3]
    #
    # def select3(self, uid, date1, date2):
    #     """
    #     Return sum of all transactions for a uid for a given period of time
    #     :param uid: str
    #     :param date1: str
    #     :param date2: str
    #     :return: dict
    #     """
    #     query = f"SELECT SUM(amount) from reciever_uid WHERE reciever_uid = '{uid}' AND (transaction_date >= '{date1}') AND " \
    #             f"(transaction_date <= '{date2}'); "
    #     return list(self.execute(query))


    def insert_values(self, table, val_dict):
        # {'domain': domain, 'uri': uri, 'user_id': user_id,
        # 'user_text': user_text, 'page_id': page_id, 'created_at': created_at}
        if table == 'domains':
            dom = val_dict["domain"]
            uri = val_dict["uri"]
            qr = f"INSERT INTO {table} (domain_, uri) VALUES ('{dom}','{uri}')"
            self.execute(qr)
        if table == 'user_pages':
            uid = val_dict["user_id"]
            uri = val_dict["uri"]
            qr = f"INSERT INTO {table} (user_id, uri) VALUES ('{uid}', '{uri}')"
            self.execute(qr)
        if table == 'page_ids':
            pid = val_dict["page_id"]
            uri = val_dict["uri"]
            qr = f"INSERT INTO {table} (page_id, uri) VALUES ('{pid}', '{uri}')"
            self.execute(qr)
        if table == 'users':
            uid = val_dict["user_id"]
            utext = val_dict["user_text"]
            uri = val_dict["uri"]
            at = val_dict["created_at"]
            qr = f"INSERT INTO {table} (user_id, user_text, uri, created_at) VALUES ('{uid}', '{utext}', '{uri}', '{at}')"
            self.execute(qr)


def extract_data(line):
    from ast import literal_eval
    dict_line = literal_eval(line)

    domain = dict_line['meta']['domain']
    uri = dict_line['meta']['uri']
    try:
        user_id = dict_line['performer']['user_id']
    except KeyError:
        user_id = '0'
    user_text = dict_line['performer']['user_text']
    page_id = dict_line['page_id']
    created_at = dict_line['meta']['dt']

    return {'domain': domain, 'uri': uri, 'user_id': user_id,
            'user_text': user_text, 'page_id': page_id, 'created_at': created_at}

def main():
    client = CassandraClient(host='cassandra-server', port=9042, keyspace='project')
    client.connect()
    for message in consumer:
        message = message.value
        message = extract_data(str(loads(message[5:])))
        client.insert_values('page_ids', message)
        client.insert_values('users', message)
        client.insert_values('domains', message)
        client.insert_values('user_pages', message)

    # client.close()


if __name__ == '__main__':
    main()

