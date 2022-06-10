import json
from datetime import datetime
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
        return self.session.execute(query)

    def close(self):
        self.session.shutdown()

    def select1(self):
        """
        Return the list of existing domains for which pages were created
        :return: list
        """
        query = f"SELECT DISTINCT domain_ from domains;"
        return list([x[0] for x in self.execute(query)])

    def select2(self, user_id):
        """
        Return all the pages which were created by the user with a specified user_id.
        :param user_id: str
        :return: list
        """
        query = f"SELECT uri from user_pages WHERE user_id = '{user_id}';"
        return list(self.execute(query))

    def select3(self, domain):
        """
        Return the number of articles created for a specified domain.
        :param domain: str
        :return: list
        """
        query = f"SELECT COUNT(*) from domains WHERE domain_ = '{domain}';"
        return list(self.execute(query))

    def select4(self, page_id):
        """
        Return the page with the specified page_id
        :param page_id: str
        :return: list
        """
        query = f"SELECT * from page_ids WHERE page_id = '{page_id}';"
        return list(self.execute(query))

    def select5(self, date1, date2):
        """
        Return the id, name, and the number of created pages of all the users
        who created at least one page in a specified time range.
        :param date1: str
        :param date2: str
        :return: dict
        """
        from dateutil import rrule
        from datetime import datetime, timedelta
        all_list = []
        date1 = datetime.strptime(date1, "%Y-%m-%d %H:%M:%S")
        date2 = datetime.strptime(date2, "%Y-%m-%d %H:%M:%S")
        for dt in rrule.rrule(rrule.MINUTELY, dtstart=date1, until=date2):
            query = f"SELECT user_id, user_text, COUNT(user_id) from users WHERE (created_at = '{dt}') GROUP BY user_id; "
            all_list.extend(list(self.execute(query)))
        return all_list


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
    user_text = dict_line['performer']['user_text'].replace("'","")
    page_id = dict_line['page_id']
    created_at = datetime.strptime(dict_line['meta']['dt'], "%Y-%m-%dT%H:%M:%SZ").replace(second=0)

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

