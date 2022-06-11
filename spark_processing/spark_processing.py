from cassandra.cluster import Cluster
from cassandra.query import dict_factory
# from datetime import datetime
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from apscheduler.schedulers.blocking import BlockingScheduler

host = 'cassandra-server'
port = 9042
keyspace = 'project'

cluster = Cluster([host], port=port)
session = cluster.connect(keyspace)
session.row_factory = dict_factory

spark = SparkSession.builder.appName('project').getOrCreate()


def current_hour():
    return datetime.datetime.now().replace(second=0)


def from_cassandra():
    query = f"SELECT * from reports;"
    return list(session.execute(query))


def get_a1(df):
    fin = []
    for h in range(1, 7):
        final_dict = {'time_start': datetime.datetime.strftime(current_hour() - datetime.timedelta(hours=h + 1),
                                                               "%Y-%m-%d %H:%M:%S"),
                      'time_end': datetime.datetime.strftime(current_hour() - datetime.timedelta(hours=h),
                                                             "%Y-%m-%d %H:%M:%S"),
                      'statistics': []}
        for_one_hour = df.filter(df['report_time'] == datetime.datetime.strftime(current_hour() -
                                                                                 datetime.timedelta(hours=h + 1),
                                                                                 "%Y-%m-%d %H:%M:%S")) \
            .groupBy(df['domain_']).count().collect()
        for row in for_one_hour:
            final_dict['statistics'].append({row['domain_']: row['count']})
        # fin.append(str(final_dict.items()))
        f = str(final_dict).replace("'", '"')
        qr = f"INSERT INTO a1 (id, entry) VALUES ({h},'{f}')"
        session.execute(qr)


def get_a2(df):
    fin = []
    for h in range(1, 7):
        final_dict = {'time_start': datetime.datetime.strftime(current_hour() - datetime.timedelta(hours=h + 1),
                                                               "%Y-%m-%d %H:%M:%S"),
                      'time_end': datetime.datetime.strftime(current_hour() - datetime.timedelta(hours=h),
                                                             "%Y-%m-%d %H:%M:%S"),
                      'statistics': []}
        for_one_hour = df.filter((df['report_time'] == datetime.datetime.strftime(current_hour() -
                                                                                  datetime.timedelta(hours=h + 1),
                                                                                  "%Y-%m-%d %H:%M:%S")) & (
                                             df['user_is_bot'] == 'True')) \
            .groupBy(df['domain_']).count().collect()
        for row in for_one_hour:
            final_dict['statistics'].append({'domain': row['domain_'], 'created_by_bots': row['count']})
        f = str(final_dict).replace("'", '"')
        qr = f"INSERT INTO a2 (id, entry) VALUES ({h},'{f}')"
        session.execute(qr)


def get_a3(df):
    for h in range(1, 7):
        if h == 1:
            for_6_hours = df.filter((df['report_time'] == datetime.datetime.strftime(current_hour() -
                                                                                     datetime.timedelta(hours=h + 1),
                                                                                     "%Y-%m-%d %H:%M:%S")))
        else:
            for_one_hour = df.filter((df['report_time'] == datetime.datetime.strftime(current_hour() -
                                                                                      datetime.timedelta(hours=h + 1),
                                                                                      "%Y-%m-%d %H:%M:%S")))

            for_6_hours = for_6_hours.union(for_one_hour)

    top_20 = for_6_hours.groupby('user_id', 'user_text').agg(f.count('user_id').alias('pages_created')).sort(
        f.desc('pages_created')).head(20)
    df = spark.createDataFrame(top_20)
    df_2 = df.alias('df').join(for_6_hours, ['user_id'], 'left').groupby('df.user_id', 'df.user_text', 'pages_created') \
        .agg(f.collect_list('page_title').alias('page_titles')) \
        .sort(f.desc('pages_created'))

    final_dict = {'time_start': datetime.datetime.strftime(current_hour() - datetime.timedelta(hours=7),
                                                           "%Y-%m-%d %H:%M:%S"),
                  'time_end': datetime.datetime.strftime(current_hour() - datetime.timedelta(hours=1),
                                                         "%Y-%m-%d %H:%M:%S"),
                  'statistics': df_2.rdd.map(lambda row: row.asDict()).collect()}

    fin = str(final_dict).replace("'", '"')
    qr = f"INSERT INTO a3 (id, entry) VALUES (0,'{fin}')"
    session.execute(qr)


def main():
    df = spark.createDataFrame(from_cassandra())
    print('new minute')
    get_a1(df)
    get_a2(df)
    get_a3(df)


if __name__ == '__main__':
    starting_hour =datetime.datetime.now().hour
    while True:
        timestamp = datetime.datetime.now().hour
        if timestamp != starting_hour:
            print(timestamp)
            main()
            starting_hour = timestamp



