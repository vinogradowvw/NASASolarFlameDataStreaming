from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from yaml import load, FullLoader

with open('config.yml', 'r') as f:
    config = load(f, Loader=FullLoader)

auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')

cluster = Cluster([config['cassandra']['IP']], auth_provider=auth_provider)
session = cluster.connect()

queries = [
    '''
    CREATE KEYSPACE IF NOT EXISTS nasa_project WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};
    ''',
    '''
    USE nasa_project;
    ''',
    '''
    CREATE TABLE IF NOT EXISTS notifications (
        message_id text PRIMARY KEY,
        message_url text,
        message_issue_time timestamp,
        message_body text
    ) WITH compression={'sstable_compression': 'SnappyCompressor'};
    ''',
    '''
    CREATE TABLE IF NOT EXISTS solar_data (
        flr_iD text PRIMARY KEY,
        begin_time timestamp,
        end_time timestamp,
        peak_time timestamp,
        class_letter text,
        class_type text,
        class_type_encoded float,
        source_location text,
        link text,
        active_region_num int,
        duration int
    ) WITH compression={'sstable_compression': 'SnappyCompressor'};
    '''
]

for query in queries:
    try:
        session.execute(query=query)
    except Exception as e:
        print(f'Error executing query: {e}')

session.shutdown()
cluster.shutdown()
