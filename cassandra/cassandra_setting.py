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
        messageID text PRIMARY KEY,
        messageURL text,
        messageIssueTime timestamp,
        messageBody text
    ) WITH compression={'sstable_compression': 'SnappyCompressor'};
    ''',
    '''
    CREATE TABLE IF NOT EXISTS solar_data (
        flrID text PRIMARY KEY,
        classType text,
        classType_encoded int,
        sourceLocation text,
        activeRegionNum int,
        duration int,
        peak_time timestamp
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
