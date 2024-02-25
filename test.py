from yaml import load, FullLoader
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

with open('config.yml', 'r') as f:
    config = load(f, Loader=FullLoader)

auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')

cluster = Cluster([config['cassandra']['IP']], auth_provider=auth_provider)
session = cluster.connect()

session.execute('USE nasa_project;')
query='SELECT DISTINCT CAST(messageID AS TEXT) from notifications;'
message_ids = [row.messageid for row in session.execute(query=query)]
print(message_ids)