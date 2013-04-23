from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated
from hive_service import ThriftHive


def run_query(q):
    socket = TSocket.TSocket("ec2-107-20-75-29.compute-1.amazonaws.com", 10000)
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocolAccelerated(transport)
    client = ThriftHive.Client(protocol)
    transport.open()
    client.execute(q)
    rows = client.fetchAll()
    transport.close()
    return [r.split('\t') for r in rows]

#
# create_table = """CREATE EXTERNAL TABLE queries (ts STRING,u STRING,ip STRING,q STRING, k STRING)
#                ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION 's3n://spark-staq01/queries/'
#                """
# print run_query(create_table)

# print run_query('DROP TABLE queries_cached')
# print run_query("""CREATE TABLE queries_cached TBLPROPERTIES ("shark.cache" = "true") AS SELECT * FROM queries""")
#
# for table in run_query('SHOW TABLES'):
#     print (table, run_query('SELECT COUNT(*) FROM %s' % table[0])[0])

q = """
    SELECT COUNT(*) FROM queries_cached
    """

for r in run_query(q):
    print r
