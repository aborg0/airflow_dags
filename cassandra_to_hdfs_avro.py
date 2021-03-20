from typing import List, Tuple
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.models.connection import Connection
from airflow.providers.apache.cassandra.sensors.table import CassandraTableSensor
from cassandra.cluster import Cluster, ResultSet, Session
from cassandra.auth import PlainTextAuthProvider

# from hdfs import Client, InsecureClient
# from hdfs.ext.avro import AvroWriter

args = {
    'owner': 'Airflow',
    'cassandra_connection': 'local_cassandra',
    'hdfs_connection': 'local_hdfs',
}

@dag(default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['cassandra', 'hdfs'],)
def cassandra_to_avro():
    @task
    def load_from_cassandra() -> List[Tuple[str, str]]:
        conn: Connection = Connection.get_connection_from_secrets(get_current_context()['cassandra_connection'])
        auth_provider = PlainTextAuthProvider(username=conn.login, password=conn.password)
        cluster: Cluster = Cluster([conn.host], conn.port, auth_provider=auth_provider)
        session: Session = cluster.connect(conn.schema)
        rows: ResultSet = session.execute("SELECT title, description FROM videos")
        return list(map(lambda row: (row[0], row[1]), rows))
    
    # @task
    # def write_to_hdfs(rows: List[Tuple[str, str]]):
    #     conn: Connection = Connection.get_connection_from_secrets(get_current_context()['hdfs_connection'])
    #     client = InsecureClient(conn.get_uri, user=conn.login)
            
    #     with AvroWriter(client, 'videos.avro', schema={
    #         'type':'record',
    #         'name':'video',
    #         'fields': [
    #             {'type': 'string', 'name': 'title'},
    #             {'type': 'string', 'name': 'description'},
    #         ]
    #     }) as writer:
    #         for row in rows:
    #             writer.write(row)
        
    ctx = get_current_context()
    table_sensor = CassandraTableSensor(
        task_id="cassandra_table_sensor",
        cassandra_conn_id=ctx['cassandra_connection'],
        table="killrvideo.videos",
    )

    load = load_from_cassandra()
    # write_to_hdfs(load)
    table_sensor >> load

cassandra_to_avro_dag = cassandra_to_avro()