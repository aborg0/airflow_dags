from typing import List, Tuple
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.models.connection import Connection
from airflow.providers.apache.cassandra.sensors.table import CassandraTableSensor
from cassandra.cluster import Cluster, ResultSet, Session
from cassandra.auth import PlainTextAuthProvider

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from hdfs import Client, InsecureClient
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
        conn: Connection = Connection.get_connection_from_secrets('local_cassandra')
        auth_provider = PlainTextAuthProvider(username=conn.login, password=conn.password)
        cluster: Cluster = Cluster([conn.host], conn.port, auth_provider=auth_provider)
        session: Session = cluster.connect(conn.schema)
        rows: ResultSet = session.execute("SELECT title, description FROM videos")
        result = list(map(lambda row: (row[0], row[1]), rows))
        print(result)
        return result
    
    @task
    def write_to_hdfs(rows: List[Tuple[str, str]]):
        conn: Connection = Connection.get_connection_from_secrets(get_current_context()['hdfs_connection'])
        client = InsecureClient(conn.get_uri, user=conn.login)
            
        sch = avro.schema.make_avsc_object({
            'type':'record',
            'name':'Video',
            'fields': [
                {'type': {'type': 'string', 'avro.java.string': 'String'}, 'name': 'title'},
                {'type': ["null", {'type': 'string', 'avro.java.string': 'String'}], 'name': 'description'},
            ]
        })
        local_file_name = 'videos.avro'
        writer = DataFileWriter(open(local_file_name, "wb"), DatumWriter(), sch)
        for row in rows:
            print(row)
            writer.append({"title":row[0], "description":row[1]})
        writer.close()
        client.upload('/tmp/videos.avro', local_file_name)
        
    # ctx = get_current_context()
    table_sensor = CassandraTableSensor(
        task_id="cassandra_table_sensor",
        cassandra_conn_id='local_cassandra',
        table="killrvideo.videos",
    )

    load = load_from_cassandra()
    write_to_hdfs(load)
    table_sensor >> load

cassandra_to_avro_dag = cassandra_to_avro()