import pyspark
import pyspark.sql
import avro

# sc = pyspark.SparkContext("local[*]")
# spark = pyspark.sql.SparkSession(sc)
# https://stackoverflow.com/a/57908610
spark = pyspark.sql.SparkSession.builder.master("local").config("spark.cassandra.auth.username", "cassandra")

# https://stackoverflow.com/a/46675457
hosts = {"spark.cassandra.connection.host": 'host.docker.internal'}
df = spark.read.format("org.apache.spark.sql.cassandra").options(**hosts).load(keyspace="killrvideo", table="videos")
# df = spark.read.csv("videos.csv", header=True)
# sch = avro.schema.make_avsc_object({
#     'type':'record', 
#     'name':'Video',
#     'fields': [
#         {'type': {'type': 'string', 'avro.java.string': 'String'}, 'name': 'title'},
#         {'type': ["null", {'type': 'string', 'avro.java.string': 'String'}], 'name': 'description'},
#     ]
# })
local_file_name = 'videos.avro'
df.select("title", "description").write.format("avro").save(local_file_name)
