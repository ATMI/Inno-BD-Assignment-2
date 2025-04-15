import os
import sys

sys.path.append(os.getcwd())
import tok

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, log, lit, avg, count, desc


def session() -> SparkSession:
	spark = SparkSession.builder \
		.appName("BM25 Search") \
		.config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
		.config("spark.cassandra.connection.host", "cassandra-server") \
		.config("spark.cassandra.connection.port", "9042") \
		.config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
		.getOrCreate()
	return spark


def main():
	spark = session()
	tokenizer = tok.Tokenizer()

	query = sys.argv[1]
	query = tokenizer(query)
	query = list(query)

	cassandra = "org.apache.spark.sql.cassandra"
	keyspace = "index_keyspace"

	tf = spark.read \
		.format(cassandra) \
		.options(table="tf", keyspace=keyspace) \
		.load() \
		.filter(col("term").isin(query)) \
		.alias("tf")

	tdf = spark.read \
		.format(cassandra) \
		.options(table="tdf", keyspace=keyspace) \
		.load() \
		.alias("tdf")

	dl = spark.read \
		.format(cassandra) \
		.options(table="dl", keyspace=keyspace) \
		.load() \
		.alias("dl")

	index = dl \
		.join(tdf, "doc") \
		.join(tf, "term")

	agg = dl.agg(avg("len"), count("*")).first()
	avgdl, n = agg
	k = 1.2
	b = 0.75

	tf = (col("freq") * (k + 1)) / (col("freq") + k * (1 - b + b * n / avgdl))
	idf = log(1 + (n - col("docs") + 0.5) / (col("docs") + 0.5))
	index = index \
		.select(col("term"), col("doc"), (tf * idf).alias("score")) \
		.groupby("doc") \
		.agg(sum("score").alias("score")) \
		.orderBy(desc("score"))

	index.show(10)
	spark.stop()

if __name__ == "__main__":
	main()
