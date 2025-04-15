import cql


def main():
	print("Hello app")

	# Connects to the cassandra server

	# displays the available keyspaces
	cluster, session = cql.connect()
	rows = session.execute("DESC keyspaces;")
	for row in rows:
		print(row)

	try:
		session.execute(
			"""
			CREATE KEYSPACE IF NOT EXISTS index_keyspace 
			WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 };
			"""
		)
		session.set_keyspace("index_keyspace")
		session.execute("CREATE TABLE IF NOT EXISTS tdf (term TEXT, doc TEXT, freq INT, PRIMARY KEY (term, doc));")
		session.execute("CREATE TABLE IF NOT EXISTS dl (doc TEXT, len INT, PRIMARY KEY (doc));")
		session.execute("CREATE TABLE IF NOT EXISTS tf (term TEXT, docs INT, total INT, PRIMARY KEY (term));")
		# session.execute("CREATE TABLE IF NOT EXISTS tf (term TEXT, docs COUNTER, total COUNTER, PRIMARY KEY (term));")
	except Exception as _:
		session.shutdown()
		cluster.shutdown()


if __name__ == "__main__":
	main()
