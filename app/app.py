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
		session.execute("CREATE TABLE IF NOT EXISTS tf (term TEXT, doc TEXT, freq INT, PRIMARY KEY (term, doc));")
		session.execute("CREATE TABLE IF NOT EXISTS df (term TEXT, freq INT, PRIMARY KEY (term));")
		session.execute("CREATE TABLE IF NOT EXISTS doc_stat (doc TEXT, terms INT, PRIMARY KEY (doc));")
	except Exception as _:
		session.shutdown()
		cluster.shutdown()


if __name__ == "__main__":
	main()
