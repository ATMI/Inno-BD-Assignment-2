import os
import sys

sys.path.append(os.getcwd())
import cql

INSERT = "INSERT INTO tf (term, doc, freq) VALUES (?, ?, ?);"


def main() -> None:
	cluster, session = cql.connect()
	session.set_keyspace("index_keyspace")
	batch = cql.BatchInserter(session, INSERT)

	curr_doc, curr_term = None, None
	curr_tf = 0

	def store() -> None:
		print(f"{curr_term}\t{curr_doc}\t{curr_tf}")
		batch.add((curr_term, curr_doc, curr_tf))

	for line in sys.stdin:
		line = line.strip()

		key, value = line.rsplit("\t", 1)
		term, doc = key.split("\t", 1)

		if (curr_doc != doc or curr_term != term) and curr_term:
			store()
			curr_tf = 0

		curr_term = term
		curr_doc = doc
		curr_tf += int(value)

	if curr_term:
		store()

	batch.close()
	session.shutdown()
	cluster.shutdown()


if __name__ == "__main__":
	main()
