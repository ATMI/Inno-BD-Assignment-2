import os
import sys

sys.path.append(os.getcwd())
import cql

INSERT_DF = "INSERT INTO df (term, freq) VALUES (?, ?);"
INSERT_TF = "INSERT INTO doc_stat (doc, terms) VALUES (?, ?);"


def main() -> None:
	cluster, session = cql.connect()
	session.set_keyspace("index_keyspace")
	batch = {
		"df": cql.BatchInserter(session, INSERT_DF),
		"tf": cql.BatchInserter(session, INSERT_TF),
	}

	curr_key = None
	curr_count = 0

	def store() -> None:
		key, tag = curr_key.rsplit(":")
		batch[tag].add((key, curr_count))

	for line in sys.stdin:
		line = line.strip()
		key, value = line.rsplit("\t")

		if curr_key != key and curr_key:
			store()
			curr_count = 0

		curr_key = key
		curr_count += int(value)

	if curr_key:
		store()

	for b in batch.values():
		b.close()
	session.shutdown()
	cluster.shutdown()


if __name__ == "__main__":
	main()
