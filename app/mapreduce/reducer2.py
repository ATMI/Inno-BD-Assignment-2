import os
import sys

sys.path.append(os.getcwd())
import cql

# INSERT_TF = "UPDATE tf SET docs = docs + ?, total = total + ? WHERE term = ?;"
INSERT_DL = "INSERT INTO dl (doc, len) VALUES (?, ?);"
INSERT_TF = "INSERT INTO tf (term, docs, total) VALUES (?, ?, ?);"


def main() -> None:
	cluster, session = cql.connect()
	session.set_keyspace("index_keyspace")

	dl_batch = cql.BatchInserter(session, INSERT_DL)
	tf_batch = cql.BatchInserter(session, INSERT_TF)

	curr_key = None
	curr_sum = 0
	curr_num = 0

	def store() -> None:
		key, tag = curr_key.rsplit(":")
		if tag == "dl":
			dl_batch.add((key, curr_sum))
		elif tag == "tf":
			tf_batch.add((key, curr_num, curr_sum))
		else:
			raise ValueError(f"Unknown tag {tag}")

	for line in sys.stdin:
		line = line.strip()
		key, value = line.rsplit("\t")

		if curr_key != key and curr_key:
			store()
			curr_sum = 0
			curr_num = 0

		curr_key = key
		curr_sum += int(value)
		curr_num += 1

	if curr_key:
		store()

	dl_batch.close()
	tf_batch.close()
	session.shutdown()
	cluster.shutdown()


if __name__ == "__main__":
	try:
		main()
	except Exception as e:
		print(e)
