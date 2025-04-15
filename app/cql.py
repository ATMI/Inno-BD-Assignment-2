from typing import Tuple, Any

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

MAX_BATCH = 32

def connect() -> Tuple[Any, Any]:
	cluster = Cluster(["cassandra-server"])
	session = cluster.connect()
	return cluster, session


class BatchInserter:
	def __init__(self, session, query: str, max_size: int = MAX_BATCH):
		self.max_size = max_size
		self.session = session
		self.query = session.prepare(query)
		self.batch = None
		self.size = 0

	def execute(self) -> None:
		if self.batch is None:
			return

		self.session.execute(self.batch)
		self.batch = None
		self.size = 0

	def add(self, values: Tuple[Any, ...]) -> None:
		self.batch = self.batch or BatchStatement()
		self.batch.add(self.query, values)
		self.size += 1
		if self.size >= self.max_size:
			self.execute()

	def close(self) -> None:
		self.execute()
