from typing import Tuple, Any

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

MAX_BATCH = 256

def connect() -> Tuple[Any, Any]:
	cluster = Cluster(["cassandra-server"])
	session = cluster.connect()
	return cluster, session


class BatchInserter:
	def __init__(self, session, max_size: int = MAX_BATCH):
		self.session = session
		self.max_size = max_size
		self.batch = None
		self.size = 0

	def execute(self) -> None:
		if self.batch is None:
			return

		self.session.execute(self.batch)
		self.batch = None
		self.size = 0

	def add(self, statement: str, values: Tuple[Any, ...]) -> None:
		self.batch = self.batch or BatchStatement()
		self.batch.add(statement, values)
		self.size += 1
		if self.size >= self.max_size:
			self.execute()

	def close(self) -> None:
		self.execute()
