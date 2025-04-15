import os
import re
import sys
from typing import Generator


class Tokenizer:
	def __init__(self) -> None:
		self.pattern = re.compile(r"\w+")

	def __call__(self, text: str) -> Generator[str, None, None]:
		yield from (m.group() for m in self.pattern.finditer(text))


def main() -> None:
	tokenizer = Tokenizer()
	doc = os.environ.get("map_input_file", "unknown")
	doc = os.path.basename(doc)

	for line in sys.stdin:
		line = line.strip()
		for term in tokenizer(line):
			print(f"{term}\t{doc}\t1")


if __name__ == "__main__":
	main()
