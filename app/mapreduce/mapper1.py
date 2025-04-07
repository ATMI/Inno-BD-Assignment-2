import sys
from typing import Generator


def tokenize(line: str) -> Generator[str, None, None]:
	yield from line.split()


def main() -> None:
	for line in sys.stdin:
		line = line.strip()
		doc, text = line.split("\t", 1)

		for term in tokenize(text):
			print(f"{term}\t{doc}\t1")


if __name__ == "__main__":
	main()
