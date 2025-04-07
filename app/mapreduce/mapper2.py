import sys


def main() -> None:
	for line in sys.stdin:
		line = line.strip()
		term, doc, tf = line.rsplit("\t", 2)

		print(f"{term}:df\t1")
		print(f"{doc}:len\t{tf}")


if __name__ == "__main__":
	main()
