import sys


def main() -> None:
	for line in sys.stdin:
		line = line.strip()
		term, doc, tf = line.rsplit("\t", 2)

		print(f"{term}:tf\t{tf}")
		print(f"{doc}:dl\t{tf}")


if __name__ == "__main__":
	try:
		main()
	except Exception as e:
		print(e)
