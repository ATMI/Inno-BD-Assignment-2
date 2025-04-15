import sys


def store_df(term: str, df: int) -> None:
	print("DF", term, df)


def store_tf(doc: str, length: int) -> None:
	print("TF", doc, length)


def store(key: str, count: int) -> None:
	key, tag = key.rsplit(":")
	if tag == "df":
		store_df(key, count)
	elif tag == "tf":
		store_tf(key, count)


def main() -> None:
	curr_key = None
	curr_count = 0

	for line in sys.stdin:
		line = line.strip()
		key, value = line.rsplit("\t")

		if curr_key != key and curr_key:
			store(curr_key, curr_count)
			curr_count = 0

		curr_key = key
		curr_count += int(value)

	if curr_key:
		store(curr_key, curr_count)


if __name__ == "__main__":
	main()
