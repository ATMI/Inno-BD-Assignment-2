import sys


def store_df(term: str, df: int) -> None:
	print("DF", term, df)


def store_len(doc: str, length: int) -> None:
	print("LN", doc, length)


def store(key: str, count: int) -> None:
	key, task = key.rsplit(":")
	match task:
		case "df":
			store_df(key, count)
		case "len":
			store_len(key, count)


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
