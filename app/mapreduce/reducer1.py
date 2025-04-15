import sys


def store(term: str, doc: str, tf: int) -> None:
	print(f"{term}\t{doc}\t{tf}")


def main() -> None:
	curr_doc, curr_term = None, None
	curr_tf = 0

	for line in sys.stdin:
		line = line.strip()

		key, value = line.rsplit("\t", 1)
		term, doc = key.split("\t", 1)

		if (curr_doc != doc or curr_term != term) and curr_term:
			store(curr_term, curr_doc, curr_tf)
			curr_tf = 0

		curr_term = term
		curr_doc = doc
		curr_tf += int(value)

	if curr_term:
		store(curr_term, curr_doc, curr_tf)


if __name__ == "__main__":
	main()
