import os
import sys

sys.path.append(os.getcwd())
import tok


def main() -> None:
	tokenizer = tok.Tokenizer()
	doc = os.environ.get("map_input_file", "unknown")
	doc = os.path.basename(doc)

	for line in sys.stdin:
		line = line.strip()
		for term in tokenizer(line):
			print(f"{term}\t{doc}\t1")


if __name__ == "__main__":
	try:
		main()
	except Exception as e:
		print(e)
