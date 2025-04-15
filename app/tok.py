import re
from typing import Generator


class Tokenizer:
	def __init__(self) -> None:
		self.pattern = re.compile(r"\w+")

	def __call__(self, text: str) -> Generator[str, None, None]:
		yield from (m.group().lower() for m in self.pattern.finditer(text))
