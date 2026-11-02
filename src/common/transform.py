"""Transform 1.0.0: body text; Unicode letters/decimal digits; no stopwords.

These pure functions run inside Spark Python UDFs on the executor.
Extraction/tokenization settings are validated by the versioned rules reader.
"""
import unicodedata

from bs4 import BeautifulSoup

TRANSFORM_VERSION = "1.0.0"
RULES_VERSION = "1.1.0"


def normalize_space(text):
    return " ".join(unicodedata.normalize("NFC", text).split())


def extract_html(raw_html):
    soup = BeautifulSoup(raw_html or "", "html.parser")
    title = normalize_space(soup.title.get_text(" ")) if soup.title else ""
    for element in soup.find_all(["script", "style", "noscript", "template"]):
        element.decompose()
    if soup.body is None:
        return title, "", "missing_body"
    text = normalize_space(soup.body.get_text(" ", strip=True))
    return title, text, "" if text else "empty_clean_text"


def tokenize(text):
    normalized = unicodedata.normalize("NFC", text.lower())
    tokens, current = [], []
    for char in normalized:
        category = unicodedata.category(char)
        if category.startswith("L") or category == "Nd":
            current.append(char)
        elif current:
            tokens.append("".join(current))
            current = []
    if current:
        tokens.append("".join(current))
    return tokens
