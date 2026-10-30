"""Build-time download only, verified against the prepared binary checksum."""
import hashlib
from pathlib import Path
from urllib.request import urlopen

url = "https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar"
with urlopen(url, timeout=60) as response:
    body = response.read()
assert hashlib.sha256(body).hexdigest() == "73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c", "JDBC checksum mismatch"
Path("/opt/spark/jars/postgresql-42.7.14.jar").write_bytes(body)
