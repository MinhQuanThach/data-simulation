"""Generate ignored local secrets once; never print secret values."""
import base64
from pathlib import Path
import secrets
import sys

root = Path(__file__).resolve().parents[1]
target = root / ".env"
if target.exists():
    print(".env already exists; preserved. Edit locally if needed.")
    sys.exit(0)
lines = []
for line in (root / ".env.example").read_text(encoding="utf-8").splitlines():
    if line and not line.startswith("#"):
        key, value = line.split("=", 1)
        if value == "CHANGE_ME":
            value = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode() if key == "AIRFLOW_FERNET_KEY" else secrets.token_hex(32)
            line = key + "=" + value
    lines.append(line)
with target.open("x", encoding="utf-8", newline="\n") as stream:
    stream.write("\n".join(lines) + "\n")
print("Created .env with random local secrets; values withheld.")
