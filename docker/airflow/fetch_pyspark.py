"""Fetch the exact official sdist in bounded ranges; verify the entire SHA-256.

Large single-stream downloads were throttled on the local Docker network.
Ranges only change transport. pip still builds/installs the original full sdist.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
from pathlib import Path
from threading import Lock
import time
from urllib.request import Request, urlopen

URL = "https://files.pythonhosted.org/packages/96/89/408b42c803db71f4a4d8a3f1ab0745a40dfe41aeacdfc453545665a171f4/pyspark-4.0.2.tar.gz"
SIZE = 434209940
SHA256 = "938b4a1883383374d331ebfcb5d92debfa1891cf3d7a6d730520a1a2d23f1a90"
CHUNK = 1024 * 1024
target = Path("/tmp/pyspark-4.0.2.tar.gz")
lock = Lock()

with target.open("w+b") as stream:
    stream.truncate(SIZE)

    def fetch(start):
        end = min(start + CHUNK, SIZE) - 1
        for attempt in range(3):
            try:
                request = Request(URL, headers={"Range": f"bytes={start}-{end}"})
                with urlopen(request, timeout=60) as response:
                    if response.status != 206 or response.headers.get("Content-Range") != f"bytes {start}-{end}/{SIZE}":
                        raise RuntimeError("Server did not honor the exact artifact range")
                    data = response.read(end - start + 2)
                if len(data) != end - start + 1:
                    raise RuntimeError("Truncated PySpark source range")
                with lock:
                    stream.seek(start)
                    stream.write(data)
                return
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(attempt + 1)

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(fetch, start) for start in range(0, SIZE, CHUNK)]
        for completed, future in enumerate(as_completed(futures), 1):
            future.result()
            if completed % 32 == 0 or completed == len(futures):
                print(f"PySpark source: {completed}/{len(futures)} verified-size ranges", flush=True)

with target.open("rb") as stream:
    if hashlib.file_digest(stream, "sha256").hexdigest() != SHA256:
        target.unlink()
        raise RuntimeError("Full PySpark source SHA-256 mismatch")
print(f"PySpark 4.0.2 source SHA-256 verified: {SHA256}", flush=True)
