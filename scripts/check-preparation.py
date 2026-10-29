"""Offline checks of preparation artifacts, not a pipeline/E2E test."""

from collections import Counter
import hashlib
import json
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]


def require(condition, message):
    if not condition:
        raise SystemExit(f"FAIL: {message}")


def main():
    required = [
        "README.md", "CONTEXT.md", "PLAN.md", "STATUS.md", ".env.example",
        ".gitignore", ".gitattributes", "docs/environment.md", "docs/versions.md",
        "docs/architecture.md", "docs/data_dictionary.md", "scripts/preflight.ps1",
        "config/quality_rules.yaml", "config/datasets.yaml", "config/stack.json",
        "requirements/airflow.in", "requirements/constraints-upstream-py311.txt",
        "requirements/constraints-project-py311.txt", "docs/evidence/sources.json",
    ]
    for name in required:
        require((ROOT / name).is_file(), f"missing {name}")

    stack = json.loads((ROOT / "config/stack.json").read_text(encoding="utf-8-sig"))
    for name, spec in stack["images"].items():
        require(not spec["tag"].endswith(":latest"), f"mutable tag: {name}")
        require(re.fullmatch(r"sha256:[0-9a-f]{64}", spec["digest"]), f"digest: {name}")
        evidence = json.loads((ROOT / f"docs/evidence/manifest-{name}.json").read_text(encoding="utf-8-sig"))
        require(any(m["digest"] == spec["digest"] and m.get("platform", {}).get("os") == "linux"
                    and m["platform"].get("architecture") == "amd64"
                    for m in evidence["manifests"]), f"amd64 evidence mismatch: {name}")

    sources = json.loads((ROOT / "docs/evidence/sources.json").read_text(encoding="utf-8-sig"))
    for entry in sources["files"]:
        actual = hashlib.sha256((ROOT / entry["path"]).read_bytes()).hexdigest()
        require(actual == entry["sha256"], f"source checksum mismatch: {entry['path']}")

    def pins(path):
        return dict(re.findall(r"^([A-Za-z0-9_.-]+)==([^\s;]+)$", path.read_text(encoding="utf-8-sig"), re.M))

    direct = pins(ROOT / "requirements/airflow.in")
    constraints = pins(ROOT / "requirements/constraints-project-py311.txt")
    for package, version in direct.items():
        if package in constraints:
            require(constraints[package] == version, f"conflicting pin: {package}")
    require(direct["apache-airflow"] == stack["airflow_version"], "Airflow pin mismatch")
    require(direct["pyspark"] == stack["spark_version"], "Spark pin mismatch")
    require(direct["apache-airflow-providers-apache-spark"] == stack["spark_provider_version"], "provider pin mismatch")

    expected = json.loads((ROOT / "fixtures/expected.json").read_text(encoding="utf-8"))
    valid = expected["valid_vi"]
    require((ROOT / "fixtures" / valid["file"]).is_file(), "valid fixture missing")
    require(Counter(valid["tokens"]) == valid["word_counts"], "manual tokens/counts disagree")
    require(sum(valid["word_counts"].values()) == valid["token_count"] == 12, "token total")
    top = sorted(valid["word_counts"].items(), key=lambda pair: (-pair[1], pair[0].encode("utf-8")))[:3]
    require(valid["top_3"] == [{"word": word, "count": count} for word, count in top], "top-N ordering")
    for case in expected["invalid_cases"]:
        require((ROOT / "fixtures" / case.get("file", case.get("reuse_file"))).is_file(), "invalid fixture missing")
    for case in expected["run_cases"]:
        require(case["input"] == case["valid"] + case["rejected"], f"oracle invariant: {case['name']}")

    for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        if any(part in key for part in ("PASSWORD", "SECRET", "FERNET")):
            require(value == "CHANGE_ME", f"example should contain placeholder: {key}")

    print("PASS: required artifacts, image platform/digest evidence, source checksums, pins, manual fixture oracle, secret placeholders")
    print("NOT TESTED: Linux dependency resolution, image pull/build, JDBC, Airflow DAG, Spark executors, web crawl, publish/retry")


if __name__ == "__main__":
    main()
