"""Small immutable DAG configuration; URLs retain path and query ordering."""
import hashlib
from urllib.parse import urldefrag, urlsplit

FIXTURE_CASES = ('valid_vi', 'empty', 'script_only', 'mixed', 'all_invalid', 'http_error', 'missing_body', 'no_tokens', 'unsupported_type')


def normalize_url(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Each URL must be a nonempty HTTP(S) string")
    value = value.strip()
    if len(value) > 4096 or any(ord(c) < 33 or ord(c) == 127 for c in value) or "\\" in value:
        raise ValueError("URL contains whitespace/control characters or exceeds 4096 characters")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise ValueError("Invalid URL host/port") from None
    if (parsed.scheme.lower() not in ("http", "https") or not parsed.hostname
            or parsed.username is not None or parsed.password is not None or port == 0):
        raise ValueError("URL requires HTTP(S), hostname and no credentials")
    return urldefrag(value)[0]


def page_id(url):
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def validate_input(conf):
    if not isinstance(conf, dict):
        raise ValueError("DAG conf must be an object")
    allowed = {"urls", "top_n", "fixture_case", "test_fail_after_documents_once", "test_fail_after_documents_always"}
    if set(conf) - allowed:
        raise ValueError("Unknown configuration key")
    top_n = conf.get("top_n", 3)
    fail_once = conf.get("test_fail_after_documents_once", False)
    fail_always = conf.get('test_fail_after_documents_always', False)
    if type(top_n) is not int or not 1 <= top_n <= 100:
        raise ValueError("top_n must be an integer in 1..100")
    if type(fail_once) is not bool:
        raise ValueError("test_fail_after_documents_once must be boolean")
    if type(fail_always) is not bool or (fail_once and fail_always):
        raise ValueError('Invalid failure injection configuration')
    if "urls" not in conf:
        case = conf.get("fixture_case", "valid_vi")
        if case not in FIXTURE_CASES:
            raise ValueError('Unsupported fixture_case')
        return {"mode": "fixture", "fixture_case": case, "top_n": top_n, "fail_once": fail_once, 'fail_always': fail_always}
    if "fixture_case" in conf:
        raise ValueError("Use urls or fixture_case in a run")
    urls = conf["urls"]
    if not isinstance(urls, list) or not 1 <= len(urls) <= 5:
        raise ValueError("urls must be a list containing 1..5 URLs before deduplication")
    unique = list(dict.fromkeys(normalize_url(url) for url in urls))
    return {"mode": "web", "urls": unique, "requested_count": len(urls),
            "unique_input_count": len(unique), "top_n": top_n, "fail_once": fail_once, 'fail_always': fail_always}
