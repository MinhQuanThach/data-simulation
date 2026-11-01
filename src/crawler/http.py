"""Depth-zero crawler. Requests are streamed, bounded, rate-limited and audited.

No automatic redirects/retries: every redirect target passes robots first.
Known source failures become Bronze reasons. Unexpected program errors propagate.
"""
import codecs
from datetime import datetime, timezone
import hashlib
import re
import time
from urllib.parse import quote, urljoin, urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

from bs4 import UnicodeDammit
import requests

from src.crawler.config import normalize_url, page_id

VERSION = "1.0.0"
AGENT = "BronzeSilverLab"
USER_AGENT = AGENT + "/1.0 (educational static HTML lab)"
REDIRECTS = {301, 302, 303, 307, 308}
TRANSIENT = {429, 500, 502, 503, 504}


class SourceError(Exception):
    """Safe reason only; never persist arbitrary HTTP exception messages."""


def _rep_path(value):
    # REP compares UTF-8 octets; decode only percent-encoded unreserved ASCII.
    value = quote(value, safe="/%?=&:;+,$!*'()@-._~")
    return re.sub(r"%([0-9a-fA-F]{2})", lambda m: (
        chr(int(m[1], 16)) if chr(int(m[1], 16)) in
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
        else "%" + m[1].upper()), value)


def robots_allowed(text, url):
    """REP groups, longest matching agent/rule, wildcard, end marker and allow tie."""
    groups, agents, rules, has_records = [], [], [], False
    for line in text.splitlines() + ["User-agent: __end__"]:
        key, sep, value = line.split("#", 1)[0].partition(":")
        if not sep:
            continue
        key, value = key.strip().lower(), value.strip()
        if key == "user-agent":
            if has_records:
                groups.append((agents, rules))
                agents, rules = [], []
                has_records = False
            agents.append(value.lower())
        elif agents:
            has_records = True
            if key in ("allow", "disallow"):
                rules.append((key, value))
    matches = [(max((len(a) for a in agents if a != "*" and a in AGENT.lower()), default=0), agents, rules)
               for agents, rules in groups]
    specific = max((size for size, _, _ in matches), default=0)
    path = urlsplit(url).path or "/"
    if urlsplit(url).query:
        path += "?" + urlsplit(url).query
    path = _rep_path(path)
    decisions = []
    for size, agents, rules in matches:
        if (specific and size != specific) or (not specific and "*" not in agents):
            continue
        for key, pattern in rules:
            if not pattern:
                continue
            pattern = _rep_path(pattern)
            terminal = pattern.endswith("$")
            if terminal:
                pattern = pattern[:-1]
            regex = "^" + ".*".join(re.escape(part) for part in pattern.split("*")) + ("$" if terminal else "")
            if re.search(regex, path):
                decisions.append((len(pattern.replace("*", "")), key == "allow"))
    return not decisions or max(decisions)[1]


class Crawler:
    def __init__(self, *, max_bytes=2 * 1024 * 1024, timeout=(5, 15),
                 max_redirects=5, retries=2, interval=1.0, clock=time.monotonic, sleep=time.sleep):
        self.max_bytes, self.timeout = max_bytes, timeout
        self.max_redirects, self.retries, self.interval = max_redirects, retries, interval
        self.clock, self.sleep = clock, sleep
        self.session = requests.Session()
        self.session.trust_env = False  # Public unauthenticated sources; no implicit .netrc credentials.
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml",
                                     "Accept-Encoding": "gzip, deflate"})
        self.last_request, self.delays, self.robots = {}, {}, {}

    def close(self):
        self.session.close()

    def _origin(self, url):
        p = urlsplit(url)
        return urlunsplit((p.scheme.lower(), p.netloc.lower(), "", "", ""))

    def _wait(self, url):
        host = urlsplit(url).hostname.lower()
        delay = max(self.interval, self.delays.get(host, 0))
        if host in self.last_request:
            remaining = delay - (self.clock() - self.last_request[host])
            if remaining > 0:
                self.sleep(remaining)
        self.last_request[host] = self.clock()

    def _request(self, url, history):
        for attempt in range(self.retries + 1):
            self._wait(url)
            trace = {"url": url, "attempt": attempt + 1}
            history.append(trace)
            try:
                response = self.session.get(url, stream=True, allow_redirects=False, timeout=self.timeout)
            except requests.RequestException as error:
                reason = "timeout" if isinstance(error, requests.Timeout) else "network_error"
                trace["error"] = reason
                if attempt < self.retries:
                    self.sleep(2 ** attempt)
                    continue
                raise SourceError(reason) from None
            trace["status"] = response.status_code
            if response.status_code in TRANSIENT and attempt < self.retries:
                retry_after = response.headers.get("Retry-After", "")
                response.close()
                # Never retry earlier than a server's Retry-After. Long/HTTP-date
                # values stop this bounded lab fetch, rather than shortening it.
                if retry_after and (not retry_after.isdigit() or int(retry_after) > 30):
                    raise SourceError("retry_after_exceeds_budget")
                self.sleep(max(2 ** attempt, int(retry_after or 0)))
                continue
            return response

    def _body(self, response):
        parts, size = [], 0
        try:
            for chunk in response.iter_content(chunk_size=16384):
                size += len(chunk)
                if size > self.max_bytes:
                    raise SourceError("response_too_large")
                parts.append(chunk)
        except requests.RequestException as error:
            reason = "timeout" if isinstance(error, requests.Timeout) or "ReadTimeout" in str(type(error.__context__)) else "response_read_error"
            raise SourceError(reason) from None
        return b"".join(parts)

    def _policy(self, url):
        origin = self._origin(url)
        if origin in self.robots:
            return self.robots[origin]
        trace, text, error = [], "", None
        current = origin + "/robots.txt"
        try:
            for hop in range(self.max_redirects + 1):
                with self._request(current, trace) as response:
                    status = response.status_code
                    if status in REDIRECTS:
                        if hop == self.max_redirects:
                            raise SourceError("robots_redirect_limit")
                        current = normalize_url(urljoin(current, response.headers.get("Location", "")))
                        if not response.headers.get("Location"):
                            raise SourceError("robots_invalid_redirect")
                        continue
                    if status in (404, 410):
                        break  # Explicitly absent robots: no restrictions.
                    if status != 200:
                        raise SourceError("robots_http_" + str(status))
                    text = self._body(response).decode("utf-8-sig", errors="replace")
                    parser = RobotFileParser()
                    parser.parse(text.splitlines())
                    delay = parser.crawl_delay(AGENT) or parser.crawl_delay("*") or 0
                    rate = parser.request_rate(AGENT) or parser.request_rate("*")
                    if rate and rate.requests > 0:
                        delay = max(delay, rate.seconds / rate.requests)
                    if delay > 30:
                        raise SourceError("robots_delay_exceeds_budget")
                    host = urlsplit(url).hostname.lower()
                    self.delays[host] = max(self.delays.get(host, 0), delay)
                    break
        except (SourceError, ValueError) as exc:
            error = "robots_unavailable:" + (str(exc) if isinstance(exc, SourceError) else "invalid_redirect")
        result = {"origin": origin, "requests": trace, "text": text, "error": error}
        self.robots[origin] = result
        return result

    def fetch(self, url):
        url = normalize_url(url)
        result = {"page_id": page_id(url), "requested_url": url, "final_url": None,
                  "fetched_at": None, "http_status": None, "content_type": None,
                  "content_hash": None, "raw_html": None, "response_encoding": None,
                  "fetch_error": None, "raw_body": None, "body_bytes": 0,
                  "request_history": [], "robots_history": [], "response_headers": {},
                  "crawler_version": VERSION}
        current = url
        try:
            for hop in range(self.max_redirects + 1):
                policy = self._policy(current)
                result["robots_history"].append(policy)
                if policy["error"]:
                    raise SourceError(policy["error"])
                if not robots_allowed(policy["text"], current):
                    raise SourceError("robots_denied")
                with self._request(current, result["request_history"]) as response:
                    result.update(final_url=response.url, http_status=response.status_code,
                                  content_type=response.headers.get("Content-Type"))
                    result["response_headers"] = {k: v for k, v in response.headers.items()
                                                  if k.lower() in ("content-type", "content-encoding", "etag", "last-modified", "date")}
                    if response.status_code in REDIRECTS:
                        if hop == self.max_redirects:
                            raise SourceError("redirect_limit")
                        location = response.headers.get("Location")
                        if not location:
                            raise SourceError("invalid_redirect")
                        try:
                            current = normalize_url(urljoin(current, location))
                        except ValueError:
                            raise SourceError("invalid_redirect") from None
                        continue
                    raw = self._body(response)
                    result.update(raw_body=raw, body_bytes=len(raw), content_hash=hashlib.sha256(raw).hexdigest())
                    media = (result["content_type"] or "").split(";", 1)[0].strip().lower()
                    # HTTP errors may still have HTML useful for debugging.
                    if media in ("text/html", "application/xhtml+xml"):
                        charset = re.search(r"charset\s*=\s*[\"']?([^\s;\"']+)", result["content_type"], re.I)
                        try:
                            if charset:
                                encoding = codecs.lookup(charset[1]).name
                                html = raw.decode(encoding, errors="strict")
                            else:
                                decoded = UnicodeDammit(raw, is_html=True)
                                html, encoding = decoded.unicode_markup, decoded.original_encoding
                                if html is None or decoded.contains_replacement_characters:
                                    raise UnicodeError()
                            if "\x00" in html:
                                raise UnicodeError()
                        except (LookupError, UnicodeError):
                            raise SourceError("decode_error") from None
                        result.update(raw_html=html, response_encoding=encoding)
                    if not 200 <= response.status_code <= 299:
                        raise SourceError("http_" + str(response.status_code))
                    if media not in ("text/html", "application/xhtml+xml"):
                        raise SourceError("unsupported_content_type")
                    break
        except SourceError as error:
            result["fetch_error"] = str(error)
        result["fetched_at"] = datetime.now(timezone.utc)
        return result
