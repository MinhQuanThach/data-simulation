"""Deterministic local HTTP tests in the pinned runtime, never web evidence."""
from collections import Counter
import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import sys
import threading
import time
import unittest
from unittest.mock import Mock, patch

import requests

from src.crawler.config import normalize_url, validate_input
from src.crawler.http import Crawler, robots_allowed

HTML = "<html><head><title>Local test</title></head><body>Xin chào dữ liệu 2026</body></html>".encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.server.hits[self.path] += 1
        status, body, headers = 200, HTML, {"Content-Type": "text/html; charset=utf-8"}
        if self.path == "/robots.txt":
            status, body = self.server.robots_status, self.server.robots.encode()
            headers = {"Content-Type": "text/plain"}
        elif self.path.startswith("/redirect"):
            status, body = 302, b""
            headers["Location"] = {"/redirect": "/ok", "/redirect-deny": "/blocked",
                                   "/redirect-invalid": "file:///etc/passwd"}.get(self.path, "/redirect-loop")
        elif self.path == "/gzip":
            body = gzip.compress(HTML)
            headers["Content-Encoding"] = "gzip"
        elif self.path == "/large":
            body = gzip.compress(b"x" * 4096)
            headers["Content-Encoding"] = "gzip"
        elif self.path == "/retry" and self.server.hits[self.path] < 3:
            status = 503
        elif self.path == "/busy":
            status = 429
            headers["Retry-After"] = "60"
        elif self.path == "/404":
            status = 404
        elif self.path == "/pdf":
            headers["Content-Type"] = "application/pdf"
            body = b"%PDF-local-test"
        elif self.path == "/charset":
            body = "<html><body>café</body></html>".encode("iso-8859-1")
            headers["Content-Type"] = "text/html; charset=iso-8859-1"
        elif self.path == "/meta":
            body = '<html><head><meta charset="utf-8"></head><body>chào</body></html>'.encode()
            headers["Content-Type"] = "text/html"
        elif self.path == "/bad-charset":
            headers["Content-Type"] = "text/html; charset=not-a-charset"
        elif self.path == "/null":
            body = b"<html><body>\x00</body></html>"
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.path == "/slow":
            time.sleep(0.15)
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass


class ConfigTests(unittest.TestCase):
    def test_normalization_dedup_and_counts(self):
        c = validate_input({"urls": [" https://example.com/a?q=2&x=1#first ",
                                     "https://example.com/a?q=2&x=1#second"], "top_n": 5})
        self.assertEqual(c["urls"], ["https://example.com/a?q=2&x=1"])
        self.assertEqual((c["requested_count"], c["unique_input_count"]), (2, 1))

    def test_invalid_config(self):
        for conf in ({"urls": []}, {"urls": "https://example.com"}, {"urls": ["x"] * 6},
                     {"urls": ["https://example.com"], "fixture_case": "valid_vi"},
                     {"top_n": True}, {"top_n": 0}, {"test_fail_after_documents_once": 1}, {"url": "typo"}):
            with self.subTest(conf=conf), self.assertRaises(ValueError):
                validate_input(conf)
        for url in ("file:///tmp/a", "https://user:password@example.com", "https://x:99999", "https://x/a\nb", "https://x/a b"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                normalize_url(url)

    def test_rep_rules(self):
        rules = "User-agent: *\nDisallow: /private*\nAllow: /private/open$\nDisallow: /*.pdf$\n"
        self.assertFalse(robots_allowed(rules, "https://example.com/private/a"))
        self.assertTrue(robots_allowed(rules, "https://example.com/private/open"))
        self.assertFalse(robots_allowed(rules, "https://example.com/a.pdf"))
        self.assertTrue(robots_allowed(rules, "https://example.com/a.pdf?view=1"))
        self.assertFalse(robots_allowed("User-agent: *\nDisallow: /%61\n", "https://example.com/a"))
        self.assertTrue(robots_allowed("User-agent: *\nDisallow: /\nUser-agent: BronzeSilverLab\nAllow: /\n", "https://example.com/a"))
        self.assertTrue(robots_allowed("User-agent: BronzeSilverLab\nCrawl-delay: 1\nUser-agent: *\nDisallow: /\n", "https://example.com/a"))


class CrawlerTests(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.hits = Counter()
        self.server.robots = "User-agent: *\nDisallow: /blocked\nDisallow: /*.pdf$\n"
        self.server.robots_status = 200
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = "http://127.0.0.1:" + str(self.server.server_port)
        self.clock_value = 0.0
        self.sleeps = []
        self.crawler = Crawler(clock=lambda: self.clock_value, sleep=self.advance)

    def advance(self, seconds):
        self.sleeps.append(seconds)
        self.clock_value += seconds

    def fetch(self, path):
        return self.crawler.fetch(self.base + path)

    def tearDown(self):
        self.crawler.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_html_gzip_and_snapshot_bytes(self):
        result = self.fetch("/gzip")
        self.assertIsNone(result["fetch_error"])
        self.assertEqual(result["raw_body"], HTML)
        self.assertIn("chào", result["raw_html"])
        self.assertEqual(result["body_bytes"], len(HTML))
        self.assertEqual(result["response_encoding"], "utf-8")

    def test_redirect_and_robots_before_target(self):
        result = self.fetch("/redirect")
        self.assertEqual(result["final_url"], self.base + "/ok")
        self.assertEqual(len(result["request_history"]), 2)
        self.assertEqual(self.fetch("/redirect-deny")["fetch_error"], "robots_denied")
        self.assertEqual(self.server.hits["/blocked"], 0)
        self.assertEqual(self.fetch("/a.pdf")["fetch_error"], "robots_denied")
        self.assertEqual(self.server.hits["/a.pdf"], 0)
        self.assertEqual(self.server.hits["/robots.txt"], 1)

    def test_redirect_limit_and_invalid_scheme(self):
        self.assertEqual(self.fetch("/redirect-loop")["fetch_error"], "redirect_limit")
        self.assertEqual(self.server.hits["/redirect-loop"], 6)
        self.assertEqual(self.fetch("/redirect-invalid")["fetch_error"], "invalid_redirect")

    def test_decompressed_size_limit(self):
        self.crawler.max_bytes = 512
        result = self.fetch("/large")
        self.assertEqual(result["fetch_error"], "response_too_large")
        self.assertIsNone(result["raw_body"])
        self.assertIsNone(result["content_hash"])

    def test_http_type_and_decoding_errors_keep_bytes(self):
        self.assertEqual(self.fetch("/404")["fetch_error"], "http_404")
        result = self.fetch("/pdf")
        self.assertEqual(result["fetch_error"], "unsupported_content_type")
        self.assertEqual(result["raw_body"], b"%PDF-local-test")
        self.assertIsNone(result["raw_html"])
        for path in ("/bad-charset", "/null"):
            result = self.fetch(path)
            self.assertEqual(result["fetch_error"], "decode_error")
            self.assertIsNotNone(result["raw_body"])

    def test_charset_and_meta(self):
        self.assertIn("café", self.fetch("/charset")["raw_html"])
        self.assertIn("chào", self.fetch("/meta")["raw_html"])

    def test_http_retry_bound_and_retry_after(self):
        self.assertIsNone(self.fetch("/retry")["fetch_error"])
        self.assertEqual(self.server.hits["/retry"], 3)
        self.assertEqual(self.fetch("/busy")["fetch_error"], "retry_after_exceeds_budget")
        self.assertEqual(self.server.hits["/busy"], 1)

    def test_connect_timeout_retry_bound(self):
        self.crawler.session.get = Mock(side_effect=requests.ConnectTimeout())
        result = self.fetch("/ok")
        self.assertEqual(result["fetch_error"], "robots_unavailable:timeout")
        self.assertEqual(self.crawler.session.get.call_count, 3)

    def test_read_timeout(self):
        self.crawler.timeout = (0.1, 0.03)
        self.assertEqual(self.fetch("/slow")["fetch_error"], "timeout")

    def test_robots_failure_closed(self):
        self.server.robots_status = 503
        result = self.fetch("/ok")
        self.assertEqual(result["fetch_error"], "robots_unavailable:robots_http_503")
        self.assertEqual(self.server.hits["/ok"], 0)
        self.assertEqual(self.server.hits["/robots.txt"], 3)

    def test_absent_robots(self):
        self.server.robots_status = 404
        self.assertIsNone(self.fetch("/ok")["fetch_error"])

    def test_rate_limit_and_crawl_delay(self):
        self.server.robots += "Crawl-delay: 3\n"
        self.fetch("/ok")
        self.fetch("/other")
        self.assertEqual(self.sleeps, [3, 3])

    def test_long_crawl_delay_stops_without_shortening(self):
        self.server.robots += "Crawl-delay: 600\n"
        self.assertEqual(self.fetch("/ok")["fetch_error"], "robots_unavailable:robots_delay_exceeds_budget")
        self.assertEqual(self.server.hits["/ok"], 0)
        self.assertFalse(self.sleeps)


def database_checks():
    """Local HTTP/real DB: rollback and replay, explicitly not public crawl proof."""
    from contextlib import closing
    from uuid import uuid4
    from src.common.smoke import connect, run_uuid
    from src.crawler.pipeline import crawl_to_bronze

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.hits, server.robots_status = Counter(), 200
    server.robots = "User-agent: *\nAllow: /\n"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    dag_id, air_id = "day3_local_http_db_test", str(uuid4())
    run_id = run_uuid(dag_id, air_id)
    base = "http://127.0.0.1:" + str(server.server_port)
    config = dict(validate_input({"urls": [base + "/ok", base + "/other"]}), run_id=run_id)
    crawler = Crawler(interval=0)
    real_fetch = crawler.fetch

    def fail_second(url):
        if url.endswith("/other"):
            raise RuntimeError("injected process failure before Bronze transaction commit")
        return real_fetch(url)

    def state():
        with closing(connect()) as conn, conn, conn.cursor() as cur:
            cur.execute("SELECT page_id,content_hash,fetched_at FROM bronze.web_pages WHERE run_id=%s::uuid ORDER BY page_id", (run_id,))
            rows = cur.fetchall()
            cur.execute("SELECT count(*) FROM bronze.fetch_details WHERE run_id=%s::uuid", (run_id,))
            details = cur.fetchone()[0]
            cur.execute("SELECT crawl_complete FROM metadata.pipeline_runs WHERE run_id=%s::uuid", (run_id,))
            complete = cur.fetchone()[0]
        return rows, details, complete

    try:
        crawler.fetch = fail_second
        with patch("src.crawler.pipeline.Crawler", return_value=crawler):
            try:
                crawl_to_bronze(config, dag_id, air_id)
                raise AssertionError("Injected failure did not execute")
            except RuntimeError as error:
                assert "injected process failure" in str(error)
        assert state() == ([], 0, False), "Partial Bronze transaction became visible"
        crawl_to_bronze(config, dag_id, air_id)
        before = state()
        assert len(before[0]) == 2 and before[1:] == (2, True)
        hits_before = dict(server.hits)
        with patch("src.crawler.pipeline.Crawler", side_effect=AssertionError("Replay attempted HTTP")):
            crawl_to_bronze(config, dag_id, air_id)
        assert state() == before and dict(server.hits) == hits_before
        # Recover the commit gap between Bronze and control metadata without recrawl.
        with closing(connect()) as conn, conn, conn.cursor() as cur:
            cur.execute("UPDATE metadata.pipeline_runs SET crawl_complete=false WHERE run_id=%s::uuid", (run_id,))
        with patch("src.crawler.pipeline.Crawler", side_effect=AssertionError("Recovery attempted HTTP")):
            crawl_to_bronze(config, dag_id, air_id)
        assert state() == before
        try:
            crawl_to_bronze(dict(config, top_n=10), dag_id, air_id)
            raise AssertionError("Changed configuration was accepted")
        except RuntimeError as error:
            assert "configuration changed" in str(error)
        print(json.dumps({"scope": "local HTTP server, not public web evidence", "run_id": run_id,
                          "partial_bronze_rollback": True, "retry_whole_snapshot": True,
                          "replay_no_http": True, "metadata_commit_gap_recovery": True,
                          "changed_conf_rejected": True}), flush=True)
    finally:
        crawler.close()
        server.shutdown()
        server.server_close()
        thread.join()


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "--db":
        database_checks()
    elif len(sys.argv) > 1 and sys.argv[1] == "--probe":
        crawler = Crawler()
        try:
            for url in sys.argv[2:]:
                result = crawler.fetch(url)
                print(json.dumps({k: result[k] for k in ("requested_url", "http_status", "content_type", "fetch_error", "body_bytes")}), flush=True)
        finally:
            crawler.close()
    else:
        unittest.main(testRunner=unittest.TextTestRunner(stream=sys.stdout, verbosity=2))
