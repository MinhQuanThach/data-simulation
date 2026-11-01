"""Airflow boundary for fixture or web input; HTML never goes through XCom."""
from contextlib import closing
import hashlib
import json
from uuid import UUID

from psycopg2.extras import Json

from src.common.pipeline import (check_versions, locked_run, output_snapshot,
                                 prepare_fixture, verify_output)
from src.common.smoke import connect, master_state, run_uuid
from src.common.transform import RULES_VERSION, TRANSFORM_VERSION
from src.crawler.config import page_id, validate_input
from src.crawler.http import Crawler


def validate_config(dag_id, airflow_run_id, **context):
    config = validate_input(context["dag_run"].conf or {})
    return dict(config, run_id=run_uuid(dag_id, airflow_run_id))


def crawl_to_bronze(config, dag_id, airflow_run_id, **context):
    if config["mode"] == "fixture":
        return dict(prepare_fixture(dag_id, airflow_run_id, **context), mode="fixture")
    run_id = str(UUID(config["run_id"]))
    if run_id != run_uuid(dag_id, airflow_run_id):
        raise RuntimeError("Run identity mismatch")
    master_state()
    # One control lock across network/write/commit, matching Spark and publisher.
    with locked_run(run_id) as control:
        with control, control.cursor() as cur:
            cur.execute("""INSERT INTO metadata.pipeline_runs
                (run_id,dag_id,airflow_run_id,transform_version,rules_version,input_config,
                 requested_count,unique_input_count)
                VALUES (%s::uuid,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (run_id) DO NOTHING""",
                (run_id, dag_id, airflow_run_id, TRANSFORM_VERSION, RULES_VERSION, Json(config),
                 config["requested_count"], config["unique_input_count"]))
            cur.execute("""SELECT transform_version,rules_version,input_config,crawl_complete
                FROM metadata.pipeline_runs WHERE run_id=%s::uuid FOR UPDATE""", (run_id,))
            row = cur.fetchone()
            check_versions(row)
            if row[2] != config:
                raise RuntimeError("Snapshot configuration changed; create a new DagRun")
        with closing(connect("CRAWLER")) as conn, conn, conn.cursor() as cur:
            cur.execute("""SELECT b.page_id,d.crawler_version FROM bronze.web_pages b
                LEFT JOIN bronze.fetch_details d USING (run_id,page_id)
                WHERE b.run_id=%s::uuid ORDER BY b.page_id""", (run_id,))
            existing = cur.fetchall()
            if existing:
                if [r[0] for r in existing] != sorted(page_id(url) for url in config["urls"]) or any(not r[1] for r in existing):
                    raise RuntimeError("Incomplete/mismatched Bronze snapshot")
                print(f"CRAWL_NOOP run_id={run_id}; persisted snapshot reused", flush=True)
            else:
                if row[3]:
                    raise RuntimeError("Crawl marked complete but Bronze is missing")
                crawler = Crawler()
                try:
                    for url in config["urls"]:
                        item = crawler.fetch(url)
                        cur.execute("""INSERT INTO bronze.web_pages
                            (run_id,page_id,requested_url,final_url,fetched_at,http_status,content_type,
                             content_hash,raw_html,response_encoding,fetch_error)
                            VALUES (%s::uuid,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            (run_id,) + tuple(item[k] for k in ("page_id", "requested_url", "final_url", "fetched_at",
                             "http_status", "content_type", "content_hash", "raw_html", "response_encoding", "fetch_error")))
                        cur.execute("""INSERT INTO bronze.fetch_details VALUES
                            (%s::uuid,%s,%s,%s,%s,%s,%s,%s)""",
                            (run_id, item["page_id"], item["raw_body"], item["body_bytes"],
                             Json(item["request_history"]), Json(item["robots_history"]),
                             Json(item["response_headers"]), item["crawler_version"]))
                        print(f"CRAWL_RESULT run_id={run_id} page_id={item['page_id']} "
                              f"status={item['http_status']} bytes={item['body_bytes']} reason={item['fetch_error']}", flush=True)
                finally:
                    crawler.close()
        # If the process dies between commits, the next attempt validates/reuses
        # the whole Bronze transaction above, without making fresh HTTP requests.
        with control, control.cursor() as cur:
            cur.execute("UPDATE metadata.pipeline_runs SET crawl_complete=true WHERE run_id=%s::uuid", (run_id,))
    return {"run_id": run_id, "mode": "web", "top_n": config["top_n"],
            "fail_once": config["fail_once"], "input_count": config["unique_input_count"]}


def web_snapshot(run_id, top_n=3):
    run_id = str(UUID(run_id))
    result = output_snapshot(run_id, int(top_n))
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute("""SELECT to_jsonb(b) || jsonb_build_object(
            'body_bytes',d.body_bytes,'raw_body_hex',encode(d.raw_body,'hex'),
            'request_history',d.request_history,'robots_history',d.robots_history,
            'response_headers',d.response_headers,'crawler_version',d.crawler_version)
            FROM bronze.web_pages b JOIN bronze.fetch_details d USING (run_id,page_id)
            WHERE b.run_id=%s::uuid ORDER BY b.page_id""", (run_id,))
        result["bronze"] = [row[0] for row in cur.fetchall()]
    return result


def verify_web(run_id, top_n=3):
    snapshot = web_snapshot(run_id, int(top_n))
    meta, docs, counts, bronze = (snapshot[k] for k in ("metadata", "documents", "word_counts", "bronze"))
    config = meta["input_config"]
    if not meta["crawl_complete"] or meta["publish_status"] != "published":
        raise RuntimeError("Expected completed crawl and published output")
    if not config or len(bronze) != config["unique_input_count"] or len(docs) != len(bronze) or meta["input_count"] != len(bronze):
        raise RuntimeError("Input/Bronze/Silver counts differ")
    by_page = {b["page_id"]: b for b in bronze}
    if set(by_page) != {page_id(url) for url in config["urls"]}:
        raise RuntimeError("Bronze URL identity mismatch")
    total = 0
    for doc in docs:
        source = by_page[doc["page_id"]]
        raw = bytes.fromhex(source["raw_body_hex"])
        words = [w for w in counts if w["page_id"] == doc["page_id"]]
        if (source["fetch_error"] or not 200 <= source["http_status"] <= 299
                or not source["raw_html"] or not source["request_history"] or not source["robots_history"]
                or len(raw) != source["body_bytes"]
                or hashlib.sha256(raw).hexdigest() != source["content_hash"]
                or source["content_hash"] != doc["content_hash"]
                or doc["source_url"] != source["final_url"] or not doc["clean_text"].strip()
                or doc["transform_version"] != TRANSFORM_VERSION):
            raise RuntimeError("Bronze bytes/HTTP/source/Silver invariant failed")
        if (not words or any(w["count"] <= 0 for w in words)
                or len(words) != len({w["word"] for w in words})
                or sum(w["count"] for w in words) != doc["token_count"]):
            raise RuntimeError("Per-page complete word counts differ from token_count")
        ordered = sorted(words, key=lambda w: (-w["count"], w["word"].encode("utf-8")))
        if words != ordered:
            raise RuntimeError("Top-N ordering differs from count DESC/word COLLATE C ASC")
        total += doc["token_count"]
    if total != meta["total_tokens"]:
        raise RuntimeError("Run token total mismatch")
    result = {"run_id": str(run_id), "input_count": len(bronze), "total_tokens": total,
              "application_id": meta["spark_application_id"], "top_n": snapshot["top_n"]}
    print("WEB_VERIFIED " + json.dumps(result, ensure_ascii=False))
    return result


def verify_ingestion(run_id, mode, top_n=3):
    return verify_web(run_id, int(top_n)) if mode == "web" else verify_output(run_id, int(top_n))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("inspect", "verify"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--top-n", type=int, default=3)
    args = parser.parse_args()
    if args.action == "verify":
        verify_web(args.run_id, args.top_n)
    else:
        print(json.dumps(web_snapshot(args.run_id, args.top_n), ensure_ascii=False, indent=2))
