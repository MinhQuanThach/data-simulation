"""Explicit catalog registration and per-attempt execution lineage, not auto-discovery."""
import hashlib
import json
from pathlib import Path

from psycopg2.extras import Json
import yaml

CATALOG_PATH = Path(__file__).resolve().parents[2] / 'config/datasets.yaml'
DATASETS = {'bronze.web_pages', 'silver.documents', 'demo.word_counts', 'quarantine.rejected_pages'}
OUTPUTS = ('silver.documents', 'demo.word_counts', 'quarantine.rejected_pages')
JOB = 'src.jobs.clean_validate_count'


def load_catalog(path=CATALOG_PATH):
    config = yaml.safe_load(Path(path).read_text(encoding='utf-8'))
    if not isinstance(config, dict) or set(config) != {'schema_version', 'datasets'} or config['schema_version'] != '1.0.0':
        raise ValueError('Unsupported catalog schema/version')
    rows = config['datasets']
    if not isinstance(rows, list) or len(rows) != len(DATASETS):
        raise ValueError('Catalog must describe every supported dataset exactly once')
    for row in rows:
        if not isinstance(row, dict) or set(row) != {'dataset_name', 'owner', 'description'}:
            raise ValueError('Unsupported dataset fields')
        if any(not isinstance(v, str) or not v.strip() for v in row.values()):
            raise ValueError('Dataset fields must be nonempty strings')
    if {r['dataset_name'] for r in rows} != DATASETS:
        raise ValueError('Missing, duplicate or unsupported dataset')
    config['datasets'] = sorted(rows, key=lambda r: r['dataset_name'])
    digest = hashlib.sha256(json.dumps(config, ensure_ascii=False, sort_keys=True,
                                       separators=(',', ':')).encode()).hexdigest()
    return config, digest


def register_catalog(cur):
    config, digest = load_catalog()
    for row in config['datasets']:
        args = (row['dataset_name'], config['schema_version'], row['owner'], row['description'])
        cur.execute('''INSERT INTO metadata.datasets(dataset_name,schema_version,owner,description)
            VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING''', args)
        cur.execute('SELECT owner,description FROM metadata.datasets WHERE dataset_name=%s AND schema_version=%s', args[:2])
        if cur.fetchone() != args[2:]:
            raise RuntimeError('Registered dataset version changed; add a schema version and reader support')
    cur.execute('INSERT INTO metadata.catalog_versions(catalog_sha256,config) VALUES (%s,%s) ON CONFLICT DO NOTHING',
                (digest, Json(config)))
    return digest


def pin_catalog(cur, run_id):
    cur.execute('SELECT catalog_sha256,publish_status FROM metadata.pipeline_runs WHERE run_id=%s::uuid', (run_id,))
    old, status = cur.fetchone()
    if status == 'published':
        return old  # Historical replay stays pinned; no fabricated backfill.
    config, digest = load_catalog()
    cur.execute('SELECT config FROM metadata.catalog_versions WHERE catalog_sha256=%s', (digest,))
    row = cur.fetchone()
    if row is None or row[0] != config:
        raise RuntimeError('Catalog not registered; run day5 migrate with admin')
    if old is not None and old != digest:
        raise RuntimeError('Catalog changed; create a new DagRun')
    if old is None:
        cur.execute('UPDATE metadata.pipeline_runs SET catalog_sha256=%s WHERE run_id=%s::uuid', (digest, run_id))
    return digest


def record_lineage(cur, run_id, attempt, status, counts=None):
    cur.execute('''SELECT transform_version,rules_version,rules_sha256,catalog_sha256,spark_application_id
        FROM metadata.pipeline_runs WHERE run_id=%s::uuid''', (run_id,))
    transform, rules, rules_hash, catalog_hash, app = cur.fetchone()
    if not app:
        return  # A failure before Spark starts has no real execution node.
    cur.execute('SELECT config FROM metadata.catalog_versions WHERE catalog_sha256=%s', (catalog_hash,))
    row = cur.fetchone()
    if row is None:
        raise RuntimeError('Execution missing pinned catalog')
    version = row[0]['schema_version']
    for output in OUTPUTS:
        cur.execute('''INSERT INTO metadata.lineage
            (run_id,attempt_id,input_dataset,input_schema_version,output_dataset,output_schema_version,
             job_name,spark_application_id,transform_version,rules_version,rules_sha256,catalog_sha256,
             publish_status,output_count)
            VALUES (%s::uuid,%s,'bronze.web_pages',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (run_id,attempt_id,input_dataset,output_dataset) DO UPDATE
            SET publish_status=excluded.publish_status,output_count=excluded.output_count,updated_at=now()''',
            (run_id, attempt, version, output, version, JOB, app, transform, rules, rules_hash,
             catalog_hash, status, (counts or {}).get(output)))


def publish_lineage(cur, run_id, attempt):
    cur.execute('SELECT catalog_sha256 FROM metadata.pipeline_runs WHERE run_id=%s::uuid', (run_id,))
    if cur.fetchone()[0] != load_catalog()[1]:
        raise RuntimeError('Catalog changed or missing; create a new DagRun')
    cur.execute('''SELECT output_dataset,spark_application_id,rules_sha256,catalog_sha256,
        transform_version,rules_version FROM metadata.lineage
        WHERE run_id=%s::uuid AND attempt_id=%s AND publish_status='staged' ''', (run_id, attempt))
    rows = cur.fetchall()
    cur.execute('''SELECT spark_application_id,rules_sha256,catalog_sha256,transform_version,rules_version
        FROM metadata.pipeline_runs WHERE run_id=%s::uuid''', (run_id,))
    expected = cur.fetchone()
    if {r[0] for r in rows} != set(OUTPUTS) or any(tuple(r[1:]) != expected for r in rows):
        raise RuntimeError('Missing or mismatched staged lineage')
    cur.execute('''UPDATE metadata.lineage SET publish_status='published',updated_at=now()
        WHERE run_id=%s::uuid AND attempt_id=%s''', (run_id, attempt))
