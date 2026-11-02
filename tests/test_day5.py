"""Catalog contracts and real TCP/password-authenticated PostgreSQL permission probes."""
from contextlib import closing
from copy import deepcopy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from uuid import UUID, uuid4

import psycopg2
import yaml

from src.common.catalog import load_catalog, register_catalog
from src.common.smoke import connect


class CatalogTests(unittest.TestCase):
    def loaded(self, config):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'catalog.yaml'
            path.write_text(yaml.safe_dump(config), encoding='utf-8')
            return load_catalog(path)

    def test_canonical_hash(self):
        config, digest = load_catalog()
        config['datasets'].reverse()
        self.assertEqual(self.loaded(config)[1], digest)

    def test_owner_is_part_of_contract(self):
        config, digest = load_catalog()
        config['datasets'][0]['owner'] = 'another_owner'
        self.assertNotEqual(self.loaded(config)[1], digest)

    def test_missing_duplicate_unknown(self):
        config, _ = load_catalog()
        for rows in (config['datasets'][:-1], [config['datasets'][0]] * 4,
                     config['datasets'] + [{'dataset_name': 'unknown', 'owner': 'x', 'description': 'x'}]):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                self.loaded(dict(config, datasets=rows))

    def test_empty_owner_and_unknown_field(self):
        config, _ = load_catalog()
        for key, value in [('owner', ''), ('typo', 'value')]:
            altered = deepcopy(config)
            altered['datasets'][0][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.loaded(altered)

    def test_unsupported_schema(self):
        config, _ = load_catalog()
        with self.assertRaises(ValueError):
            self.loaded(dict(config, schema_version='99.0.0'))


def permissions(source_run):
    source = str(UUID(source_run))
    fresh = str(uuid4())
    report = {'source_run': source, 'probes': [], 'identities': []}

    def probe(role, name, statement, allowed):
        with closing(connect(role)) as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(statement)
                if not allowed:
                    raise AssertionError(f'{role} unexpectedly allowed {name}')
                code = None
            except psycopg2.Error as error:
                if allowed or error.pgcode != '42501':
                    raise AssertionError(f'{role} {name}: expected {"allow" if allowed else "42501"}, got {error.pgcode}') from None
                code = error.pgcode
            finally:
                conn.rollback()
        report['probes'].append({'role': role, 'operation': name, 'expected': 'allow' if allowed else 'deny',
                                 'result': 'PASS', 'sqlstate': code})

    for role in ('CRAWLER', 'SPARK', 'PUBLISHER', 'OBSERVER'):
        with closing(connect(role)) as conn, conn, conn.cursor() as cur:
            cur.execute('''SELECT session_user,current_user,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls
                FROM pg_roles WHERE rolname=current_user''')
            user, current, *flags = cur.fetchone()
            assert user == current == os.environ[f'{role}_DB_USER'] and not any(flags)
            cur.execute('SELECT count(*) FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=current_user)')
            assert cur.fetchone()[0] == 0
            report['identities'].append({'role': role, 'session_user': user, 'superuser': False, 'memberships': 0})
        for name, statement in [('create_table', 'CREATE TABLE public.day5_forbidden(x int)'),
                                ('create_schema', 'CREATE SCHEMA day5_forbidden'),
                                ('temporary_table', 'CREATE TEMP TABLE day5_forbidden(x int)'),
                                ('assume_admin', 'SET ROLE "' + os.environ['LAB_ADMIN_DB_USER'].replace('"', '""') + '"')]:
            probe(role, name, statement, False)

    probe('CRAWLER', 'read_bronze_for_replay', f"SELECT page_id FROM bronze.web_pages WHERE run_id='{source}'", True)
    probe('CRAWLER', 'write_bronze', f"""INSERT INTO bronze.web_pages SELECT '{fresh}'::uuid,page_id,
        requested_url,final_url,fetched_at,http_status,content_type,content_hash,raw_html,response_encoding,fetch_error
        FROM bronze.web_pages WHERE run_id='{source}'""", True)
    for name, statement in [('delete_bronze', 'DELETE FROM bronze.web_pages WHERE false'),
                            ('update_bronze', 'UPDATE bronze.web_pages SET raw_html=raw_html WHERE false'),
                            ('read_silver', 'SELECT * FROM silver.documents LIMIT 1'),
                            ('write_staging', 'DELETE FROM staging.documents WHERE false'),
                            ('write_metadata', 'UPDATE metadata.pipeline_runs SET error_summary=NULL WHERE false')]:
        probe('CRAWLER', name, statement, False)

    probe('SPARK', 'read_bronze', f"SELECT raw_html FROM bronze.web_pages WHERE run_id='{source}'", True)
    probe('SPARK', 'write_staging', f"""INSERT INTO staging.documents SELECT '{fresh}','test-permission',
        page_id,source_url,content_hash,title,clean_text,token_count,processed_at,transform_version
        FROM silver.documents WHERE run_id='{source}'""", True)
    probe('SPARK', 'write_quality', f"""INSERT INTO quality.check_results
        (run_id,attempt_id,scope_key,check_name,status,observed_value,threshold,rules_version)
        VALUES ('{source}','permission','@run','permission','pass','1','1','1.1.0')""", True)
    probe('SPARK', 'write_quarantine', f"""INSERT INTO quarantine.rejected_pages
        SELECT run_id,page_id,'permission','test_only','1.1.0',now() FROM bronze.web_pages
        WHERE run_id='{source}' AND page_id NOT IN (SELECT page_id FROM quarantine.rejected_pages WHERE run_id='{source}') LIMIT 1""", True)
    probe('SPARK', 'write_control', f"""INSERT INTO metadata.pipeline_runs
        (run_id,dag_id,airflow_run_id,transform_version,rules_version) VALUES
        ('{fresh}','permission','{fresh}','1.0.0','1.1.0');
        UPDATE metadata.pipeline_runs SET publish_status='running' WHERE run_id='{fresh}'""", True)
    for name, statement in [('write_bronze', 'DELETE FROM bronze.web_pages WHERE false'),
                            ('publish_silver', 'DELETE FROM silver.documents WHERE false'),
                            ('publish_demo', 'DELETE FROM demo.word_counts WHERE false'),
                            ('fake_published_flag', f"UPDATE metadata.pipeline_runs SET publish_status='published' WHERE run_id='{source}'"),
                            ('fake_published_time', f"UPDATE metadata.pipeline_runs SET published_at=now() WHERE run_id='{source}'"),
                            ('fake_lineage_publication', f"UPDATE metadata.lineage SET publish_status='published' WHERE run_id='{source}'"),
                            ('edit_published_metadata', f"UPDATE metadata.pipeline_runs SET total_tokens=0 WHERE run_id='{source}'"),
                            ('register_dataset', "UPDATE metadata.datasets SET owner='unauthorized'"),
                            ('assume_publisher', 'SET ROLE "' + os.environ['PUBLISHER_DB_USER'].replace('"', '""') + '"')]:
        probe('SPARK', name, statement, False)

    probe('PUBLISHER', 'read_staging', f"SELECT * FROM staging.documents WHERE run_id='{source}'", True)
    probe('PUBLISHER', 'write_public_tables', f"""DELETE FROM demo.word_counts WHERE run_id='{source}';
        DELETE FROM silver.documents WHERE run_id='{source}';
        INSERT INTO silver.documents SELECT run_id::uuid,page_id,source_url,content_hash,title,clean_text,
        token_count,processed_at,transform_version FROM staging.documents WHERE run_id='{source}';
        INSERT INTO demo.word_counts SELECT run_id::uuid,page_id,word,count FROM staging.word_counts WHERE run_id='{source}';
        UPDATE metadata.pipeline_runs SET publish_status='published' WHERE run_id='{source}';
        UPDATE metadata.lineage SET publish_status='published' WHERE run_id='{source}'""", True)
    for name, statement in [('write_bronze', 'DELETE FROM bronze.web_pages WHERE false'),
                            ('write_staging', 'DELETE FROM staging.documents WHERE false'),
                            ('change_rules', f"UPDATE metadata.pipeline_runs SET rules_sha256='bad' WHERE run_id='{source}'"),
                            ('change_catalog', "UPDATE metadata.datasets SET owner='unauthorized'")]:
        probe('PUBLISHER', name, statement, False)

    for table in ('metadata.datasets', 'metadata.run_summary', 'metadata.lineage', 'demo.published_word_counts'):
        probe('OBSERVER', 'read_' + table, f'SELECT * FROM {table} LIMIT 1', True)
    for table in ('bronze.web_pages', 'bronze.fetch_details', 'staging.documents', 'silver.documents',
                  'demo.word_counts', 'metadata.pipeline_runs', 'quality.check_results'):
        probe('OBSERVER', 'deny_read_' + table, f'SELECT * FROM {table} LIMIT 1', False)
    for table in ('metadata.datasets', 'metadata.lineage', 'demo.word_counts'):
        probe('OBSERVER', 'deny_write_' + table, f'DELETE FROM {table} WHERE false', False)

    # Registration is idempotent; an edited definition at the same version fails
    # instead of silently rewriting catalog history. Admin is used only here.
    with closing(connect('LAB_ADMIN')) as conn:
        with conn.cursor() as cur:
            register_catalog(cur)
            cur.execute("UPDATE metadata.datasets SET owner='test_drift' WHERE dataset_name='bronze.web_pages'")
            try:
                register_catalog(cur)
                raise AssertionError('Dataset drift silently accepted')
            except RuntimeError as error:
                assert 'version changed' in str(error)
        conn.rollback()
    report['catalog_drift_blocked'] = True
    report['acceptance'] = 'PASS'
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--permissions':
        permissions(sys.argv[2])
    else:
        unittest.main(testRunner=unittest.TextTestRunner(stream=sys.stdout, verbosity=2))
