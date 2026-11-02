"""Day-5 access/catalog/lineage acceptance with actual Airflow and standalone Spark."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import subprocess
from uuid import UUID

from day1 import ENV, ROOT, compose, redact, save
from day2 import DAG, await_run, collect, scheduler
import day4

EVIDENCE = ROOT / 'docs/evidence/day5'


def hashes():
    files = [ROOT / 'compose.yaml']
    for folder in ('src', 'dags', 'scripts', 'tests', 'sql', 'config', 'fixtures'):
        files.extend(p for p in (ROOT / folder).rglob('*') if p.suffix in ('.py', '.sql', '.yaml', '.json', '.html'))
    return {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(files)}


def administrative_runtime(*args):
    # Only this short-lived operator container gets admin/observer credentials.
    # DAG tasks never receive either credential; each probe connects as its role.
    return compose('run', '--rm', '--no-deps', '--entrypoint', 'python',
                   '--volume', str(ROOT / 'tests') + ':/opt/lab/tests:ro', 'lab-init', *args)


def lineage_snapshot(run_id):
    run_id = str(UUID(run_id))
    return json.loads(administrative_runtime('-c',
        "import json; from src.common.smoke import connect; "
        "c=connect('OBSERVER'); q=c.cursor(); "
        "q.execute('SELECT row_to_json(r) FROM metadata.run_summary r WHERE run_id=%s::uuid', ('" + run_id + "',)); "
        "r=q.fetchone()[0]; q.execute('SELECT row_to_json(l) FROM metadata.lineage l WHERE run_id=%s::uuid ORDER BY attempt_id,output_dataset', ('" + run_id + "',)); "
        "print(json.dumps({'run':r,'lineage':[x[0] for x in q.fetchall()]})); c.close()"))


def check_lineage(run, snapshot):
    result = lineage_snapshot(run['run_id'])
    meta, edges = result['run'], result['lineage']
    assert len(meta['catalog_sha256']) == len(meta['rules_sha256']) == 64
    active = [e for e in edges if e['attempt_id'] == meta['active_attempt']]
    assert len(active) == 3
    assert {e['output_dataset'] for e in active} == {'silver.documents', 'demo.word_counts', 'quarantine.rejected_pages'}
    for edge in edges:
        assert edge['input_dataset'] == 'bronze.web_pages'
        assert edge['input_schema_version'] == edge['output_schema_version'] == '1.0.0'
        assert edge['job_name'] == 'src.jobs.clean_validate_count'
        assert edge['transform_version'] == meta['transform_version']
        assert edge['rules_version'] == meta['rules_version']
        assert edge['rules_sha256'] == meta['rules_sha256'] and edge['catalog_sha256'] == meta['catalog_sha256']
        assert edge['spark_application_id'].startswith('app-')
    assert all(e['spark_application_id'] == meta['spark_application_id'] for e in active)
    target = 'published' if run['state'] == 'success' else 'failed'
    assert all(e['publish_status'] == target for e in active)
    assert all(e['publish_status'] == 'failed' for e in edges if e not in active)
    if target == 'published':
        expected = {'silver.documents': len(snapshot['documents']), 'demo.word_counts': len(snapshot['word_counts']),
                    'quarantine.rejected_pages': len(snapshot['quarantine'])}
        assert {e['output_dataset']: e['output_count'] for e in active} == expected
    save(run['evidence'] / 'observer-lineage.json', json.dumps(result, indent=2))
    return result


def trigger(case='mixed', **kwargs):
    day4.EVIDENCE = EVIDENCE
    run, snapshot = day4.trigger(case, prefix='day5', **kwargs)
    check_lineage(run, snapshot)
    return run, snapshot


def accept():
    before = hashes()
    compose('run', '--rm', '--no-deps', 'lab-init')
    # Reapply to prove grants/catalog/migration are rerunnable with existing data.
    compose('run', '--rm', '--no-deps', 'lab-init')
    for day in (2, 3, 4, 5):
        compose('cp', str(ROOT / f'tests/test_day{day}.py'), f'airflow-scheduler:/tmp/test_day{day}.py')
        save(EVIDENCE / f'unit-day{day}.txt', scheduler('python', f'/tmp/test_day{day}.py', '-v'))
    errors = scheduler('airflow', 'dags', 'list-import-errors', '--output', 'json')
    save(EVIDENCE / 'dag-import-errors.json', errors)
    assert not json.loads(errors)
    normal, _ = trigger('valid_vi')
    output = administrative_runtime('/opt/lab/tests/test_day5.py', '--permissions', normal['run_id'])
    permissions = json.loads(output)
    save(EVIDENCE / 'permissions.json', output)
    save(EVIDENCE / 'transaction-tests.txt', scheduler('python', '/tmp/test_day2.py', '--transactions', normal['run_id']))
    mixed, snapshot = trigger('mixed', fail_once=True)
    old_lineage = lineage_snapshot(mixed['run_id'])
    scheduler('airflow', 'tasks', 'clear', DAG, '--start-date', mixed['logical_date'],
              '--end-date', mixed['logical_date'], '--yes')
    await_run(mixed)
    report, replay, logs = collect(mixed, 'replay')
    assert mixed['state'] == 'success' and snapshot == replay
    assert old_lineage == lineage_snapshot(mixed['run_id'])
    assert all(t['state'] == 'success' and t['try_number'] >= 2 for t in report['tasks'])
    assert 'TRANSFORM_NOOP' in logs and 'PUBLISH_NOOP' in logs
    report.update(acceptance='PASS', scope='day5 exact partial replay with lineage unchanged')
    save(mixed['evidence'] / 'replay/report.json', json.dumps(report, indent=2))
    invalid, _ = trigger('all_invalid')
    system, _ = trigger('valid_vi', fail_always=True)
    urls = json.loads((ROOT / 'config/day3_acceptance.json').read_text(encoding='utf-8'))
    web, snapshot = trigger(urls=[urls['first_url'], urls['error_url']])
    web_lineage = lineage_snapshot(web['run_id'])
    scheduler('airflow', 'tasks', 'clear', DAG, '--start-date', web['logical_date'],
              '--end-date', web['logical_date'], '--yes')
    await_run(web)
    report, replay, logs = collect(web, 'replay')
    assert web['state'] == 'success' and snapshot == replay and web_lineage == lineage_snapshot(web['run_id'])
    assert all(t['state'] == 'success' and t['try_number'] >= 2 for t in report['tasks'])
    assert all(marker in logs for marker in ('CRAWL_NOOP', 'TRANSFORM_NOOP', 'PUBLISH_NOOP'))
    report.update(acceptance='PASS', scope='day5 real web exact replay; no recrawl')
    save(web['evidence'] / 'replay/report.json', json.dumps(report, indent=2))
    # Query the documented SQL as observer, not through the admin psql socket.
    save(EVIDENCE / 'lineage-psql.txt', observer_sql(mixed['run_id']))
    services = [{k: row.get(k) for k in ('Service', 'State', 'Health')}
                for row in map(json.loads, filter(str.strip, compose('ps', '--format', 'json').splitlines()))]
    assert len(services) == 7 and all(s['Health'] == 'healthy' for s in services)
    runtime = {s: json.loads(compose('exec', '-T', s, 'python', '-m', 'src.common.runtime_inventory'))
               for s in ('airflow-scheduler', 'spark-worker')}
    assert {k:v for k,v in runtime['airflow-scheduler'].items() if k != 'spark_submit'} == {
        k:v for k,v in runtime['spark-worker'].items() if k != 'spark_submit'}
    save(EVIDENCE / 'runtime.json', json.dumps(runtime, indent=2))
    images = {s: compose('images', '-q', s).strip() for s in ('airflow-scheduler', 'spark-worker')}
    assert images['airflow-scheduler'] == images['spark-worker']
    save(EVIDENCE / 'images.json', json.dumps(images, indent=2))
    assert hashes() == before, 'Source changed during acceptance; rerun after stabilizing code'
    runs = [normal, mixed, invalid, system, web]
    summary = {'acceptance': 'PASS', 'checked_at_utc': datetime.now(timezone.utc).isoformat(),
               'scope': 'day5 access/catalog/lineage; days6-7 pending', 'services': services,
               'normal_run': normal['run_id'], 'partial_retry_replay_run': mixed['run_id'],
               'all_invalid_run': invalid['run_id'], 'system_failure_run': system['run_id'],
               'web_partial_replay_run': web['run_id'], 'permission_probes': len(permissions['probes']),
               'atomic_publish_lineage_rollback_visibility': 'PASS',
               'fixture_and_web_exact_replay': 'PASS', 'source_sha256': hashes(),
               'evidence_directories': [r['evidence'].relative_to(ROOT).as_posix() for r in runs]}
    save(EVIDENCE / 'latest.json', json.dumps(summary, indent=2))
    print('PASS day5: role allow/deny, catalog, attempt lineage, atomic publish/retry/replay; docs/evidence/day5/latest.json', flush=True)


def observer_sql(run_id):
    run_id = str(UUID(run_id))
    return administrative_runtime('-c',
        "import os,subprocess; e=dict(os.environ,PGPASSWORD=os.environ['OBSERVER_DB_PASSWORD']); "
        "subprocess.run(['psql','-X','-v','ON_ERROR_STOP=1','-h',os.environ['LAB_DB_HOST'],"
        "'-U',os.environ['OBSERVER_DB_USER'],'-d',os.environ['LAB_DB_NAME'],"
        "'-v','run_id=" + run_id + "','-f','/opt/lab/sql/queries/day5_lineage.sql'],env=e,check=True)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('migrate', 'run', 'accept', 'inspect'))
    parser.add_argument('--case', default='mixed')
    parser.add_argument('--run-id')
    args = parser.parse_args()
    if args.action == 'migrate':
        compose('run', '--rm', '--no-deps', 'lab-init', show=True)
    elif args.action == 'accept':
        accept()
    elif args.action == 'run':
        compose('cp', str(ROOT / 'tests/test_day2.py'), 'airflow-scheduler:/tmp/test_day2.py')
        trigger(args.case)
    else:
        if not args.run_id:
            parser.error('inspect requires --run-id')
        print(observer_sql(args.run_id))


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, ValueError, AssertionError, subprocess.TimeoutExpired) as error:
        raise SystemExit(redact(str(error))) from None
