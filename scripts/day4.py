"""Day-4 operator: actual Airflow/standalone/SQL quality acceptance, redacted evidence."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import re
import subprocess
from uuid import NAMESPACE_URL, UUID, uuid5

from day1 import ENV, ROOT, compose, query, redact, save
from day2 import DAG, TASK, await_run, collect, inspect_output, scheduler

EVIDENCE = ROOT / 'docs/evidence/day4'
SOURCE_FILES = ('src/common/quality.py', 'config/quality_rules.yaml', 'src/common/transform.py',
                'src/common/pipeline.py', 'src/jobs/clean_validate_count.py', 'src/crawler/pipeline.py',
                'src/crawler/config.py', 'dags/bronze_to_silver.py', 'sql/migrations/004_day4.sql',
                'scripts/init-lab.py', 'scripts/day4.py', 'tests/test_day4.py', 'tests/test_day2.py',
                'fixtures/html/valid_vi.html', 'fixtures/html/empty.html', 'fixtures/html/script_only.html',
                'fixtures/html/missing_body.html', 'fixtures/html/no_tokens.html', 'fixtures/expected.json',
                'tests/test_day4_spark.py', 'scripts/day1.py', 'scripts/day2.py', 'tests/test_day3.py',
                'src/crawler/http.py', 'compose.yaml', 'config/day3_acceptance.json',
                'sql/queries/day4_quality.sql', 'sql/migrations/002_day2.sql', 'sql/migrations/003_day3.sql')


def hashes():
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCE_FILES}


def check_snapshot(run, snapshot):
    meta, docs, counts, rejected, checks = (snapshot[k] for k in
        ('metadata', 'documents', 'word_counts', 'quarantine', 'quality_checks'))
    expected = json.loads((ROOT / 'fixtures/expected.json').read_text(encoding='utf-8'))['valid_vi']
    active = [c for c in checks if c['attempt_id'] == meta['active_attempt']]
    assert len(active) >= 11, 'Persisted checks missing'
    assert meta['rules_version'] == '1.1.0' and len(meta['rules_sha256']) == 64
    assert meta['input_count'] == meta['valid_count'] + meta['rejected_count']
    assert len(rejected) == meta['rejected_count']
    assert all(q['reason'] and q['attempt_id'] == meta['active_attempt'] for q in rejected)
    assert not {q['page_id'] for q in rejected} & {d['page_id'] for d in docs}
    if run['expected'] == 'success':
        assert meta['publish_status'] == 'published' and meta['staging_complete']
        assert len(docs) == meta['valid_count'] == 1
        assert docs[0]['clean_text'] == expected['clean_text'] and docs[0]['token_count'] == 12
        assert {c['word']: c['count'] for c in counts} == expected['word_counts']
        assert sum(c['count'] for c in counts) == meta['total_tokens'] == 12
        assert meta['quality_status'] == ('partial' if rejected else 'valid')
        assert all(c['status'] == 'pass' for c in active if c['scope_key'] == '@run')
    else:
        assert meta['publish_status'] == 'failed' and not meta['staging_complete']
        assert not docs and not counts
        blocked = scheduler('python', '/tmp/test_day2.py', '--assert-blocked', run['run_id'])
        save(run['evidence'] / 'publish-blocked.txt', blocked)
        if run['conf'].get('test_fail_after_documents_always'):
            assert meta['quality_status'] == 'system_failure'
            assert not rejected, 'Infrastructure failure misclassified as page rejection'
        else:
            assert meta['quality_status'] == 'all_invalid'
            assert meta['valid_count'] == 0 and len(rejected) == meta['input_count']


def trigger(case='valid_vi', fail_once=False, fail_always=False, urls=None, prefix='day4'):
    timestamp = datetime.now(timezone.utc)
    if prefix not in ('day4', 'day5'):
        raise ValueError('Unsupported acceptance prefix')
    airflow_id = prefix + '_' + timestamp.strftime('%Y%m%dT%H%M%S%fZ')
    run_id = str(uuid5(NAMESPACE_URL, json.dumps([DAG, airflow_id], separators=(',', ':'))))
    conf = {'top_n': 3, 'test_fail_after_documents_once': fail_once,
            'test_fail_after_documents_always': fail_always}
    conf.update({'urls': urls} if urls else {'fixture_case': case})
    expected = 'success' if (urls or case in ('valid_vi', 'mixed')) and not fail_always else 'failed'
    run = {'airflow_run_id': airflow_id, 'run_id': run_id,
           'logical_date': timestamp.replace(microsecond=0).isoformat(),
           'conf': conf, 'evidence': EVIDENCE / airflow_id, 'expected': expected}
    scheduler('airflow', 'dags', 'unpause', DAG)
    scheduler('airflow', 'dags', 'trigger', DAG, '--run-id', airflow_id,
              '--logical-date', run['logical_date'], '--conf', json.dumps(conf))
    print(f'Triggered {airflow_id}; UUID={run_id}; conf={conf}', flush=True)
    await_run(run, observe_retry=fail_once)
    report, snapshot, logs = collect(run)
    report.update(scope='day4 quality; fixtures are handwritten, web runs explicitly labelled', source_sha256=hashes())
    save(run['evidence'] / 'initial/report.json', json.dumps(report, indent=2))
    assert run['state'] == expected, f"DAG {run['state']}, expected {expected}; see {run['evidence']}"
    for task in report['tasks']:
        target = 'success' if expected == 'success' or task['task_id'] in ('validate_config', 'crawl_to_bronze') else (
            'failed' if task['task_id'] == TASK else 'upstream_failed')
        assert task['state'] == target, task
    if urls:
        meta = snapshot['metadata']
        assert meta['quality_status'] == 'partial' and meta['input_count'] == 2
        assert meta['valid_count'] == meta['rejected_count'] == 1
        assert snapshot['quarantine'][0]['reason'] == 'http_404'
        scheduler('python', '-m', 'src.crawler.pipeline', 'verify', '--run-id', run_id)
    else:
        check_snapshot(run, snapshot)
    report['acceptance'] = 'PASS'
    save(run['evidence'] / 'initial/report.json', json.dumps(report, indent=2))
    print(f"PASS {case if not urls else 'real web partial'}: {expected}, {snapshot['metadata']['quality_status']}", flush=True)
    return run, snapshot


def accept():
    start_hashes = hashes()
    compose('run', '--rm', '--no-deps', 'lab-init')
    for day in (2, 3, 4):
        compose('cp', str(ROOT / f'tests/test_day{day}.py'), f'airflow-scheduler:/tmp/test_day{day}.py')
        output = scheduler('python', f'/tmp/test_day{day}.py', '-v')
        save(EVIDENCE / f'unit-day{day}.txt', output)
    errors = scheduler('airflow', 'dags', 'list-import-errors', '--output', 'json')
    save(EVIDENCE / 'dag-import-errors.json', errors)
    assert not json.loads(errors)
    normal, _ = trigger()
    mixed, before = trigger('mixed', fail_once=True)
    assert {q['reason'] for q in before['quarantine']} == {'empty_clean_text', 'http_404', 'missing_body'}
    assert (before['metadata']['input_count'], before['metadata']['valid_count'], before['metadata']['rejected_count']) == (5, 1, 4)
    scheduler('airflow', 'tasks', 'clear', DAG, '--start-date', mixed['logical_date'],
              '--end-date', mixed['logical_date'], '--yes')
    await_run(mixed)
    report, after, logs = collect(mixed, 'replay')
    assert mixed['state'] == 'success' and before == after
    assert 'TRANSFORM_NOOP' in logs and 'PUBLISH_NOOP' in logs
    assert all(t['state'] == 'success' and t['try_number'] >= 2 for t in report['tasks'])
    report.update(acceptance='PASS', scope='day4 partial replay', source_sha256=hashes())
    save(mixed['evidence'] / 'replay/report.json', json.dumps(report, indent=2))
    print('PASS exact partial replay: data, quarantine, checks, timestamps unchanged', flush=True)
    # mixed + all_invalid exercise each empty/script/HTTP/missing-body/no-token
    # page together, including the distinct zero-valid publication decision.
    cases = [trigger(case)[0] for case in ('all_invalid', 'unsupported_type')]
    system, _ = trigger(fail_always=True)
    output = scheduler('python', '/tmp/test_day4.py', '--database-guards', normal['run_id'])
    save(EVIDENCE / 'database-guards.txt', output)
    output = scheduler('python', '/tmp/test_day2.py', '--transactions', normal['run_id'])
    save(EVIDENCE / 'transaction-tests.txt', output)
    compose('cp', str(ROOT / 'tests/test_day4_spark.py'), 'airflow-scheduler:/tmp/test_day4_spark.py')
    output = scheduler('/opt/spark/bin/spark-submit', '--master', 'spark://spark-master:7077',
        '--deploy-mode', 'client', '--conf', 'spark.driver.host=airflow-scheduler',
        '--conf', 'spark.driver.bindAddress=0.0.0.0', '--conf', 'spark.driver.port=39001',
        '--conf', 'spark.blockManager.port=39002', '--conf', 'spark.cores.max=1',
        '--conf', 'spark.executor.cores=1', '--conf', 'spark.sql.shuffle.partitions=1',
        '--conf', 'spark.pyspark.python=/usr/local/bin/python', '/tmp/test_day4_spark.py')
    save(EVIDENCE / 'spark-policy-duplicate-tests.txt', output)
    for app_id in sorted(set(re.findall(r'application_id=(app-[A-Za-z0-9_-]+)', output))):
        executor = compose('exec', '-T', 'spark-worker', 'python', '-c',
            "from pathlib import Path; p=Path('/opt/spark/work/" + app_id + "'); "
            "[print(str(f)+'\\n'+f.read_text()) for f in sorted(p.rglob('*')) "
            "if f.is_file() and f.name in ('stdout','stderr')]")
        assert 'Executor' in executor and 'Running task' in executor
        save(EVIDENCE / f'policy-executor-{app_id}.txt', executor)
    web_config = json.loads((ROOT / 'config/day3_acceptance.json').read_text(encoding='utf-8'))
    web, _ = trigger(urls=[web_config['first_url'], web_config['error_url'], web_config['first_url']])
    compose('cp', str(ROOT / 'sql/queries/day4_quality.sql'), 'lab-db:/tmp/day4_quality.sql')
    output = compose('exec', '-T', 'lab-db', 'psql', '-X', '-v', 'ON_ERROR_STOP=1',
                     '-U', ENV.get('LAB_ADMIN_USER', 'lab_admin'), '-d', ENV.get('LAB_DB_NAME', 'lab'),
                     '-v', f"run_id={mixed['run_id']}", '-f', '/tmp/day4_quality.sql')
    save(EVIDENCE / 'quality-psql.txt', output)
    services = [{k: item.get(k) for k in ('Service', 'State', 'Health')}
                for item in map(json.loads, filter(str.strip, compose('ps', '--format', 'json').splitlines()))]
    assert len(services) == 7 and all(s['Health'] == 'healthy' for s in services)
    inventories = {service: json.loads(compose('exec', '-T', service, 'python', '-m', 'src.common.runtime_inventory'))
                   for service in ('airflow-scheduler', 'spark-worker')}
    comparable = lambda data: {k: v for k, v in data.items() if k != 'spark_submit'}
    assert comparable(inventories['airflow-scheduler']) == comparable(inventories['spark-worker'])
    save(EVIDENCE / 'runtime.json', json.dumps(inventories, indent=2))
    assert hashes() == start_hashes, 'Source changed during acceptance; rerun on stable code'
    summary = {'acceptance': 'PASS', 'checked_at_utc': datetime.now(timezone.utc).isoformat(),
               'scope': 'day4 quality/quarantine/partial/all-invalid/system failure; days5-7 pending',
               'normal_run': normal['run_id'], 'partial_retry_replay_run': mixed['run_id'],
               'invalid_runs': [r['run_id'] for r in cases], 'system_failure_run': system['run_id'],
               'web_partial_run': web['run_id'], 'services': services,
               'transaction_rollback_visibility': 'PASS', 'source_sha256': hashes(),
               'evidence_directories': [r['evidence'].relative_to(ROOT).as_posix() for r in [normal, mixed, *cases, system, web]]}
    save(EVIDENCE / 'latest.json', json.dumps(summary, indent=2))
    print('PASS day4: actual Airflow/Spark/SQL quality acceptance; evidence docs/evidence/day4/latest.json', flush=True)


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
        trigger(args.case)
    else:
        if not args.run_id:
            parser.error('inspect requires --run-id')
        print(json.dumps(inspect_output(str(UUID(args.run_id))), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, ValueError, AssertionError, subprocess.TimeoutExpired) as error:
        raise SystemExit(redact(str(error))) from None
