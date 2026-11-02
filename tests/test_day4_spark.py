"""Standalone tests: configurable threshold, missing fields and duplicate read fault.

Executed with spark-submit on the same master/worker, never local[*].
Duplicate rows are injected at the JDBC read boundary because Bronze has a PK.
"""
from contextlib import closing
import json
import sys
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.readwriter import DataFrameReader

from src.common.pipeline import prepare_fixture, output_snapshot, publish_results
from src.common.quality import classify, load_rules
from src.common.smoke import connect
from src.jobs.clean_validate_count import main


def run():
    spark = SparkSession.builder.appName('day4-policy-tests').getOrCreate()
    assert spark.sparkContext.master == 'spark://spark-master:7077'
    frame = spark.createDataFrame([
        ('test', 'page-a', 'https://fixture.invalid/a', 'a' * 64,
         '<html><body>Xin chào</body></html>', 200, 'text/html', None),
        ('test', 'page-b', None, 'b' * 64,
         '<html><body>Xin chào</body></html>', 200, 'text/html', None),
        ('test', 'page-c', 'https://fixture.invalid/c', 'c' * 64,
         '<html><body>Xin chào</body></html>', 200, 'text/html', ''),
    ], 'run_id string,page_id string,source_url string,content_hash string,raw_html string,http_status int,content_type string,fetch_error string')
    rules, _ = load_rules()
    observations = classify(frame, rules).orderBy('page_id').collect()
    assert [(r.reject_reason, r.token_count) for r in observations] == [('', 2), ('missing_source', 2), ('fetch_error', 2)]
    rules['checks']['min_token_count'] = 3
    assert classify(frame, rules).orderBy('page_id').first().reject_reason == 'below_min_token_count'
    print('SPARK_THRESHOLD_AND_SOURCE_CHECK_PASS application_id=' + spark.sparkContext.applicationId, flush=True)
    spark.stop()

    prepared = prepare_fixture('day4_duplicate_test', str(uuid4()), dag_run=SimpleNamespace(conf={'fixture_case': 'valid_vi'}))
    run_id = prepared['run_id']
    original = DataFrameReader.jdbc

    def duplicate_read(reader, url, table, *args, **kwargs):
        result = original(reader, url, table, *args, **kwargs)
        return result.unionByName(result) if 'bronze.web_pages' in table else result

    with patch.object(DataFrameReader, 'jdbc', duplicate_read), patch.object(sys, 'argv',
            ['clean_validate_count.py', '--run-id', run_id, '--attempt-id', 'spark-1']):
        try:
            main()
            raise AssertionError('Duplicate Spark keys were accepted')
        except RuntimeError as error:
            assert str(error) == 'duplicate_key', str(error)
    snapshot = output_snapshot(run_id)
    assert snapshot['metadata']['publish_status'] == 'failed'
    assert snapshot['metadata']['quality_status'] == 'system_failure'
    assert not snapshot['documents'] and not snapshot['word_counts'] and not snapshot['quarantine']
    assert any(c['check_name'] == 'unique_keys' and c['status'] == 'fail' for c in snapshot['quality_checks'])
    try:
        publish_results(run_id)
        raise AssertionError('Duplicate run published')
    except RuntimeError as error:
        assert 'incomplete' in str(error)
    print('DUPLICATE_SQL_SNAPSHOT ' + json.dumps(snapshot), flush=True)
    print('SPARK_DUPLICATE_FAULT_PASS ' + json.dumps({'run_id': run_id, 'no_public_rows': True}), flush=True)


if __name__ == '__main__':
    run()
