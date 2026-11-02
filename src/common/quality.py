"""Strict versioned policy and Spark quality expressions; no Python word counts."""
import hashlib
import json
from pathlib import Path

import yaml

from src.common.transform import RULES_VERSION, TRANSFORM_VERSION, extract_html, tokenize

RULES_PATH = Path(__file__).resolve().parents[2] / 'config/quality_rules.yaml'


def load_rules(path=RULES_PATH):
    rules = yaml.safe_load(Path(path).read_text(encoding='utf-8'))
    # Fixed algorithms are explicit contracts, never silently ignored YAML knobs.
    expected = {
        'rules_version': RULES_VERSION, 'transform_version': TRANSFORM_VERSION,
        'extraction': {'parser': 'html.parser', 'text_scope': 'body',
                       'remove_elements': ['script', 'style', 'noscript', 'template'],
                       'unicode_normalization': 'NFC', 'collapse_whitespace': True},
        'tokenization': {'lowercase': True, 'unicode_categories': ['L', 'Nd'],
                         'keep_numbers': True, 'remove_stop_words': False,
                         'stemming': False, 'lemmatization': False},
        'publish': {'partial_valid': 'publish_valid_with_partial_status',
                    'all_invalid': 'fail_without_publish', 'system_failure': 'fail_without_publish'},
        'display': {'default_top_n': 3, 'max_top_n': 100, 'order': ['count_desc', 'word_asc']},
    }
    if not isinstance(rules, dict) or set(rules) != set(expected) | {'checks'}:
        raise ValueError('Invalid quality rules structure')
    for key, value in expected.items():
        if json.dumps(rules[key], sort_keys=True) != json.dumps(value, sort_keys=True):
            raise ValueError(f'Unsupported quality policy: {key}')
    checks = rules['checks']
    fixed = {'required_fields': ['page_id', 'source_url'], 'require_body': True,
             'require_nonempty_clean_text': True, 'duplicate_key_policy': 'fail_run',
             'require_input_equals_valid_plus_rejected': True, 'require_positive_word_counts': True,
             'require_word_count_sum_equals_token_count': True}
    dynamic = {'http_status_min', 'http_status_max', 'supported_content_types', 'min_token_count'}
    if not isinstance(checks, dict) or set(checks) != set(fixed) | dynamic:
        raise ValueError('Invalid quality checks structure')
    if any(json.dumps(checks[k], sort_keys=True) != json.dumps(v, sort_keys=True) for k, v in fixed.items()):
        raise ValueError('Unsupported mandatory quality check policy')
    for key in ('http_status_min', 'http_status_max', 'min_token_count'):
        if type(checks[key]) is not int:
            raise ValueError(f'Invalid integer quality threshold: {key}')
    if not 200 <= checks['http_status_min'] <= checks['http_status_max'] <= 299 or checks['min_token_count'] < 1:
        raise ValueError('Invalid quality threshold range')
    types = checks['supported_content_types']
    if not isinstance(types, list) or not types or any(t not in ('text/html', 'application/xhtml+xml') for t in types):
        raise ValueError('Unsupported quality content types')
    digest = hashlib.sha256(json.dumps(rules, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return rules, digest


def classify(bronze, rules):
    """Checks and transformation execute on the Spark executor."""
    from pyspark.sql import functions as F, types as T
    checks = rules['checks']
    schema = T.StructType([T.StructField(n, T.StringType(), False) for n in ('title', 'clean_text', 'reason')])
    frame = (bronze.withColumn('parsed', F.udf(extract_html, schema)('raw_html'))
             .withColumn('tokens', F.udf(tokenize, T.ArrayType(T.StringType(), False))('parsed.clean_text'))
             .withColumn('token_count', F.size('tokens').cast('long')))
    conditions = [
        ('required_fields', F.col('page_id').isNotNull() & (F.length(F.trim('page_id')) > 0) &
         F.col('source_url').isNotNull() & (F.length(F.trim('source_url')) > 0), 'missing_source'),
        ('fetch_success', F.col('fetch_error').isNull(),
         F.when(F.length(F.trim('fetch_error')) > 0, F.col('fetch_error')).otherwise(F.lit('fetch_error'))),
        ('http_status', F.col('http_status').between(checks['http_status_min'], checks['http_status_max']), 'http_error'),
        ('content_type', F.lower(F.trim(F.split('content_type', ';').getItem(0))).isin(checks['supported_content_types']),
         'unsupported_content_type'),
        ('parse_body', F.col('parsed.reason') != 'missing_body', 'missing_body'),
        ('nonempty_clean_text', F.length(F.trim('parsed.clean_text')) > 0, 'empty_clean_text'),
        ('min_token_count', F.col('token_count') >= checks['min_token_count'],
         F.when(F.col('token_count') == 0, F.lit('no_tokens')).otherwise(F.lit('below_min_token_count'))),
        ('content_hash', F.col('content_hash').rlike('^[0-9a-f]{64}$'), 'missing_content_hash'),
    ]
    reason = F.lit('')
    for name, condition, failure in reversed(conditions):
        reason = F.when(~F.coalesce(condition, F.lit(False)), failure).otherwise(reason)
    frame = frame.withColumn('reject_reason', reason)
    for name, condition, _ in conditions:
        frame = frame.withColumn('check_' + name, F.coalesce(condition, F.lit(False)))
    return frame


def record_check(cur, run_id, attempt, scope, name, passed, observed, threshold):
    cur.execute('''INSERT INTO quality.check_results
        (run_id,attempt_id,scope_key,check_name,status,observed_value,threshold,rules_version)
        VALUES (%s::uuid,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (run_id,attempt_id,scope_key,check_name) DO UPDATE SET
        status=excluded.status,observed_value=excluded.observed_value,
        threshold=excluded.threshold,checked_at=now()''',
        (run_id, attempt, scope, name, 'pass' if passed else 'fail',
         json.dumps(observed, ensure_ascii=False), json.dumps(threshold, ensure_ascii=False), RULES_VERSION))


def quality_snapshot(cur, run_id):
    result = {}
    for key, table, order in [('quality_checks', 'quality.check_results', 'attempt_id,scope_key,check_name'),
                              ('quarantine', 'quarantine.rejected_pages', 'page_id')]:
        cur.execute(f'SELECT row_to_json(t) FROM {table} t WHERE run_id=%s::uuid ORDER BY {order}', (run_id,))
        result[key] = [r[0] for r in cur.fetchall()]
    return result
