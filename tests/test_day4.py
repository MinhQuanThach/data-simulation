"""Policy tests and real PostgreSQL guard checks; no external web dependency."""
from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path
import sys
import tempfile
import unittest

import psycopg2
import yaml

from src.common.quality import load_rules
from src.common.pipeline import output_snapshot, publish_results
from src.common.smoke import connect
from src.crawler.config import validate_input


class PolicyTests(unittest.TestCase):
    def changed(self, rules):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'rules.yaml'
            path.write_text(yaml.safe_dump(rules), encoding='utf-8')
            return load_rules(path)

    def test_version_and_hash(self):
        rules, digest = load_rules()
        self.assertEqual(rules['rules_version'], '1.1.0')
        self.assertEqual(self.changed(rules)[1], digest)

    def test_threshold_is_effective_configuration(self):
        rules, digest = load_rules()
        rules['checks']['min_token_count'] = 13
        modified, other = self.changed(rules)
        self.assertEqual(modified['checks']['min_token_count'], 13)
        self.assertNotEqual(digest, other)

    def test_unsupported_or_disabled_policy_fails(self):
        original, _ = load_rules()
        for section, key, value in [('checks', 'duplicate_key_policy', 'ignore'),
                                    ('checks', 'min_token_count', True),
                                    ('checks', 'require_nonempty_clean_text', False),
                                    ('publish', 'all_invalid', 'success'),
                                    ('extraction', 'text_scope', 'main')]:
            rules = deepcopy(original)
            rules[section][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.changed(rules)

    def test_unknown_and_version_changes_fail(self):
        rules, _ = load_rules()
        for key, value in [('rules_version', '99.0.0'), ('typo', {})]:
            changed = dict(rules, **{key: value})
            with self.assertRaises(ValueError):
                self.changed(changed)

    def test_injection_and_dedup(self):
        with self.assertRaises(ValueError):
            validate_input({'test_fail_after_documents_once': True, 'test_fail_after_documents_always': True})
        c = validate_input({'urls': ['https://example.com#1', 'https://example.com#2']})
        self.assertEqual((c['requested_count'], c['unique_input_count']), (2, 1))


def database_guards(run_id):
    before = output_snapshot(run_id)
    with closing(connect('CRAWLER')) as conn:
        try:
            with conn, conn.cursor() as cur:
                cur.execute('''INSERT INTO bronze.web_pages SELECT * FROM bronze.web_pages
                    WHERE run_id=%s::uuid LIMIT 1''', (run_id,))
            raise AssertionError('Duplicate Bronze key accepted')
        except psycopg2.errors.UniqueViolation:
            pass
    assert output_snapshot(run_id) == before
    assert publish_results(run_id)['noop']
    print(json.dumps({'duplicate_bronze_key_denied': True, 'published_snapshot_unchanged': True}))


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--database-guards':
        database_guards(sys.argv[2])
    else:
        unittest.main(testRunner=unittest.TextTestRunner(stream=sys.stdout, verbosity=2))
