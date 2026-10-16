import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('inventory', Path(__file__).with_name('input-inventory.py'))
inventory = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inventory)

class InventoryTest(unittest.TestCase):
    def test_missing_and_bootstrap_never_claim_complete_history(self):
        identity = {'kind': 'identity', 'server_uuid': 'test-server'}
        empty = inventory.classify([identity], 'test-server')
        self.assertEqual(empty['unlisted_source_ranges'], 'unknown')
        self.assertFalse(empty['full_history_certified'])
        rows = [identity, {'kind': 'plan', 'state': 5}, {'kind': 'unreadable', 'key_hash': 'original'}]
        result = inventory.classify(rows, 'test-server')
        self.assertEqual(result['plans'][0]['classification'], 'verified_rows_only')
        self.assertEqual(result['unreadable'][0]['key_hash'], 'original')
        self.assertFalse(result['full_history_certified'])

    def test_identity_overflow_and_publish_fail_closed(self):
        with self.assertRaises(ValueError):
            inventory.classify([{'kind': 'identity', 'server_uuid': 'wrong'}], 'test-server')
        with self.assertRaises(ValueError):
            inventory.classify([{'kind': 'identity', 'server_uuid': 'test-server'}] + [{'kind': 'plan', 'state': 2}] * (inventory.ROW_LIMIT + 1), 'test-server')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'report.json'
            inventory.publish(path, {'proof': 'original'})
            before = path.read_bytes()
            with self.assertRaises(ValueError):
                inventory.publish(path, {'proof': 'replacement'})
            self.assertEqual(path.read_bytes(), before)

if __name__ == '__main__':
    unittest.main()
