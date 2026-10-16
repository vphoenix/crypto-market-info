import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('prepare_restore', Path(__file__).with_name('prepare-restore.py'))
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class RestoreFenceTests(unittest.TestCase):
    def fixture(self, directory):
        root = Path(directory)
        state = root / 'runtime'
        state.mkdir()
        (state / 'runtime.lock').touch()
        tool = root / 'runtime-state'
        tool.write_bytes(b'test binary; never executed')
        inventory = root / 'inventory.json'
        item = {'target': 'a93e84b2-c7c3-11f1-bf43-7ab521822a4d/test',
                'epoch_file': str(root / 'epoch'), 'expected_epoch': 'before',
                'services': ['crypto-market-info-across.service'],
                'state_directories': [str(state)]}
        inventory.write_text(json.dumps({'version': 1, 'targets': [item]}))
        return inventory, tool, MODULE.digest(tool), root / 'evidence', item

    def test_active_service_prevents_any_epoch_change(self):
        with tempfile.TemporaryDirectory() as directory:
            inventory, tool, digest, output, _ = self.fixture(directory)
            with patch.object(MODULE, 'stopped', side_effect=RuntimeError('active')), patch.object(MODULE, 'epoch') as epoch:
                with self.assertRaisesRegex(RuntimeError, 'active'):
                    MODULE.prepare(inventory, tool, digest, output, 'restore test')
                epoch.assert_not_called()

    def test_rotation_survives_lost_result_and_resumes_without_second_rotation(self):
        with tempfile.TemporaryDirectory() as directory:
            inventory, tool, digest, output, item = self.fixture(directory)
            state = {'Target': item['target'], 'ID': 'before'}
            rotations = []

            def epoch(_tool, operation, _item, *extra):
                if operation == 'epoch-rotate':
                    rotations.append(operation)
                    state.update(ID='after', Previous='before', Reason=extra[3])
                    raise RuntimeError('result lost after durable rotation')
                return dict(state)

            with patch.object(MODULE, 'stopped', return_value={'MainPID': '0', 'ActiveState': 'inactive'}), patch.object(MODULE, 'epoch', side_effect=epoch):
                with self.assertRaisesRegex(RuntimeError, 'result lost'):
                    MODULE.prepare(inventory, tool, digest, output, 'restore test')
                report = MODULE.prepare(inventory, tool, digest, output, 'restore test')
            self.assertEqual(len(rotations), 1)
            self.assertEqual(report['targets'][item['target']]['new_epoch'], 'after')
            self.assertFalse(report['database_restored'])
            self.assertTrue(Path(item['state_directories'][0]).exists())

    def test_unknown_epoch_cannot_be_relabelled_as_this_restore(self):
        with tempfile.TemporaryDirectory() as directory:
            inventory, tool, digest, output, item = self.fixture(directory)
            with patch.object(MODULE, 'stopped', return_value={}), patch.object(MODULE, 'epoch', return_value={'Target': item['target'], 'ID': 'unrelated'}):
                with self.assertRaisesRegex(RuntimeError, 'outside this restore job'):
                    MODULE.prepare(inventory, tool, digest, output, 'restore test')


if __name__ == '__main__':
    unittest.main()
