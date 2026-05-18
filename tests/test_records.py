from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from mermaid_timeline.records import InputFileError, iter_jsonl


class JsonlRecordTests(unittest.TestCase):
    def test_iter_jsonl_reports_file_line_column_and_preserves_cause(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            path = Path(tmp_name) / "bad.jsonl"
            path.write_text('{"ok": true}\n{"broken":\n', encoding="utf-8")

            with self.assertRaises(InputFileError) as cm:
                list(iter_jsonl(path))

            message = str(cm.exception)
            self.assertIn(f"file: {path.resolve()}", message)
            self.assertIn("line: 2", message)
            self.assertIn("column:", message)
            self.assertIn("expected: one JSON object per line", message)
            self.assertIsInstance(cm.exception.__cause__, json.JSONDecodeError)

    def test_iter_jsonl_reports_non_object_record_file_and_line(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            path = Path(tmp_name) / "bad.jsonl"
            path.write_text('{"ok": true}\n[]\n', encoding="utf-8")

            with self.assertRaises(InputFileError) as cm:
                list(iter_jsonl(path))

            message = str(cm.exception)
            self.assertIn(f"file: {path.resolve()}", message)
            self.assertIn("line: 2", message)
            self.assertIn("value: []", message)
            self.assertIn("expected: JSON object", message)


if __name__ == "__main__":
    unittest.main()
