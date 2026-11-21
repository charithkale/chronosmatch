import os
import tempfile
import unittest

from chronosmatch.pipeline import run_pipeline


class PipelineTests(unittest.TestCase):
    def test_end_to_end_and_ledger(self):
        db = os.path.join(tempfile.mkdtemp(), "ledger.db")
        s = run_pipeline(rate=20_000, duration=2.0, ui="none", db_path=db)
        e = s["engine"]
        self.assertGreater(e["orders"], 10_000)
        self.assertGreater(e["trades"], 0)
        self.assertEqual(e["orders"], s["feed"]["sent"])              # nothing lost on the bus
        self.assertEqual(s["ledger"]["rows"], e["trades"] - e["ledger_dropped"])
        self.assertGreater(s["ledger"]["p50_us"], 0)


if __name__ == "__main__":
    unittest.main()
