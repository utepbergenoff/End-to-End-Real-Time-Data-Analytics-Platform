import tempfile
import unittest
from pathlib import Path

from core import Processor, evaluate_anomalies, generate, train_and_evaluate, validate


class CoreTests(unittest.TestCase):
    def test_generator_is_reproducible_and_ordered(self):
        a = list(generate(100))
        self.assertEqual(a, list(generate(100)))
        self.assertEqual(len(a), 300)
        self.assertEqual([x["room_id"] for x in a[:3]], ["A101", "B202", "C303"])
        self.assertEqual(len({x["event_id"] for x in a}), 300)

    def test_validation_and_cleaning(self):
        event = next(generate())
        self.assertEqual(validate(event), (True, "ok"))
        bad = dict(event, co2_ppm=9000)
        self.assertFalse(validate(bad)[0])
        missing = dict(event, temperature_c=None)
        result = Processor().process(missing)
        self.assertEqual(result["temperature_c"], 22.0)
        self.assertEqual(result["cleaned_fields"], ["temperature_c"])

    def test_models_and_anomaly_evaluation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            ml = train_and_evaluate(path, minutes=240)
            anomaly = evaluate_anomalies(path, minutes=240)
            self.assertGreater(ml["test_pairs"], 0)
            self.assertGreater(anomaly["labeled_spikes"], 0)
            self.assertTrue((path / "co2_model.joblib").exists())
            self.assertGreaterEqual(anomaly["recall"], .8)


if __name__ == "__main__":
    unittest.main()
