"""Unit-test scientific helpers without importing the study's CLI/input side effects."""
import ast
import pathlib
import unittest
import numpy as np
import pandas as pd

source = pathlib.Path(__file__).parents[1] / "scripts" / "analyze-btc-impulses.py"
tree = ast.parse(source.read_text())
helpers = ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef)], type_ignores=[])
namespace = {"np": np, "pd": pd, "capture": {"cutoff": 2000000000000}}
exec(compile(helpers, str(source), "exec"), namespace)


class StudyHelpersTest(unittest.TestCase):
    def bars(self, n=240):
        start = 1767225600000
        return [[start+i*3600000, 100+i*.1, 103+i*.1, 98+i*.1, 101+i*.1,
            100, start+(i+1)*3600000-1, 10000, 20, 70, 7000, 0] for i in range(n)]

    def test_taker_volume_is_actual_executed_difference(self):
        data = namespace["frame"](self.bars(2))
        self.assertEqual(data.iloc[0].delta, 40)
        self.assertEqual(data.iloc[0].delta_quote, 4000)
        invalid = self.bars(2)
        invalid[0][9] = 110
        with self.assertRaisesRegex(ValueError, "accounting"):
            namespace["frame"](invalid)

    def test_wilder_seeds_with_sma_not_future_data(self):
        result = namespace["wilder"](pd.Series([1., 2., 3., 4., 5.]), 3)
        self.assertTrue(result.iloc[:2].isna().all())
        self.assertEqual(result.iloc[2], 2)
        self.assertAlmostEqual(result.iloc[3], 8/3)
        self.assertAlmostEqual(result.iloc[4], 31/9)

    def test_completed_four_hour_periods_keep_millisecond_units(self):
        raw = namespace["frame"](self.bars(10))
        result = namespace["resampled"](raw, 4)
        start = raw.iloc[0].time
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0].end, start+4*3600000-1)
        self.assertEqual(result.iloc[1].end, start+8*3600000-1)
        eligible = result[result.end < start+5*3600000]
        self.assertEqual(len(eligible), 1)
        self.assertEqual(eligible.iloc[0].close, raw.iloc[3].close)

    def test_indicators_and_resampling_are_prefix_causal(self):
        raw = namespace["frame"](self.bars())
        full = namespace["indicators"](raw)
        prefix = namespace["indicators"](raw.iloc[:100])
        pd.testing.assert_frame_equal(full.iloc[:100], prefix)
        h4_full = namespace["resampled"](raw, 4)
        h4_prefix = namespace["resampled"](raw.iloc[:101], 4)
        pd.testing.assert_frame_equal(h4_full.iloc[:25].reset_index(drop=True), h4_prefix)

    def test_asof_metrics_lag_excludes_future_and_stale_samples(self):
        cutoff = 1767225600000
        namespace["metrics"] = pd.DataFrame({"stamp": [cutoff-10*60000, cutoff-5*60000, cutoff, cutoff+5*60000],
            "sum_open_interest": [100, 110, 120, 130]})
        self.assertEqual(namespace["snapshot"](cutoff, 5).sum_open_interest, 110)
        self.assertEqual(namespace["snapshot"](cutoff, 10).sum_open_interest, 100)
        self.assertIsNone(namespace["snapshot"](cutoff+3600000, 5))


if __name__ == "__main__":
    unittest.main()
