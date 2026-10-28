import json
import tempfile
import unittest
from pathlib import Path

from e4.contracts import EntityRef, RankedAnswer, ScoredCandidate
from e4.judge.bank import DevOnlyFeedbackEvaluator, FeedbackInputError, FeedbackUnavailable


FIRST = "bank-20990101T0000-utc8"
SECOND = "bank-20990102T0000-utc8"


def answer(components):
    leads = []
    for index, item in enumerate(components):
        component = item[0] if isinstance(item, tuple) else item
        category = item[1] if isinstance(item, tuple) else None
        leads.append(ScoredCandidate(EntityRef("component", component), 1.0 - index * 0.1,
                                     category, ()))
    return RankedAnswer(tuple(leads))


def write_gold(root, rows):
    path = root / "data/prepared/bank/evaluator/dev_ground_truth.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows))
    return path


class DevOnlyFeedbackTests(unittest.TestCase):
    def test_child_process_accepts_casefolded_top_three_component_and_returns_only_bool(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_gold(root, [
                {"incident_id": FIRST, "split": "dev", "ground_truth": [
                    {"component": "MG01", "reason": "private label", "timestamp_epoch_seconds": 123}]},
                {"incident_id": SECOND, "split": "dev", "ground_truth": [{"component": "Mysql02"}]},
            ])
            evaluator = DevOnlyFeedbackEvaluator(root=root, allowed_case_ids={FIRST, SECOND})
            result = evaluator.evaluate(FIRST, answer(["other", "mg01", "else"]))
            self.assertEqual(result, {"accepted": True})
            self.assertEqual(set(result), {"accepted"})
            self.assertIs(type(result["accepted"]), bool)

    def test_rank_four_component_is_a_rejection_and_category_is_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_gold(root, [{"incident_id": FIRST, "split": "dev",
                "ground_truth": [{"component": "MG01", "reason": "private reason"}]}])
            evaluator = DevOnlyFeedbackEvaluator(root=root, allowed_case_ids={FIRST})
            ranked = answer([("A", "CPU fault"), ("B", "network loss"),
                             ("C", "db close"), ("mg01", "network delay")])
            self.assertEqual(evaluator.evaluate(FIRST, ranked), {"accepted": False})

    def test_empty_answer_is_valid_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_gold(root, [{"incident_id": FIRST, "split": "dev",
                "ground_truth": [{"component": "MG01"}]}])
            evaluator = DevOnlyFeedbackEvaluator(root=root, allowed_case_ids={FIRST})
            self.assertEqual(evaluator.evaluate(FIRST, answer([])), {"accepted": False})

    def test_scored_or_unlisted_case_is_rejected_before_gold_process(self):
        with tempfile.TemporaryDirectory() as directory:
            evaluator = DevOnlyFeedbackEvaluator(root=directory, allowed_case_ids={FIRST})
            with self.assertRaises(FeedbackInputError):
                evaluator.evaluate("bank-20990103T0000-utc8", answer(["MG01"]))

    def test_missing_or_malformed_dev_gold_is_fatal_not_a_diagnosis_miss(self):
        for write_content in (None, "not-json\n"):
            with self.subTest(write_content=write_content), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                if write_content is not None:
                    path = root / "data/prepared/bank/evaluator/dev_ground_truth.jsonl"
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(write_content)
                evaluator = DevOnlyFeedbackEvaluator(root=root, allowed_case_ids={FIRST})
                with self.assertRaises(FeedbackUnavailable):
                    evaluator.evaluate(FIRST, answer(["wrong"]))

    def test_missing_case_or_non_dev_row_is_fatal(self):
        bad_rows = [
            [{"incident_id": SECOND, "split": "dev", "ground_truth": [{"component": "Mysql02"}]}],
            [{"incident_id": FIRST, "split": "scored", "ground_truth": [{"component": "MG01"}]}],
            [{"incident_id": FIRST, "split": "dev", "ground_truth": []}],
        ]
        for rows in bad_rows:
            with self.subTest(rows=rows), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); write_gold(root, rows)
                evaluator = DevOnlyFeedbackEvaluator(root=root, allowed_case_ids={FIRST})
                with self.assertRaises(FeedbackUnavailable):
                    evaluator.evaluate(FIRST, answer(["wrong"]))

    def test_malformed_contract_and_gold_path_symlink_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evaluator = DevOnlyFeedbackEvaluator(root=root, allowed_case_ids={FIRST})
            with self.assertRaises(FeedbackInputError):
                evaluator.evaluate(FIRST, {"leads": []})
            evaluator_path = root / "data/prepared/bank/evaluator/dev_ground_truth.jsonl"
            evaluator_path.parent.mkdir(parents=True, exist_ok=True)
            outside = root / "outside.jsonl"
            outside.write_text(json.dumps({"incident_id": FIRST, "split": "dev",
                                           "ground_truth": [{"component": "MG01"}]}) + "\n")
            try:
                evaluator_path.symlink_to(outside)
            except OSError:
                self.skipTest("symlink creation unavailable in this environment")
            with self.assertRaises(FeedbackUnavailable):
                evaluator.evaluate(FIRST, answer(["MG01"]))


if __name__ == "__main__":
    unittest.main()
