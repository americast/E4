"""Separate-process development evaluator that returns one binary feedback bit."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


CASE_ID = re.compile(r"^bank-[0-9]{8}T[0-9]{4}-utc8$")
MAX_INPUT_BYTES = 16_384
MAX_COMPONENT_CHARS = 256


class FeedbackUnavailable(RuntimeError):
    """The trusted evaluator could not produce a valid binary result."""


class FeedbackInputError(ValueError):
    """Invalid or out-of-scope evaluator input."""


class DevOnlyFeedbackEvaluator:
    """Evaluate frozen development cases in a trusted stdlib-only child process.

    The caller receives only ``{"accepted": bool}``. Diagnostic code and the
    local model must run outside this evaluator process and must not receive the
    evaluator/raw-label mount.
    """

    def __init__(self, *, root: Path | str,
                 allowed_case_ids: Iterable[str], timeout_seconds: float = 5.0):
        self.root = Path(root).resolve()
        self.gold_path = self.root / "data/prepared/bank/evaluator/dev_ground_truth.jsonl"
        self.allowed_case_ids = frozenset(allowed_case_ids)
        if not self.allowed_case_ids or any(not isinstance(case, str) or not CASE_ID.fullmatch(case)
                                            for case in self.allowed_case_ids):
            raise FeedbackInputError("development evaluator requires an explicit frozen dev case allowlist")
        if type(timeout_seconds) not in (int, float) or timeout_seconds <= 0 or timeout_seconds > 30:
            raise FeedbackInputError("evaluation timeout must be between 0 and 30 seconds")
        self.timeout_seconds = float(timeout_seconds)

    def evaluate(self, case_id: str, ranked_answer: Any) -> dict[str, bool]:
        """Return one accepted bit for component-only top-three matching."""
        if not isinstance(case_id, str) or case_id not in self.allowed_case_ids:
            raise FeedbackInputError("case is outside the frozen development allowlist")
        from .contracts import RankedAnswer
        if not isinstance(ranked_answer, RankedAnswer):
            raise FeedbackInputError("feedback requires a trusted RankedAnswer contract")
        leads = []
        for candidate in ranked_answer.leads[:3]:
            component = candidate.entity.identifier
            if not isinstance(component, str) or not component.strip() or len(component) > MAX_COMPONENT_CHARS:
                raise FeedbackInputError("top-three component identity is malformed")
            leads.append(component)
        payload = json.dumps({"case_id": case_id, "components": leads},
                             ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
        if len(payload) > MAX_INPUT_BYTES:
            raise FeedbackInputError("feedback input exceeds its fixed size limit")
        child_env = {"PATH": os.environ.get("PATH", ""),
                     "PYTHONDONTWRITEBYTECODE": "1", "PYTHONIOENCODING": "utf-8"}
        command = [sys.executable, "-I", "-S", str(Path(__file__).resolve()),
                   "--trusted-child", "--dev-gold"]
        try:
            result = subprocess.run(command, input=payload, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL, cwd=str(self.root), env=child_env,
                timeout=self.timeout_seconds, check=False)
        except Exception:
            raise FeedbackUnavailable("trusted feedback process failed") from None
        if result.returncode != 0 or len(result.stdout) > 128:
            raise FeedbackUnavailable("trusted feedback process failed")
        try:
            response = json.loads(result.stdout)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise FeedbackUnavailable("trusted feedback process returned invalid output") from None
        if (not isinstance(response, dict) or set(response) != {"accepted"}
                or type(response["accepted"]) is not bool):
            raise FeedbackUnavailable("trusted feedback process returned invalid output")
        return {"accepted": response["accepted"]}


def _trusted_child(root: Path) -> int:
    """Read only the fixed development gold file and emit a single bool object."""
    try:
        request_bytes = sys.stdin.buffer.read(MAX_INPUT_BYTES + 1)
        if len(request_bytes) > MAX_INPUT_BYTES:
            return 2
        request = json.loads(request_bytes)
        if (not isinstance(request, dict) or set(request) != {"case_id", "components"}
                or not isinstance(request.get("case_id"), str)
                or not CASE_ID.fullmatch(request["case_id"])
                or not isinstance(request.get("components"), list)
                or len(request["components"]) > 3
                or any(not isinstance(item, str) or not item.strip() or len(item) > MAX_COMPONENT_CHARS
                       for item in request["components"])):
            return 2
        gold_path = root / "data/prepared/bank/evaluator/dev_ground_truth.jsonl"
        # Exact fixed development path; reject path substitution and symlinks.
        if gold_path.is_symlink() or gold_path.resolve() != gold_path.absolute() or not gold_path.is_file():
            return 2
        target_components: set[str] = set()
        matched_rows = 0
        for line in gold_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (not isinstance(row, dict) or row.get("split") != "dev"
                    or not isinstance(row.get("incident_id"), str)
                    or not CASE_ID.fullmatch(row["incident_id"])):
                return 2
            targets = row.get("ground_truth")
            if not isinstance(targets, list) or not targets:
                return 2
            row_components = set()
            for item in targets:
                component = item.get("component") if isinstance(item, dict) else None
                if not isinstance(component, str) or not component.strip() or len(component) > MAX_COMPONENT_CHARS:
                    return 2
                row_components.add(component.strip().casefold())
            if row["incident_id"] == request["case_id"]:
                matched_rows += 1
                target_components.update(row_components)
        if matched_rows != 1 or not target_components:
            return 2
        accepted = any(component.strip().casefold() in target_components
                                 for component in request["components"])
        return _emit(bool(accepted))
    except BaseException:
        return 2


def _emit(accepted: bool) -> int:
    sys.stdout.write(json.dumps({"accepted": bool(accepted)}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--trusted-child" and sys.argv[2] == "--dev-gold":
        # The project root arrives as cwd, never as a caller-controlled gold path.
        raise SystemExit(_trusted_child(Path.cwd()))
    raise SystemExit(2)
