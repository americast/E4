import contextlib
import io
import asyncio
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from e4.baselines.terminus.task import submit_diagnosis
from e4.baselines.terminus.terminus2 import ModelCallBudget


class SubmitDiagnosisResponseTests(unittest.TestCase):
    def test_response_symlink_is_rejected_without_following_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            requests = root / "requests"
            responses = root / "responses"
            requests.mkdir()
            responses.mkdir()
            secret = root / "outside.json"
            secret.write_text('{"accepted":true}')
            response = responses / "response-0123456789abcdef0123456789abcdef.json"
            try:
                response.symlink_to(secret)
            except OSError:
                self.skipTest("symlink creation unavailable")
            with patch.object(submit_diagnosis, "REQUEST_ROOT", requests), \
                    patch.object(submit_diagnosis, "RESPONSE_ROOT", responses), \
                    patch.object(submit_diagnosis.uuid, "uuid4") as uuid4, \
                    patch.object(submit_diagnosis, "WAIT_SECONDS", 1):
                uuid4.return_value.hex = "0123456789abcdef0123456789abcdef"
                errors = io.StringIO()
                with contextlib.redirect_stderr(errors):
                    result = submit_diagnosis.main(["-"])
            self.assertEqual(result, 3)
            self.assertIn("cannot be opened safely", errors.getvalue())
            self.assertEqual(secret.read_text(), '{"accepted":true}')

    def test_native_agent_gets_local_finalization_after_evaluation_is_terminal(self):
        class FakeResponse:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class FakeUsage:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        llms = types.ModuleType("harbor.llms")
        base = types.ModuleType("harbor.llms.base")
        base.LLMResponse = FakeResponse
        models = types.ModuleType("harbor.models")
        metric = types.ModuleType("harbor.models.metric")
        metric.UsageInfo = FakeUsage
        modules = {module.__name__: module for module in (llms, base, models, metric)}

        class FakeLLM:
            async def call(self, **_kwargs):
                raise AssertionError("terminal run must not request another model generation")

        class FakeAgent:
            _model_name = "fake-gemma"
            _llm = FakeLLM()

        agent = FakeAgent()
        budget = ModelCallBudget(agent, terminal_predicate=lambda: True,
                                 terminal_reason=lambda: "accepted")
        try:
            with patch.dict(sys.modules, modules):
                response = asyncio.run(agent._llm.call())
            self.assertIn('"task_complete":true', response.content)
            self.assertEqual(response.extra["termination_reason"], "accepted")
            self.assertEqual(budget.terminal_finalization_responses, 1)
        finally:
            budget.close()


if __name__ == "__main__":
    unittest.main()
