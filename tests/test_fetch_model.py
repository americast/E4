import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.fetch_model import fetch, resolve


class FakeResponse:
    def __init__(self, body, status=200):
        self.body = body
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self, size=-1):
        if size < 0:
            data, self.body = self.body, b""
        else:
            data, self.body = self.body[:size], self.body[size:]
        return data


class FetchModelTests(unittest.TestCase):
    def metadata(self, payload):
        class Response(FakeResponse):
            def read(self, size=-1):
                return super().read(size)

        return Response(__import__("json").dumps(payload).encode())

    def test_resolve_pins_commit_size_and_lfs_sha(self):
        payload = {"sha": "commit123", "siblings": [
            {"rfilename": "weights.gguf", "lfs": {"sha256": "a" * 64, "size": 123}}
        ]}
        data = {"repository": "org/model", "filename": "weights.gguf"}
        with patch("scripts.fetch_model.urllib.request.urlopen", return_value=self.metadata(payload)):
            result = resolve(data)
        self.assertEqual((result["revision"], result["sha256"], result["size_bytes"]),
                         ("commit123", "a" * 64, 123))

    def test_resumes_partial_file_and_verifies_digest(self):
        content = b"abcdef"
        data = {"repository": "org/model", "filename": "weights.gguf", "revision": "rev",
                "sha256": hashlib.sha256(content).hexdigest(), "size_bytes": len(content)}
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "weights.gguf"
            target.with_name(target.name + ".part").write_bytes(b"ab")
            with patch("scripts.fetch_model.urllib.request.urlopen", return_value=FakeResponse(b"cdef", 206)) as urlopen:
                fetch(data, target)
            self.assertEqual(urlopen.call_args.args[0].headers["Range"], "bytes=2-")
            self.assertEqual(target.read_bytes(), content)

    def test_restarts_when_server_ignores_range_and_keeps_bad_hash_partial(self):
        content = b"abcdef"
        data = {"repository": "org/model", "filename": "weights.gguf", "revision": "rev",
                "sha256": hashlib.sha256(content).hexdigest(), "size_bytes": len(content)}
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "weights.gguf"
            target.with_name(target.name + ".part").write_bytes(b"bad")
            with patch("scripts.fetch_model.urllib.request.urlopen", return_value=FakeResponse(content, 200)):
                fetch(data, target)
            self.assertEqual(target.read_bytes(), content)

            target.unlink()
            data["sha256"] = "0" * 64
            with patch("scripts.fetch_model.urllib.request.urlopen", return_value=FakeResponse(content, 200)):
                with self.assertRaisesRegex(RuntimeError, "SHA-256 mismatch"):
                    fetch(data, target)
            self.assertTrue(target.with_name(target.name + ".part").exists())


if __name__ == "__main__":
    unittest.main()
