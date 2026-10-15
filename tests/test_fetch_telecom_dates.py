import hashlib
import tempfile
import unittest
from pathlib import Path

from scripts.fetch_telecom_dates import transfer, verify_entry_bytes


class TelecomFetcherIntegrityTests(unittest.TestCase):
    def test_accepts_git_blob_and_lfs_hashes(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "data.csv"
            payload = b"a,b\n1,2\n"
            path.write_bytes(payload)
            blob_oid = hashlib.sha1(b"blob " + str(len(payload)).encode() + b"\0" + payload).hexdigest()
            actual, method = verify_entry_bytes(path, {"size": len(payload), "oid": blob_oid})
            self.assertEqual(actual, hashlib.sha256(payload).hexdigest())
            self.assertEqual(method, "hub_git_blob_sha1")
            actual, method = verify_entry_bytes(path, {"size": len(payload),
                                                       "lfs": {"oid": "sha256:" + hashlib.sha256(payload).hexdigest()}})
            self.assertEqual(method, "hub_lfs_sha256")

    def test_rejects_bad_git_blob_and_non_commit_revision(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "data.csv"
            path.write_bytes(b"payload")
            with self.assertRaisesRegex(RuntimeError, "Git blob SHA-1 mismatch"):
                verify_entry_bytes(path, {"size": 7, "oid": "0" * 40})
            with self.assertRaisesRegex(ValueError, "40-character commit"):
                transfer({"path": "file.csv", "size": 0, "oid": "0" * 40},
                         "tracer-cloud/opensre", "main", path.with_name("target.csv"))


if __name__ == "__main__":
    unittest.main()
