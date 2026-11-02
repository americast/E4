import importlib.util, json, tempfile, unittest
from pathlib import Path
MODULE=Path(__file__).with_name('watcher_v3.py')
spec=importlib.util.spec_from_file_location('watcher_r6',MODULE); w=importlib.util.module_from_spec(spec); spec.loader.exec_module(w)

class WatcherV3Tests(unittest.TestCase):
    def test_resume_status_supersedes_historical_base(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); base=root/'bank10_local'/'bank10_status.json'; base.parent.mkdir(); base.write_text('{}')
            old=root/'bank10_local_resume_0700'/'bank10_status.json'; old.parent.mkdir(); old.write_text('{}')
            new=root/'bank10_local_resume_0800'/'bank10_status.json'; new.parent.mkdir(); new.write_text('{}')
            self.assertEqual(w.select_status([base,old,new]),new)
    def with_env(self, root):
        project=root/'project'; run=project/'artifacts'/'runs'/'paired_r6_final'; out=project/'artifacts'/'monitor_r6_final'; run.mkdir(parents=True)
        w.PROJECT=project; w.RUN=run; w.OUT=out
        return project,run,out
    def test_final_result_hash_mismatch_is_reported(self):
        with tempfile.TemporaryDirectory() as d:
            project,run,out=self.with_env(Path(d)); p=project/'result.json'; p.write_text('{}')
            s=w.finalize({'status':'complete','seed_results':{'42':{'combined_result_path':'result.json','combined_result_sha256':'0'*64},'43':{}}})
            self.assertFalse(s['accuracy_valid']); self.assertEqual(s['stage_prefix_by_seed'],{})
            self.assertTrue(any('SHA-256 mismatch' in e for e in s['verification_errors']))
    def test_missing_or_partial_seed_results_suppress_accuracy(self):
        with tempfile.TemporaryDirectory() as d:
            self.with_env(Path(d))
            s=w.finalize({'status':'partial_with_failures','seed_results':{'42':{'partial':True}}})
            self.assertFalse(s['accuracy_valid']); self.assertEqual(s['stage_prefix_by_seed'],{}); self.assertEqual(s['mean_prefix_successes'],{})
            self.assertTrue(any('missing or do not contain exactly' in e for e in s['verification_errors']))
    def test_distinct_output_preserves_existing_r5_report(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); project,run,out=self.with_env(root); r5=project/'artifacts'/'monitor_r5'; r5.mkdir(parents=True)
            sentinel=r5/'final_summary.json'; sentinel.write_text('{"r5":"preserve"}\n')
            w.OUT=out; w.finalize({'status':'partial_with_failures','seed_results':{}})
            self.assertEqual(json.loads(sentinel.read_text()),{'r5':'preserve'})
            self.assertTrue((out/'final_summary.json').is_file())
            self.assertNotEqual(out.resolve(),r5.resolve())

if __name__=='__main__': unittest.main()
