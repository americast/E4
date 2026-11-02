#!/usr/bin/env python3
"""Version 3 read-only watcher for an already-launched E4 paired run."""
from __future__ import annotations
import argparse, hashlib, json, os, time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT = Path('/raid/ssinha97/E4').resolve()
SAFE_SLOT = ('valid', 'error_code', 'accepted', 'status')
SAFE_CASE = ('case_id', 'status', 'accepted', 'accepted_stage', 'worker_exit_code', 'case_wall_timeout', 'model_calls', 'prompt_tokens', 'completion_tokens', 'charged_generated_tokens', 'model_seconds', 'wall_seconds')
RUN: Path
OUT: Path
LAUNCH: dict[str, Any]

def utc(): return datetime.now(timezone.utc).isoformat()
def atomic(path: Path, data: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(data, encoding='utf-8'); os.replace(tmp, path)
def load(path: Path):
    try: return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError): return None
def sha(path: Path): return hashlib.sha256(path.read_bytes()).hexdigest()
def select_status(paths: list[Path]) -> Path | None:
    """Prefer newest explicit resume checkpoint so original histories are not double-counted."""
    resumed = sorted((p for p in paths if '_resume_' in str(p)), key=lambda p: (p.stat().st_mtime_ns, str(p)))
    if resumed: return resumed[-1]
    base = [p for p in paths if '_resume_' not in str(p)]
    return max(base, key=lambda p: (p.stat().st_mtime_ns, str(p))) if base else None
def proc_identity(pid: int):
    try:
        stat = Path(f'/proc/{pid}/stat').read_text().split()
        uid = Path(f'/proc/{pid}/status').read_text()
        uidline = next(x for x in uid.splitlines() if x.startswith('Uid:'))
        return stat[21], int(uidline.split()[1])
    except (OSError, StopIteration, ValueError, IndexError): return None

def safe_case(row: dict[str, Any]):
    out = {k: row.get(k) for k in SAFE_CASE if k in row}
    slots = row.get('stage_slots')
    if isinstance(slots, list): out['stage_slots'] = [{k: s.get(k) for k in SAFE_SLOT if k in s} for s in slots if isinstance(s, dict)]
    return out

def snapshot():
    receipt = load(RUN/'launch_receipt.json') or LAUNCH
    pid = receipt.get('pid'); ticks = str(receipt.get('process_start_ticks', '')); uid = receipt.get('uid')
    identity = proc_identity(pid) if isinstance(pid, int) else None
    running = identity == (ticks, uid)
    orchestration = load(RUN/'orchestration_result.json')
    run_manifest = load(RUN/'parallel_manifest.json') or {}
    pins = {}
    for seed in ('42','43'):
        seed_manifest = load(RUN/f'seed{seed}'/'seed_manifest.json') or {}
        audit_dirs = sorted((RUN/f'seed{seed}'/'audit').glob('*'))
        audit_manifest = load(audit_dirs[-1]/'audit_manifest.json') if audit_dirs else {}
        audit_manifest = audit_manifest or {}
        pins[seed] = {k: seed_manifest.get(k) or audit_manifest.get(k) for k in ('source_fingerprint','source_inventory_sha256','initial_library_snapshot_id','model_profile_sha256')}
    shards = {}
    for seed in ('42', '43'):
        adirs = sorted((RUN/f'seed{seed}'/'audit').glob('*'))
        adir = adirs[-1] if adirs else None
        for shard in ('local', 'wingtip'):
            key=f'{seed}.{shard}'
            if adir is None: shards[key]={'state':'no_audit'}; continue
            recs=list(adir.glob(f'seed{seed}_{shard}_task_receipt.json')); task=load(recs[-1]) if recs else None
            states=list(adir.glob(f'bank10_{shard}_resume_*/bank10_status.json'))+list(adir.glob(f'bank10_{shard}/bank10_status.json'))
            sp=select_status(states); state=load(sp) if sp else None
            state_name=('receipt_'+str(task.get('status','unknown'))) if task else str(state.get('status','unknown')) if isinstance(state,dict) else 'waiting'
            completed=state.get('completed_cases',[]) if isinstance(state,dict) else []
            shards[key]={'state':state_name,'status_path':str(sp) if sp else None,'closed_cases':[safe_case(x) for x in completed if isinstance(x,dict)],'task_exit_code':task.get('exit_code') if task else None}
    state='running' if running else ('result_written' if orchestration else 'stopped_without_result')
    live={'updated_utc':utc(),'run_manifest':{k:run_manifest.get(k) for k in ('status','runner_source_sha256','schedule') if k in run_manifest},'seed_pins':pins,'orchestrator':{'pid':pid,'start_ticks':ticks,'uid':uid,'identity_matches':running,'state':state},'chains':shards}
    if orchestration: live['orchestration_status']=orchestration.get('status')
    return live,orchestration

def write_live(live):
    atomic(OUT/'live_status.json',json.dumps(live,indent=2,sort_keys=True)+'\n')
    lines=[f'# {RUN.name} progress','',f"Updated: {live['updated_utc']}",f"Orchestrator: {live['orchestrator']['state']} (PID {live['orchestrator']['pid']}, identity verified: {live['orchestrator']['identity_matches']})",'','| Chain | State | Closed cases |','|---|---|---:|']
    for name,row in live['chains'].items(): lines.append(f"| {name} | {row['state']} | {len(row.get('closed_cases',[]))}/5 |")
    lines += ['', 'Each seed reuses the same 10 historical scored cases; the seeds are not 20 independent cases.']
    atomic(OUT/'live_status.md','\n'.join(lines)+'\n')

def finalize(orchestration):
    OUT.mkdir(parents=True,exist_ok=True)
    seed_rows=orchestration.get('seed_results') or {}; errors=[]; observed={}; failures={}
    if orchestration.get('status')!='complete': errors.append('orchestration status is not complete')
    if set(seed_rows)!={'42','43'}: errors.append('seed results are missing or do not contain exactly seeds 42 and 43')
    for seed in ('42','43'):
        entry=seed_rows.get(seed)
        if not isinstance(entry,dict): continue
        rel=entry.get('combined_result_path')
        try: p=(PROJECT/rel).resolve(strict=True) if isinstance(rel,str) else None
        except (OSError,ValueError): p=None
        if p is None or not p.is_relative_to(PROJECT): errors.append(f'seed{seed}: combined result path is missing or outside project'); continue
        expected=entry.get('combined_result_sha256')
        if not isinstance(expected,str) or sha(p)!=expected: errors.append(f'seed{seed}: combined result SHA-256 mismatch'); continue
        obj=load(p)
        if not isinstance(obj,dict) or obj.get('status')!='complete' or obj.get('case_count')!=10 or len(obj.get('cases',[]))!=10:
            errors.append(f'seed{seed}: combined result is partial or does not contain exactly 10 cases'); continue
        observed[seed]={'case_count':10,'prefix_successes':obj.get('cumulative_top3_prefix_successes',{}),'totals':{k:obj.get(k) for k in ('total_model_calls','total_prompt_tokens','total_completion_tokens','total_charged_generated_tokens','total_model_seconds','total_wall_seconds')},'result_path':str(p),'result_sha256':expected}
        for case in obj.get('cases',[]):
            if isinstance(case,dict):
                for slot in case.get('stage_slots',[]):
                    if isinstance(slot,dict) and slot.get('error_code'): failures[slot['error_code']]=failures.get(slot['error_code'],0)+1
    valid=(not errors and set(observed)=={'42','43'})
    means={}
    if valid:
        keys=set(observed['42']['prefix_successes']) & set(observed['43']['prefix_successes'])
        means={k:(observed['42']['prefix_successes'][k]+observed['43']['prefix_successes'][k])/2 for k in keys}
    summary={'created_utc':utc(),'orchestration_status':orchestration.get('status'),'accuracy_valid':valid,'verification_errors':errors,'stage_prefix_by_seed':{s:r['prefix_successes'] for s,r in observed.items()} if valid else {},'mean_prefix_successes':means,'raw_totals_by_seed':{s:r['totals'] for s,r in observed.items()},'result_links':{s:{'path':r['result_path'],'sha256':r['result_sha256']} for s,r in observed.items()},'failure_categories':failures,'case_reuse':'Each seed reuses the same 10 historical scored cases; the paired seeds are not 20 independent cases.','cost_note':'Raw recorded totals only; no baseline percentage comparison is made.'}
    final=OUT/'final_summary.json'
    if not final.exists():
        fd=os.open(final,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o644)
        with os.fdopen(fd,'w') as f: json.dump(summary,f,indent=2,sort_keys=True); f.write('\n')
    rows=[f'# {RUN.name} final summary','',summary['case_reuse'],'',f"Accuracy summary valid: {valid}",'']
    if valid:
        rows += ['| Seed | Cases | Stage 1 prefix | Stage 2 prefix | Stage 3 prefix | Prompt tokens | Completion tokens | Result |','|---:|---:|---:|---:|---:|---:|---:|---|']
        for seed,r in observed.items():
            p=r['prefix_successes']; c=r['totals']; rows.append(f"| {seed} | 10 | {p.get('stage1_prefix_successes_of_10','?')} | {p.get('stage2_prefix_successes_of_10','?')} | {p.get('stage3_prefix_successes_of_10','?')} | {c.get('total_prompt_tokens','unknown')} | {c.get('total_completion_tokens','unknown')} | [combined result]({r['result_path']}) |")
        rows += ['', 'Mean stage prefixes: '+', '.join(f'{k}={v:.2f}/10' for k,v in means.items())]
    else: rows += ['Stage accuracy is suppressed because both seeds do not have complete, hash-verified results for exactly 10 cases.']
    rows += ['', 'Failure categories (stage error codes): '+(', '.join(f'{k}={v}' for k,v in failures.items()) if failures else 'none recorded'), 'Verification errors: '+('; '.join(errors) if errors else 'none'), '']
    md=OUT/'final_summary.md'
    if not md.exists():
        fd=os.open(md,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o644)
        with os.fdopen(fd,'w') as f:f.write('\n'.join(rows))
    return summary

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--run-root',required=True); ap.add_argument('--output-dir',required=True); ap.add_argument('--interval',type=int,default=60); ap.add_argument('--max-hours',type=float,default=12); ap.add_argument('--once',action='store_true'); args=ap.parse_args()
    global RUN,OUT,LAUNCH
    RUN=Path(args.run_root).resolve(strict=True)
    if not RUN.is_dir() or not RUN.is_relative_to(PROJECT/'artifacts'): raise SystemExit('--run-root must resolve inside project artifacts')
    OUT=Path(args.output_dir).resolve()
    if not OUT.is_relative_to(PROJECT/'artifacts') or 'monitor_r5' in OUT.parts or OUT==RUN or OUT.is_relative_to(RUN) or RUN.is_relative_to(OUT): raise SystemExit('--output-dir must be a distinct directory inside project artifacts and must not target R5')
    LAUNCH=load(RUN/'launch_receipt.json')
    if not isinstance(LAUNCH,dict) or not isinstance(LAUNCH.get('pid'),int) or not LAUNCH.get('process_start_ticks') or not isinstance(LAUNCH.get('uid'),int): raise SystemExit('valid immutable launch_receipt.json with PID, start ticks, and UID is required')
    deadline=time.monotonic()+args.max_hours*3600
    while True:
        live,orchestration=snapshot(); write_live(live)
        if orchestration: finalize(orchestration); return 0
        if live['orchestrator']['state']=='stopped_without_result':
            live['monitor_outcome']='stopped_without_result'; write_live(live); return 0
        if time.monotonic()>=deadline:
            live['monitor_outcome']='monitor_timeout'; write_live(live); atomic(OUT/'monitor_timeout.json',json.dumps({'at_utc':utc(),'outcome':'monitor_timeout'},indent=2)+'\n'); return 0
        if args.once:return 0
        time.sleep(args.interval)
if __name__=='__main__': raise SystemExit(main())
