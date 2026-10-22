#!/usr/bin/env python3
"""Render the pinned initial M5/M6 report from its immutable run artifacts."""
import argparse
import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SCORED = ROOT / "artifacts/m3/model_runs/bankm6i2/m6_comparison/comparison_result.json"
DEFAULT_DEV = ROOT / "artifacts/m3/model_runs/m5dev05/m5_development/development_comparison_result.json"
DEFAULT_BOOTSTRAP = ROOT / "artifacts/m3/model_runs/m5-schema-bootstrap-20261017-01/m5_bootstrap/bootstrap/bootstrap_result.json"
DEFAULT_MANIFEST = ROOT / "artifacts/m5/experiments/bank_m6_initial_20261017_02/frozen_manifest.json"
OUT = ROOT / "docs/results/m5_m6"
SYSTEMS = ("E4", "Terminus-2")
METRICS = ("accuracy_at_3_1", "accuracy_at_3_2", "accuracy_at_3_3")
PREFIX_LABELS = ("After attempt 1", "After attempt 2", "After attempt 3")
PINNED_INPUTS = {
    "scored": "07d374ea333aacc7b38ef69efb1c98d3c3ddf47093774614c5e4a47d1c457bfd",
    "development": "2b75f456be69ee91559ff2eb0457cd78fd897ce7974ee903e3c5e85c5ddfd1a6",
    "bootstrap": "ed790891adfe11329406e119802818849a7f90091645d7594f153e3f4dceb290",
    "manifest": "a4bb8489736cd9b01b72128b974db7e975334c6dd06f407b7e0f252bf96a68c7",
}


def read_json(path):
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def sha256_file(path):
    import hashlib
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fmt(value, digits=1):
    if value is None:
        return "unknown"
    return ("{:,.%df}" % digits).format(value)


def attempts_for(result):
    return result.get("attempts", [])


def status_counts(result):
    counts = {"submitted": 0, "failed": 0, "not_run": 0, "other": 0}
    for slot in attempts_for(result):
        status = slot.get("status", "other")
        counts[status if status in counts else "other"] += 1
    return counts


def render_svg(scored):
    width, height = 920, 520
    left, right, top, bottom = 90, 42, 70, 80
    plot_w, plot_h = width - left - right, 210
    colors = {"E4": "#2563eb", "Terminus-2": "#e66b24"}
    lines = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="920" height="520" viewBox="0 0 920 520">',
        '<rect width="100%" height="100%" fill="#fff"/>',
        '<style>text{font-family:Arial,sans-serif;fill:#172033}.muted{fill:#5b6475}.grid{stroke:#d9dee8;stroke-width:1}</style>',
        '<text x="90" y="32" font-size="22" font-weight="700">Accuracy and measured model time</text>',
    ]
    for i in range(6):
        yval = i / 5
        y = top + plot_h * (1 - yval)
        lines.append('<line class="grid" x1="%s" y1="%.1f" x2="%s" y2="%.1f"/>' % (left, y, width-right, y))
        lines.append('<text class="muted" x="%s" y="%.1f" text-anchor="end" font-size="12">%d%%</text>' % (left-12, y+4, round(yval*100)))
    for idx, label in enumerate(PREFIX_LABELS):
        x = left + idx * plot_w / 2
        lines.append('<text class="muted" x="%.1f" y="%.1f" text-anchor="middle" font-size="13">Prefix %d</text>' % (x, top+plot_h+25, idx+1))
    lines.append('<text class="muted" x="90" y="%.1f" font-size="13">Accuracy across accepted diagnosis slots</text>' % (top-14))
    bar_top = 355
    max_hours = max(scored["systems"][s]["aggregate_cost"]["model_seconds"] / 3600 for s in SYSTEMS)
    for system in SYSTEMS:
        secs = scored["systems"][system]["aggregate_cost"]["model_seconds"]
        accuracy = [scored["systems"][system][m] for m in METRICS]
        coords = []
        for idx, value in enumerate(accuracy):
            x = left + idx * plot_w / 2 + (-5 if system == "E4" else 5)
            y = top + plot_h * (1-value)
            coords.append("%.1f,%.1f" % (x, y))
            if system == "E4":
                lines.append('<circle cx="%.1f" cy="%.1f" r="6" fill="%s"/>' % (x, y, colors[system]))
                label_y = y-14
            else:
                lines.append('<polygon points="%.1f,%.1f %.1f,%.1f %.1f,%.1f %.1f,%.1f" fill="%s"/>' % (x, y-6, x+6, y, x, y+6, x-6, y, colors[system]))
                label_y = y+23 if idx == 0 else y-14
            lines.append('<text x="%.1f" y="%.1f" text-anchor="middle" font-size="12" fill="%s">%d%%</text>' % (x, label_y, colors[system], round(value*100)))
        lines.append('<polyline fill="none" stroke="%s" stroke-width="3" points="%s"/>' % (colors[system], " ".join(coords)))
        ybar = bar_top + (0 if system == "E4" else 42)
        barw = (secs/3600) / max_hours * (plot_w * .72)
        lines.append('<rect x="%d" y="%d" width="%.1f" height="24" rx="3" fill="%s"/>' % (left, ybar, barw, colors[system]))
        lines.append('<text x="%.1f" y="%d" font-size="13" fill="%s">%s %.2f h</text>' % (left+barw+10, ybar+17, colors[system], system, secs/3600))
    lines.append('<text class="muted" x="90" y="340" font-size="13">Aggregate model request time (hours; scored run)</text>')
    lines.extend([
        '<text class="muted" x="90" y="490" font-size="11">Single seed, ten paired cases; accuracy is descriptive. E4 slots are stages in one controller trace.</text>',
        '</svg>',
    ])
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scored", type=Path, default=DEFAULT_SCORED)
    parser.add_argument("--development", type=Path, default=DEFAULT_DEV)
    parser.add_argument("--bootstrap", type=Path, default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--output-dir", type=Path, default=OUT)
    args = parser.parse_args()
    input_paths = {"scored": args.scored, "development": args.development,
                   "bootstrap": args.bootstrap, "manifest": args.manifest}
    for label, path in input_paths.items():
        actual = sha256_file(path)
        if actual != PINNED_INPUTS[label]:
            raise SystemExit("%s input SHA-256 mismatch for %s: expected %s, got %s" %
                             (label, path, PINNED_INPUTS[label], actual))
    scored, dev, bootstrap, manifest = map(read_json, (args.scored, args.development, args.bootstrap, args.manifest))
    out = args.output_dir

    rows = []
    for cid in scored["case_ids"]:
        pair = {s: scored["case_jobs"][cid][s] for s in SYSTEMS}
        records = {}
        for system in SYSTEMS:
            result_path = Path(pair[system]["result_path"])
            actual_result_hash = sha256_file(result_path)
            expected_result_hash = pair[system]["result_sha256"]
            if actual_result_hash != expected_result_hash:
                raise SystemExit("case result SHA-256 mismatch for %s/%s at %s: expected %s, got %s" %
                                 (cid, system, result_path, expected_result_hash, actual_result_hash))
            records[system] = read_json(result_path)
        row = {"case_id": cid}
        for system in SYSTEMS:
            result = records[system]
            cost = pair[system]["cost"]
            slots = attempts_for(result)
            prefix_correct = [any(a.get("accepted") is True for a in slots[:n]) for n in (1, 2, 3)]
            for n, ok in enumerate(prefix_correct, start=1):
                row["%s_through_slot_%d_accepted" % (system, n)] = int(ok)
            counts = status_counts(result)
            row[system + "_submitted"] = counts["submitted"]
            row[system + "_failed_slots"] = counts["failed"]
            row[system + "_not_run_slots"] = counts["not_run"]
            for metric in ("prompt_tokens", "completion_tokens", "model_calls", "model_seconds", "wall_seconds", "startup_seconds", "cpu_seconds", "load_seconds", "logical_bytes_scanned", "returned_bytes", "physical_bytes_read"):
                row[system + "_" + metric] = cost.get(metric)
        rows.append(row)
    csv_path = out / "paired_cases.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    manifest_hash = PINNED_INPUTS["manifest"]
    out.mkdir(parents=True, exist_ok=True)
    lines = [
        "# M5 and initial M6 results",
        "",
        "## Finding",
        "",
        "On this single seed-42 pass over ten fixed OpenRCA Bank incidents, E4's Accuracy@3 did not increase across its three stage-slot prefixes (2/10, 2/10, 2/10). Terminus-2 reached 2/10, 4/10, and 4/10 across its submission prefixes. This does not reproduce a rising E4 accuracy trend. These are descriptive counts from one small sample and do not support an inferential comparison.",
        "",
        "E4 used fewer measured tokens and model/wall-clock seconds, while its final prefix accuracy was lower: 2/10 versus 4/10. Including bootstrap, E4 used 13.3%% of Terminus-2's prompt tokens, 27.6%% of its generated tokens, 28.1%% of its model seconds, and 37.3%% of its wall seconds. This comparison does not establish an efficiency gain at equal accuracy. E4's schema-only bootstrap adds %.1f model seconds and %.1f wall seconds separately; including it, E4 used %.2f model hours and %.2f wall hours. Amortized over ten cases, that bootstrap adds %.1f model seconds and %.1f wall seconds per case. These are observed run costs under a shared local model server." % (
            scored["bootstrap_cost_separate"]["E4"]["model_seconds"],
            scored["bootstrap_cost_separate"]["E4"]["wall_seconds"],
            scored["systems"]["E4"]["aggregate_cost_with_bootstrap"]["model_seconds"] / 3600,
            scored["systems"]["E4"]["aggregate_cost_with_bootstrap"]["wall_seconds"] / 3600,
            scored["bootstrap_cost_separate"]["E4"]["model_seconds"] / 10,
            scored["bootstrap_cost_separate"]["E4"]["wall_seconds"] / 10),
        "",
        "## Accuracy and paired cases",
        "",
        "Accuracy@3 here means at least one accepted diagnosis within the first 1, 2, or 3 diagnosis slots; a case contributes at most once at each prefix. E4 and Terminus-2 were evaluated on the same ten incidents. E4's slots are its retrieval, composition, and synthesis stages from the same controller trace, not three separate runs; Terminus-2's slots are successive diagnosis submissions.",
        "",
        "| System | Attempt 1 | Through attempt 2 | Through attempt 3 |",
        "|---|---:|---:|---:|",
    ]
    for system in SYSTEMS:
        vals = [scored["systems"][system][m] for m in METRICS]
        lines.append("| %s | %d/10 (%.0f%%) | %d/10 (%.0f%%) | %d/10 (%.0f%%) |" % (system, round(vals[0]*10), vals[0]*100, round(vals[1]*10), vals[1]*100, round(vals[2]*10), vals[2]*100))
    lines.extend(["", "![Accuracy by slot prefix and aggregate model time](results/m5_m6/accuracy_cost.svg)", "", "Per-case cumulative prefix outcomes, token counts, costs, and counts of submitted diagnoses, failed slots, and unrun slots are in [paired_cases.csv](results/m5_m6/paired_cases.csv). The full ordered slot records remain in each raw case JSON. Aggregate `submissions` fields in the comparison JSON count attempted stage slots, including failed slots, and should not be interpreted as actual submitted diagnoses.", "", "## Resource accounting", "", "| Measure | E4 scored | E4 + bootstrap | E4 + bootstrap per case | Terminus-2 scored |", "|---|---:|---:|---:|---:|"])
    metric_names = (("prompt_tokens", "Prompt tokens"), ("completion_tokens", "Generated tokens"), ("model_calls", "Model calls"), ("model_seconds", "Model seconds"), ("wall_seconds", "Wall seconds"))
    for metric, label in metric_names:
        vals = [scored["systems"]["E4"]["aggregate_cost"][metric], scored["systems"]["E4"]["aggregate_cost_with_bootstrap"][metric], scored["systems"]["E4"]["aggregate_cost_with_bootstrap_amortized_per_case"][metric], scored["systems"]["Terminus-2"]["aggregate_cost"][metric]]
        digits = [1 if metric.endswith("seconds") or (metric == "model_calls" and idx == 2) else 0
                  for idx in range(len(vals))]
        lines.append("| %s | %s | %s | %s | %s |" % ((label,) + tuple(fmt(v, digits[idx]) for idx, v in enumerate(vals))))
    lines.extend(["", "CPU execution, physical reads, and full physical I/O are unknown (`null`), not zero. The shared model server load took %.2f seconds and is recorded separately. E4's logical-scan and returned-byte counters cover only instrumented public-query paths and are partial; zeros mean those counters recorded no bytes for that path, not that no data was processed. Terminus-2 has no comparable query byte counters. We therefore make no I/O or cache-efficiency claim." % scored.get("shared_model_server", {}).get("load_seconds", 0), "", "The paired jobs ran serially. Wall time includes task setup, data access, program execution and validation, along with model requests; server reuse and prompt caching affect the observed costs. Model seconds measure request time. Neither quantity isolates system-independent latency.", "", "## M5 development and bootstrap", "", "The two fixed development cases produced E4 Accuracy@3 across attempt prefixes of %.0f%%, %.0f%%, %.0f%% and Terminus-2 of %.0f%%, %.0f%%, %.0f%%. This run was used for readiness and budget measurement; it is separate from the ten scored cases." % tuple(dev["systems"][s][m]*100 for s in SYSTEMS for m in METRICS), "", "The fresh schema-only E4 bootstrap completed with one model call, %s prompt tokens, %s generated tokens, %.1f model seconds, and %.1f wall seconds. It admitted zero generated operators and zero generated playbooks; the initial library retained ten built-in operators and four verified seed playbooks. Terminus-2 has no analogous bootstrap phase, so its bootstrap cost is zero." % (bootstrap["model_cost"]["prompt_tokens"], bootstrap["model_cost"]["completion_tokens"], bootstrap["model_cost"]["elapsed_seconds"], bootstrap["wall_seconds"]), "", "Frozen manifest SHA-256: `%s`. Frozen source archive SHA-256: `14b00ba6c7f028d91ede80abe8b01bb255de626e9ba2a372b1532be5319040ae`." % manifest_hash, ""])
    lines.extend(["## Failure and admission audit", "", "Across the ten E4 traces, stage 1 accepted 2 cases and rejected 8. Stage 2 had 3 diagnostic rejections and 5 static-validation failures. Stage 3 had 6 runtime execution failures, 1 truncated generation, and 1 static-validation failure; two cases had already been accepted at stage 1. E4 had 13 actual submitted diagnoses, 13 failed slots, and 4 not-run slots. Six generated operator definitions were attempted and zero were admitted; zero generated playbooks were admitted. The retained library therefore contains the initial built-ins and seed playbooks only.", "", "The six stage 3 runtime failures include four serialized operator inputs reaching the 2 MiB input limit and two generated code errors involving missing `isinstance` or `math` names. The serving guidance documents the 2 MiB cap; a discoverability gap remains in the generated-spec contract's builtins list. Stage 2 failures included `inputs=array` static validation with exhausted repair. These are observed execution and admission outcomes; zero admissions does not mean zero synthesis attempts.", "", "Terminus-2 submitted 14 diagnoses and left 16 slots not run. One case (1/10) explicitly ended with `CallBudgetExhausted` at the 73,728 generated-token ceiling and did not reach a third submission. Five other nonaccepted cases ended with complete job status but no error or termination reason in the recorded result; they are unresolved trajectory outcomes and are not attributed to turn-budget exhaustion.", "", "A separate interrupted first invocation (`bankm6i1`) is excluded from the completed comparison totals. It started one Terminus-2 HTTP request that received no response; the server log shows at least 590 generated tokens, but exact usage is unknown. The request and server logs remain under `artifacts/m3/model_runs/bankm6i1/`. This is retained as execution overhead rather than counted as a scored case or included in the completed run's resource totals.", "", "No diagnosis was manually selected. The evaluator's exact component-identity rule determined acceptance. The frozen source archive is `artifacts/m5/experiments/bank_m6_initial_20261017_02/frozen_source.tar.gz`; the first scored pass is preserved as measured, including its errors and missing trajectory detail.", "", "## Reproduction and limits", "", "The frozen scored manifest SHA-256 is `%s`. Raw scored outputs remain under `artifacts/m3/model_runs/bankm6i2/m6_comparison/`; development and bootstrap artifacts are preserved under their respective run directories. This report reads only aggregate results, attempt metadata, and stage status counts; it does not publish evaluator truth labels or predicted component identities." % manifest_hash, "", "This is a local comparison, not a direct reproduction of the paper's setup: it used Gemma 4 31B Q4_0 rather than Gemini, reconstructed four Bank seed playbooks and ten trusted built-ins, and had no original E4 implementation or original incident subset to run. Harbor Terminus-2 was pinned to upstream revision `26106c6d9c66848a1440faf5724d336affac52aa`; the paper's exact baseline revision is not established. These constraints, the single seed and ten-case sample, and incomplete CPU/I/O instrumentation mean the observed trend should not be treated as a faithful refutation or reproduction of a paper result.", "", "The full source test suite had passed 270 tests with 5 environment skips before the frozen run. Local Terminus integration and shared-server reuse also limit broader conclusions.", ""])
    report_path = ROOT / "docs/M5_M6_RESULTS.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    (out / "accuracy_cost.svg").write_text(render_svg(scored), encoding="utf-8")
    print("wrote", report_path)
    print("wrote", csv_path)
    print("wrote", out / "accuracy_cost.svg")


if __name__ == "__main__":
    main()
