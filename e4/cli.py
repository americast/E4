"""Command line interface for E4."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from pathlib import Path

from .data.agent_prompt import get_adapter_generation_prompt
from .data.submissions import (
    AdapterInputRequired,
    AdapterSubmissionError,
    adapter_code_template,
    parse_adapter_submission,
    validate_adapter_config,
)
from .engine import setup
from .prompts.initialization import render_initialization_prompt


def _missing_adapter_config_fields(
    config_schema: Mapping[str, object], config: Mapping[str, object]
) -> list[dict[str, str]]:
    properties = config_schema.get("properties", {})
    required = config_schema.get("required", [])
    if not isinstance(properties, Mapping) or not isinstance(required, list):
        return []
    missing = []
    for name in required:
        if not isinstance(name, str) or name in config:
            continue
        spec = properties.get(name, {})
        if not isinstance(spec, Mapping):
            spec = {}
        raw_type = spec.get("type", "string")
        if isinstance(raw_type, list):
            raw_type = next((item for item in raw_type if item in {"string", "number", "integer", "boolean", "object", "array"}), "string")
        if raw_type not in {"string", "number", "integer", "boolean", "object", "array"}:
            raw_type = "string"
        field = {"name": name, "type": raw_type}
        reason = spec.get("description")
        if isinstance(reason, str) and reason.strip():
            field["reason"] = reason
        missing.append(field)
    return missing


def main() -> None:
    parser = argparse.ArgumentParser(prog="e4", description="E4 Root-Cause Analysis Framework CLI")
    subparsers = parser.add_subparsers(dest="command")

    setup_parser = subparsers.add_parser("setup", help="Initialize E4 workspace and sample mapping")
    setup_parser.add_argument("dir", nargs="?", default=".", help="Target directory (default: current directory)")

    prompt_parser = subparsers.add_parser("adapter-prompt", help="Generate AI prompt to create an adapter for your data")
    prompt_parser.add_argument("sample_file", nargs="?", default=None, help="Path to sample telemetry file to embed")
    prompt_parser.add_argument("--domain-file", help="Path to a natural-language task/domain brief")
    prompt_parser.add_argument("--metadata-file", help="Path to explicitly supplied public source metadata")

    template_parser = subparsers.add_parser(
        "adapter-template", help="Print the packaged Python adapter scaffold"
    )

    validate_parser = subparsers.add_parser(
        "adapter-validate", help="Validate a structured adapter response and its configuration without executing source"
    )
    validate_parser.add_argument("response_file", help="Path to the adapter JSON submission")
    validate_parser.add_argument("--config", help="Path to runtime adapter configuration JSON")

    init_parser = subparsers.add_parser(
        "initialization-prompt",
        help="Export the reviewable initialization prompt (offline; no model call)",
    )
    init_parser.add_argument("domain_file", nargs="?", help="Path to a natural-language domain brief")
    init_parser.add_argument("--schema", help="Path to validated adapter DatasetSchema JSON")
    init_parser.add_argument(
        "--clean-slate",
        action="store_true",
        help="Exclude the shipped paper library from executable capabilities",
    )
    init_parser.add_argument("--bank", action="store_true", help="Include the Bank task/schema brief")
    init_parser.add_argument(
        "--include-domain-priors",
        action="store_true",
        help="Append separately attributed, evaluation-informed Bank analytical priors",
    )

    args = parser.parse_args()

    if args.command == "setup":
        setup(args.dir)
    elif args.command == "adapter-prompt":
        sample_text = ""
        if args.sample_file:
            p = Path(args.sample_file)
            if p.is_file():
                sample_text = p.read_text(encoding="utf-8")
        if args.domain_file and not Path(args.domain_file).is_file():
            parser.error(f"Domain brief file does not exist: {args.domain_file}")
        if args.metadata_file and not Path(args.metadata_file).is_file():
            parser.error(f"Public metadata file does not exist: {args.metadata_file}")
        domain_text = Path(args.domain_file).read_text(encoding="utf-8") if args.domain_file else ""
        metadata_text = (
            Path(args.metadata_file).read_text(encoding="utf-8") if args.metadata_file else ""
        )
        print(
            get_adapter_generation_prompt(
                sample_text,
                domain_brief=domain_text,
                public_metadata_text=metadata_text,
            )
        )
    elif args.command == "adapter-template":
        print(adapter_code_template())
    elif args.command == "adapter-validate":
        response_path = Path(args.response_file)
        if not response_path.is_file():
            parser.error(f"Adapter response file does not exist: {args.response_file}")
        config: dict[str, object] = {}
        try:
            response_text = response_path.read_text(encoding="utf-8")
            submission = parse_adapter_submission(response_text)
            if submission.get("status") == "needs_input":
                print(json.dumps({
                    "status": "needs_input",
                    "required_fields": submission.get("required_fields", []),
                }, separators=(",", ":")))
                return
            if args.config:
                config_path = Path(args.config)
                if not config_path.is_file():
                    parser.error(f"Config file does not exist: {args.config}")
                loaded_config = json.loads(config_path.read_text(encoding="utf-8"))
                if not isinstance(loaded_config, dict):
                    raise AdapterSubmissionError("config", "config JSON must be an object")
                config = loaded_config
            missing_fields = _missing_adapter_config_fields(submission["config_schema"], config)
            if missing_fields:
                print(json.dumps({
                    "status": "needs_input",
                    "required_fields": missing_fields,
                }, separators=(",", ":")))
                return
            validate_adapter_config(submission["config_schema"], config)
        except AdapterInputRequired as exc:
            print(json.dumps({
                "status": "needs_input",
                "required_fields": getattr(exc, "required_fields", []),
            }, separators=(",", ":")))
            return
        except AdapterSubmissionError as exc:
            print(json.dumps({
                "status": "error",
                **exc.as_dict(),
            }, separators=(",", ":")))
            raise SystemExit(2) from exc
        except (json.JSONDecodeError, OSError, TypeError, KeyError) as exc:
            print(json.dumps({
                "status": "error",
                "stage": "adapter_submission",
                "details": str(exc),
            }, separators=(",", ":")))
            raise SystemExit(2) from exc
        print(json.dumps({
            "status": "ready",
            "summary": {
                "source_validated": True,
                "config_validated": True,
                "config_field_names": sorted(config),
            },
        }, separators=(",", ":")))
    elif args.command == "initialization-prompt":
        if args.include_domain_priors and not args.bank:
            parser.error("--include-domain-priors requires --bank")
        if args.domain_file and not Path(args.domain_file).is_file():
            parser.error(f"Domain brief file does not exist: {args.domain_file}")
        domain_brief = (
            Path(args.domain_file).read_text(encoding="utf-8") if args.domain_file else None
        )
        schema = None
        if args.schema:
            schema_path = Path(args.schema)
            if not schema_path.is_file():
                parser.error(f"Schema file does not exist: {args.schema}")
            try:
                schema = json.loads(schema_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                parser.error(f"Schema file is not valid JSON: {exc}")
        print(
            render_initialization_prompt(
                domain_brief,
                include_bank=args.bank,
                include_domain_priors=args.include_domain_priors,
                clean_slate=args.clean_slate,
                schema=schema,
            )
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
