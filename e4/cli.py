"""Command line interface for E4."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .data.agent_prompt import get_adapter_generation_prompt
from .engine import setup


def main() -> None:
    parser = argparse.ArgumentParser(prog="e4", description="E4 Root-Cause Analysis Framework CLI")
    subparsers = parser.add_subparsers(dest="command")

    setup_parser = subparsers.add_parser("setup", help="Initialize E4 workspace and sample mapping")
    setup_parser.add_argument("dir", nargs="?", default=".", help="Target directory (default: current directory)")

    prompt_parser = subparsers.add_parser("adapter-prompt", help="Generate AI prompt to create an adapter for your data")
    prompt_parser.add_argument("sample_file", nargs="?", default=None, help="Path to sample telemetry file to embed")

    args = parser.parse_args()

    if args.command == "setup":
        setup(args.dir)
    elif args.command == "adapter-prompt":
        sample_text = ""
        if args.sample_file:
            p = Path(args.sample_file)
            if p.is_file():
                sample_text = p.read_text(encoding="utf-8")
        print(get_adapter_generation_prompt(sample_text))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
