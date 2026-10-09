"""``wcd`` — one entry point for every pipeline stage.

Each subcommand delegates to a stage module's ``main(argv)``; the stage
modules keep their own argparse definitions, so ``wcd extract --help`` shows
exactly the flags the stage accepts. Stage modules are imported lazily so that
e.g. ``wcd extract`` does not require database configuration.
"""
from __future__ import annotations

import sys

COMMANDS = {
    # name           (module,                       summary)
    "extract":     ("wcd_pipeline.extract",     "Stage 1: stage one .mwrev.zst bundle as Parquet"),
    "extract-all": ("wcd_pipeline.extract_all", "Stage 1: run extract for every bundle in a directory"),
    "dedup":       ("wcd_pipeline.dedup",       "Stage 3: consolidate and deduplicate staged Parquet"),
    "load":        ("wcd_pipeline.load",        "Stage 3: load deduplicated Parquet into Postgres"),
    "init-db":     ("wcd_pipeline.init_db",     "Create Postgres tables / manage secondary indexes"),
    "purge":       ("wcd_pipeline.purge",       "Drop or truncate Postgres tables"),
    "fixture-dump": ("wcd_pipeline.fixture_dump", "Filter XML history dumps down to a page list (fixture input for RevisionChest)"),
}


def _usage() -> str:
    width = max(len(k) for k in COMMANDS)
    lines = ["usage: wcd <command> [args...]", "", "commands:"]
    lines += [f"  {name.ljust(width)}  {summary}" for name, (_, summary) in COMMANDS.items()]
    lines += ["", "Run `wcd <command> --help` for the stage's own options."]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("-h", "--help"):
        print(_usage())
        return 0 if argv else 2
    name, rest = argv[0], argv[1:]
    if name not in COMMANDS:
        print(f"wcd: unknown command '{name}'\n\n{_usage()}", file=sys.stderr)
        return 2
    module_name, _ = COMMANDS[name]
    import importlib

    module = importlib.import_module(module_name)
    # argparse prog name in stage help output
    sys.argv[0] = f"wcd {name}"
    result = module.main(rest)
    return int(result or 0)


if __name__ == "__main__":
    raise SystemExit(main())
