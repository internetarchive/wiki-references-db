"""``wcd setup``: build a wiki's database from its dumps, streaming the shards.

    wcd setup --domain de.wikipedia.org [--mirror https://dumps.wikimedia.your.org] [--date 20261101]

Finds the wiki's dumps (database name from the site's siteinfo, the newest run
whose page-history dump is finished, or ``--date``), then works through the
history dump's files ("shards") as they arrive instead of waiting for all of
them:

  download ──► RevisionChest build ──► extract (Stage 1)        per shard, overlapped
                                                               ─── barrier: every shard done ───
  init-db → stage0-load → dedup → load → gate 3 → parts → … → revision-stats     whole wiki

``--download-jobs`` files download at once (largest first, so the longest
shards start early); as each is verified (sha1) it is queued for one of
``--jobs`` shard workers, which builds its bundle and metadata and extracts it.
So downloading, Stage 0 and Stage 1 overlap, and the run only waits at the
first step that needs the whole wiki: ``stage0-load`` (it rebuilds the
domain's ``revisions`` dataset from every shard's metadata), then ``dedup`` and
everything after it, which run in the runbook's order. ``--shards-only`` stops
at the barrier.

Everything is resumable: a verified download has ``<file>.ok``, a built shard
``state/<shard>.built``, an extracted shard ``staging/<shard>/DONE.txt``, a
finished whole-wiki step ``state/steps/<step>.done``. Re-running the same
command skips what is done and redoes what was interrupted; a failed shard
fails the run (after the others finish) without touching the barrier.

Layout, under ``--work-dir`` (default ``WCD_WORK_DIR``, else ``./wcd-work``)::

    <work>/<domain>/<dbname>-<date>/
        dumps/      the dump files (or --dumps-dir)
        bundles/    .mwrev.zst bundles           meta/   per-shard RevisionChest metadata
        staging/    Stage 1 staging, then dedup  state/  markers      logs/  per shard and step

The Parquet tier goes to ``PARQUET_DIR`` (``--parquet-dir``) and Postgres to
``DATABASE_URL``, as for every stage. Global source ids are qualified with the
run id ``<dbname>-<date>`` (``WCD_RUN_ID``, unless set). DuckDB and Postgres
settings for the whole-wiki steps come from the environment, as in the runbook.
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

from wcd_schema.ids import check_run_id

from . import dumps
from .config import load_dotenv  # noqa: F401  (loads .env on import)
from .extract_all import extract_command, mark_shard, prepare_shard, shard_done, shard_name

REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_REVISIONCHEST = REPO_ROOT / "rust" / "revisionchest" / "target" / "release" / "RevisionChest"


def bundle_name(dump_name: str) -> str:
    """RevisionChest's ``bundle_name``: one trailing ``.bz2``/``.7z``/``.gz``/``.xml`` dropped,
    ``.mwrev.zst`` appended (``…history1.xml-p1p857.7z`` → ``…history1.xml-p1p857.mwrev.zst``)."""
    for ext in (".bz2", ".7z", ".gz", ".xml"):
        if dump_name.endswith(ext):
            return dump_name[: -len(ext)] + ".mwrev.zst"
    return dump_name + ".mwrev.zst"


def whole_wiki_steps(domain: str, run: Path, jobs: int) -> list[tuple[str, list[str]]]:
    """The steps after the barrier, in the runbook's order (``docs/runbook.md``,
    "Full build, one domain"). Each is ``wcd <args>``; each ends with its gate."""
    j = str(jobs)
    meta, staging = str(run / "meta"), str(run / "staging")
    return [
        ("init-db", ["init-db", "--no-indexes"]),
        ("stage0-load", ["stage0-load", "--domain", domain, "--metadata", meta, "--work-dir", staging, "--jobs", j]),
        ("dedup", ["dedup", "-d", staging]),
        ("load", ["load", "-d", staging]),
        ("gate-3", ["gate", "3", "--domain", domain]),
        ("indexes-3", ["init-db", "--add-indexes", "--table", "web_resources", "citation_instances", "revisions"]),
        ("parts", ["parts", "--domain", domain, "--jobs", j]),
        ("indexes-4", ["init-db", "--add-indexes", "--table", "normalized_citation_parts"]),
        ("denorm-export", ["denorm-export", "--domain", domain]),
        ("ref-name-links", ["ref-name-links", "--domain", domain]),
        ("indexes-5", ["init-db", "--add-indexes", "--table", "ref_name_links"]),
        ("match", ["match", "--domain", domain, "--jobs", j, "--skip-global"]),
        ("match-global", ["match", "--domain", domain, "--jobs", "1"]),       # 6d + gate 6 (buckets done above are skipped)
        ("materialize", ["materialize", "--domain", domain]),
        ("indexes-7", ["init-db", "--add-indexes", "--table", "global_sources", "citation_source_resolution",
                       "page_source_summary"]),
        ("revision-stats", ["revision-stats", "--domain", domain, "--jobs", j]),
    ]


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [setup] {msg}", flush=True)


def _gb(n: float) -> str:
    """Bytes in GB, or MB below 1 GB (a small wiki's dump is tens of MB)."""
    return f"{n / 1e9:,.1f} GB" if n >= 1e9 else f"{n / 1e6:,.1f} MB"


@dataclass
class Progress:
    total: int
    total_bytes: int
    downloaded: int = 0
    built: int = 0
    extracted: int = 0
    failed: list = field(default_factory=list)
    bytes_in: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, attr: str, n: int = 1) -> None:
        with self.lock:
            setattr(self, attr, getattr(self, attr) + n)

    def line(self, started: float) -> str:
        rate = self.bytes_in / max(1.0, time.time() - started)
        return (f"shards {self.total}: downloaded {self.downloaded}, built {self.built}, extracted {self.extracted}"
                + (f", FAILED {len(self.failed)}" if self.failed else "")
                + f" | fetched {_gb(self.bytes_in)} this run ({rate / 1e6:,.1f} MB/s)")


class ShardRunner:
    """Downloads, builds and extracts the shards of one dump run."""

    def __init__(self, args, run: dumps.DumpRun, run_dir: Path, dumps_dir: Path, parquet_dir: str, env: dict):
        self.args, self.run, self.run_dir, self.dumps_dir = args, run, run_dir, dumps_dir
        self.parquet_dir, self.env = parquet_dir, env
        for d in ("bundles", "meta", "staging", "state", "logs"):
            (run_dir / d).mkdir(parents=True, exist_ok=True)
        self.progress = Progress(total=len(run.files), total_bytes=run.total_bytes)
        # downloaded-but-not-yet-built shards allowed at once (disk); None: no limit
        ahead = args.max_ahead if args.max_ahead is not None else (2 * args.jobs if args.delete_dumps else None)
        self.ahead = threading.BoundedSemaphore(ahead) if ahead else None

    # -- per-shard state -------------------------------------------------------
    def _paths(self, f: dumps.DumpFile):
        bundle = self.run_dir / "bundles" / bundle_name(f.name)
        shard = shard_name(str(bundle))
        return (bundle, self.run_dir / "meta" / f"{shard}.parquet", self.run_dir / "state" / f"{shard}.built",
                self.run_dir / "staging" / shard, shard)

    def _is_built(self, f) -> bool:
        bundle, meta, built, _, _ = self._paths(f)
        return built.exists() and bundle.exists() and meta.exists()

    def _is_extracted(self, f) -> bool:
        return self._is_built(f) and shard_done(str(self._paths(f)[3]))

    # -- work ------------------------------------------------------------------
    def _run(self, cmd: list[str], logfile: Path) -> int:
        with open(logfile, "a") as out:
            out.write(f"\n# {time.strftime('%Y-%m-%d %H:%M:%S')} {' '.join(cmd)}\n")
            out.flush()
            return subprocess.call(cmd, stdout=out, stderr=subprocess.STDOUT, env=self.env)

    def _build(self, f: dumps.DumpFile, dump: Path) -> None:
        bundle, meta, built, _, shard = self._paths(f)
        if self._is_built(f):
            return
        for p in (bundle, meta):
            p.unlink(missing_ok=True)
        cmd = [self.args.revisionchest, "build", str(dump), "-o", str(self.run_dir / "bundles"),
               "--parquet", str(meta), "--domain", self.args.domain]
        if self.args.namespace:
            cmd += ["--namespace", self.args.namespace]
        t = time.time()
        code = self._run(cmd, self.run_dir / "logs" / f"{shard}.build.log")
        if code != 0 or not bundle.exists() or not meta.exists():
            raise RuntimeError(f"RevisionChest build failed on {f.name} (exit {code}); see logs/{shard}.build.log")
        built.write_text(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {time.time() - t:.0f}s\n")
        log(f"built {shard} ({time.time() - t:.0f}s)")

    def _extract(self, f: dumps.DumpFile) -> None:
        bundle, _, _, staging, shard = self._paths(f)
        if shard_done(str(staging)):
            return
        prepare_shard(str(staging))
        t = time.time()
        code = self._run(extract_command(str(bundle), str(staging), self.args.domain, self.parquet_dir),
                         self.run_dir / "logs" / f"{shard}.extract.log")
        mark_shard(str(staging), code)
        if code != 0:
            raise RuntimeError(f"extract failed on {shard} (exit {code}); see logs/{shard}.extract.log")
        log(f"extracted {shard} ({time.time() - t:.0f}s)")

    def _process(self, f: dumps.DumpFile, dump: Path, holds_slot: bool) -> None:
        try:
            self._build(f, dump)
            self.progress.add("built")
            if self.args.delete_dumps and dump.exists():
                dump.unlink()                     # the .ok marker stays: the shard is built
        finally:
            if holds_slot:
                self.ahead.release()
        self._extract(f)
        self.progress.add("extracted")

    def _fetch(self, f: dumps.DumpFile) -> Path:
        dump = self.dumps_dir / f.name
        if not dumps.is_downloaded(dump):
            t = time.time()
            dumps.download(f, self.dumps_dir, prefix=self.args.mirror, progress=lambda n: self.progress.add("bytes_in", n))
            log(f"downloaded {f.name} ({_gb(f.size)}, {time.time() - t:.0f}s)")
        self.progress.add("downloaded")
        return dump

    def run_all(self) -> bool:
        started = time.time()
        todo = []
        for f in self.run.files:
            if self._is_extracted(f):
                for attr in ("downloaded", "built", "extracted"):
                    self.progress.add(attr)
            else:
                todo.append(f)
        log(f"{len(self.run.files)} shards ({_gb(self.run.total_bytes)}), {len(self.run.files) - len(todo)} already done")
        todo.sort(key=lambda f: -f.size)          # largest first: the long shards start early

        work: queue.Queue = queue.Queue()
        failures = self.progress.failed
        stop = threading.Event()

        def report():
            while not stop.wait(self.args.status_interval):
                log(self.progress.line(started))

        def fetch(f):
            if stop.is_set():
                return
            built = self._is_built(f)
            holds_slot = False
            try:
                if built:                       # the dump is no longer needed (and may be deleted)
                    self.progress.add("downloaded")
                    work.put((f, self.dumps_dir / f.name, False))
                    return
                if self.ahead:
                    self.ahead.acquire()
                    holds_slot = True
                dump = self._fetch(f)
                work.put((f, dump, holds_slot))
            except Exception as e:  # noqa: BLE001  (reported, the rest go on)
                if holds_slot:
                    self.ahead.release()
                failures.append((f.name, str(e)))
                log(f"FAILED {f.name}: {e}")

        def worker():
            while True:
                item = work.get()
                if item is None:
                    return
                f, dump, holds_slot = item
                try:
                    self._process(f, dump, holds_slot)
                except Exception as e:  # noqa: BLE001
                    failures.append((f.name, str(e)))
                    log(f"FAILED {f.name}: {e}")

        reporter = threading.Thread(target=report, daemon=True)
        reporter.start()
        workers = [threading.Thread(target=worker, daemon=True) for _ in range(self.args.jobs)]
        for w in workers:
            w.start()
        try:
            with ThreadPoolExecutor(self.args.download_jobs) as pool:
                list(pool.map(fetch, todo))
        finally:
            for _ in workers:
                work.put(None)
            for w in workers:
                w.join()
            stop.set()
        log(self.progress.line(started))
        if failures:
            for name, err in failures:
                log(f"  failed: {name}: {err}")
            return False
        return True


def run_steps(steps, run_dir: Path, env: dict, from_step: str | None) -> bool:
    done_dir = run_dir / "state" / "steps"
    done_dir.mkdir(parents=True, exist_ok=True)
    names = [n for n, _ in steps]
    if from_step:
        for n in names[names.index(from_step):]:
            (done_dir / f"{n}.done").unlink(missing_ok=True)
    for name, argv in steps:
        marker = done_dir / f"{name}.done"
        if marker.exists():
            log(f"step {name}: done earlier")
            continue
        logfile = run_dir / "logs" / f"step-{name}.log"
        log(f"step {name}: wcd {' '.join(argv)}  (log: {logfile})")
        t = time.time()
        with open(logfile, "a") as out:
            code = subprocess.call([sys.executable, "-m", "wcd_pipeline.cli", *argv], stdout=out,
                                   stderr=subprocess.STDOUT, env=env)
        if code != 0:
            log(f"step {name} FAILED (exit {code}, {time.time() - t:.0f}s); see {logfile}. Fix, then re-run: "
                "finished steps are skipped")
            return False
        marker.write_text(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {time.time() - t:.0f}s\n")
        log(f"step {name}: ok ({time.time() - t:.0f}s)")
    return True


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="wcd setup", description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--domain", required=True, help="the wiki, e.g. de.wikipedia.org")
    ap.add_argument("--mirror", default=os.getenv("DUMPS_MIRROR", dumps.PRIMARY),
                    help=f"dump URL prefix, laid out like {dumps.PRIMARY} (e.g. https://dumps.wikimedia.your.org; "
                         "default DUMPS_MIRROR, else the primary site)")
    ap.add_argument("--date", help="dump run date (YYYYMMDD); default: the newest run whose history dump is done")
    ap.add_argument("--dbname", help="the wiki's dump name (e.g. dewiki); default: its siteinfo wikiid")
    ap.add_argument("--format", choices=sorted(dumps.HISTORY_JOBS), default="7z", help="history dump format (default 7z)")
    ap.add_argument("--work-dir", default=os.getenv("WCD_WORK_DIR", "./wcd-work"),
                    help="root of the run directories (default WCD_WORK_DIR, else ./wcd-work)")
    ap.add_argument("--dumps-dir", help="where the dump files go (default <run>/dumps); point at an existing "
                                        "download to reuse it (files with <name>.ok are not fetched again)")
    ap.add_argument("--parquet-dir", default=os.getenv("PARQUET_DIR"), help="Parquet tier base (default PARQUET_DIR)")
    ap.add_argument("-j", "--jobs", type=int, default=os.cpu_count() or 4,
                    help="shard workers (build + extract one shard each), and --jobs of the whole-wiki steps "
                         "(default: CPU count; each extract worker can take ~2 GB)")
    ap.add_argument("--download-jobs", type=int, default=4, help="concurrent downloads (default 4)")
    ap.add_argument("--max-ahead", type=int, default=None,
                    help="downloaded-but-unbuilt shards allowed at once (disk); default unlimited, or 2 × --jobs "
                         "with --delete-dumps")
    ap.add_argument("--delete-dumps", action="store_true", help="delete each dump file once its bundle is built")
    ap.add_argument("--namespace", default="0", help="namespaces to keep, comma-separated (default 0; '' for all)")
    ap.add_argument("--revisionchest", default=os.getenv("REVISIONCHEST") or (
        str(DEFAULT_REVISIONCHEST) if DEFAULT_REVISIONCHEST.exists() else shutil.which("RevisionChest") or "RevisionChest"),
                    help="RevisionChest binary (default REVISIONCHEST, the repo's release build, or PATH)")
    ap.add_argument("--run-id", help="WCD_RUN_ID for the build (default: WCD_RUN_ID, else <dbname>-<date>)")
    ap.add_argument("--shards-only", action="store_true", help="stop at the barrier (all shards built and extracted)")
    ap.add_argument("--from-step", help="redo the whole-wiki steps from this one (their markers are removed)")
    ap.add_argument("--status-interval", type=float, default=60, help="seconds between progress lines (default 60)")
    ap.add_argument("--dry-run", action="store_true", help="show the run and its shards, then stop")
    args = ap.parse_args(argv)

    if not args.parquet_dir:
        ap.error("--parquet-dir (or PARQUET_DIR) is required")
    steps_preview = [n for n, _ in whole_wiki_steps(args.domain, Path("."), args.jobs)]
    if args.from_step and args.from_step not in steps_preview:
        ap.error(f"--from-step must be one of: {', '.join(steps_preview)}")
    if args.jobs < 1 or args.download_jobs < 1:
        ap.error("--jobs and --download-jobs must be at least 1")

    dbname = args.dbname or dumps.wiki_dbname(args.domain)
    run = dumps.find_run(dbname, prefix=args.mirror, date=args.date, fmt=args.format, log=log)
    run_dir = Path(args.work_dir) / args.domain / run.run_id
    dumps_dir = Path(args.dumps_dir) if args.dumps_dir else run_dir / "dumps"
    run_id = args.run_id or os.getenv("WCD_RUN_ID") or run.run_id
    try:
        check_run_id(run_id)
    except ValueError as e:
        ap.error(f"{e} (pass --run-id)")
    log(f"{args.domain}: {run.dbname} {run.date} {run.job}, {len(run.files)} files, {_gb(run.total_bytes)} "
        f"from {args.mirror}; run {run_dir} (run id {run_id})")
    if args.dry_run:
        for f in sorted(run.files, key=lambda f: -f.size):
            state = "downloaded" if dumps.is_downloaded(dumps_dir / f.name) else "to fetch"
            print(f"  {f.name}  {_gb(f.size)}  {state}")
        return 0

    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run.json").write_text(json.dumps({
        "domain": args.domain, "dbname": run.dbname, "date": run.date, "job": run.job, "mirror": args.mirror,
        "run_id": run_id, "namespace": args.namespace, "files": [f.__dict__ for f in run.files]}, indent=1))
    # the steps run as child processes with this environment (this process's own is left alone)
    env = {**os.environ, "PARQUET_DIR": args.parquet_dir, "STAGING_DIR": str(run_dir / "staging"), "WCD_RUN_ID": run_id}

    runner = ShardRunner(args, run, run_dir, dumps_dir, args.parquet_dir, env)
    if not runner.run_all():
        log("some shards failed; fix and re-run (finished shards are skipped). The whole-wiki steps need every shard.")
        return 1
    log("barrier: every shard is built and extracted")
    if args.shards_only:
        log("--shards-only: stopping here; re-run without it to continue with the whole-wiki steps")
        return 0
    ok = run_steps(whole_wiki_steps(args.domain, run_dir, args.jobs), run_dir, env, args.from_step)
    if ok:
        log(f"{args.domain} is built (run {run_id})")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
