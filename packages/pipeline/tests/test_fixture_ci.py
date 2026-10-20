"""The whole pipeline on the committed ci fixture shard, stage by stage, with every gate,
and a comparison of every output with the golden snapshots in fixtures/golden/ci/.

Input: fixtures/shards/ci/ — the RevisionChest bundle and metadata for the ci tier of
fixtures/pages.tsv (51 English Wikipedia pages with full history to the cutoff; see
fixtures/README.md). The tests run in file order, one per pipeline command, exactly the
sequence of the README:

    init-db --no-indexes → stage0-load → gate 0 → extract-all → dedup → load → gate 3
    → init-db --add-indexes → parts → gate 4 → denorm-export → ref-name-links → gate 5
    → match → gate 6 → materialize → gate 7 → revision-stats → gate 8

If a step fails, the later steps are skipped (they would only fail for the same reason).
Then come coverage checks (the fixture still contains what it was selected for) and one
golden comparison per Postgres table and per Parquet dataset.

Needs a scratch Postgres database; its schema ``public`` is dropped and recreated.

    export WCD_TEST_DATABASE_URL=postgresql://user:pass@127.0.0.1:5432/wcd_fixture_test
    # first run, or after an intentional pipeline change: record the goldens
    UPDATE_GOLDEN=1 uv run pytest packages/pipeline/tests/test_fixture_ci.py -v
    git diff --stat fixtures/golden/ci/        # review summary.json before committing
    # every other run: compare
    uv run pytest packages/pipeline/tests/test_fixture_ci.py -v

Optional: ``WCD_FIXTURE_WORK=/some/dir`` keeps the staging/Parquet output there instead
of a temporary directory (inspect it afterwards; it is emptied at the start of a run),
and ``WCD_FIXTURE_JOBS`` sets the worker count for extract-all / match / revision-stats
(default 4). Step timings are written to ``<work>/timings.json``.

Golden format. Surrogate keys that depend on load order (``normalized_citations.id``,
``web_resources.id``, ``wiki_templates.id``, ``containers.id``, ``domains.id``,
``revision_bundles.id``) are replaced by the row's natural key (``normalized_sha1``,
``url_hash``, template name, label, domain value, file path) wherever they appear;
``page_summary_cache.updated_at`` is dropped; floats are rounded to 9 places and JSONB
is serialised with sorted keys. Content-derived ids (citation instance ids, source ids)
are kept. A snapshot of at most FINGERPRINT_ROWS rows is stored in full
(``<name>.parquet``, compared row for row); a larger one as per-page digests
(``<name>.fp.parquet``: row count and MD5 of the sorted rows per page_id, or per hash
prefix for tables without a page), so a mismatch still names the pages that changed.
``summary.json`` lists row counts and the fixture coverage, for review in diffs.
"""
from __future__ import annotations

import datetime as dt
import decimal
import json
import os
import shutil
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from wcd_pipeline import cli
from wcd_schema import parquet as layout
from wcd_schema.postgres import Base

REPO_ROOT = Path(__file__).resolve().parents[3]
SHARD = REPO_ROOT / "fixtures" / "shards" / "ci"
EXPORT = REPO_ROOT / "fixtures" / "export.json"
PAGES_CI = REPO_ROOT / "fixtures" / "pages.ci.txt"
GOLDEN = REPO_ROOT / "fixtures" / "golden" / "ci"
DOMAIN = "en.wikipedia.org"
JOBS = os.environ.get("WCD_FIXTURE_JOBS", "4")
FINGERPRINT_ROWS = 50_000
UPDATE = bool(os.environ.get("UPDATE_GOLDEN"))

#: tables whose integer primary key is a surrogate, and the column that replaces it
NATURAL_KEY = {
    "normalized_citations": "normalized_sha1",
    "web_resources": "url_hash",
    "wiki_templates": "name",
    "containers": "label",
    "domains": "value",
    "revision_bundles": "file_path",
}
DROP_COLUMNS = {"page_summary_cache": {"updated_at"}}
PG_TABLES = sorted(Base.metadata.tables)
PQ_DATASETS = sorted(layout.SCHEMAS)
PARTITION_COLUMNS = {"domain", "page_bucket"}
PQ_DROP = {"citation_instances_denorm": {"normalized_id"}}   # load-order surrogate; normalized_sha1 is there

pytestmark = [
    pytest.mark.skipif(not os.environ.get("WCD_TEST_DATABASE_URL"),
                       reason="set WCD_TEST_DATABASE_URL to a scratch Postgres database"),
    pytest.mark.skipif(not (SHARD / "meta.parquet").exists(), reason=f"no fixture shard at {SHARD}"),
]


# ---------------------------------------------------------------------------
# Running the pipeline, one test per command
# ---------------------------------------------------------------------------

class Run:
    def __init__(self, work: Path):
        self.work = work
        self.parquet = work / "parquet"
        self.staging = work / "staging"
        self.failed: str | None = None
        self.timings: dict[str, float] = {}

    def wcd(self, label: str, *argv: str) -> None:
        """Run one `wcd` command; record its time; mark the run failed if it does not return 0."""
        if self.failed:
            pytest.skip(f"skipped: earlier step {self.failed!r} failed")
        t0 = time.monotonic()
        try:
            rc = cli.main(list(argv))
        except BaseException:
            self.failed = label
            raise
        finally:
            self.timings[label] = round(time.monotonic() - t0, 2)
            (self.work / "timings.json").write_text(json.dumps(self.timings, indent=2) + "\n")
        if rc != 0:
            self.failed = label
            pytest.fail(f"`wcd {' '.join(argv)}` returned {rc}")

    def require_complete(self) -> None:
        if self.failed:
            pytest.skip(f"skipped: pipeline step {self.failed!r} failed")


@pytest.fixture(scope="module")
def run(tmp_path_factory) -> Run:
    keep = os.environ.get("WCD_FIXTURE_WORK")
    if keep:
        work = Path(keep)
        if work.exists():
            shutil.rmtree(work)
        work.mkdir(parents=True)
    else:
        work = tmp_path_factory.mktemp("fixture_ci")
    os.environ["DATABASE_URL"] = os.environ["WCD_TEST_DATABASE_URL"]
    return Run(work)


def test_00_fresh_schema(run: Run):
    from sqlalchemy import create_engine, text
    from wcd_pipeline.config import database_url
    engine = create_engine(database_url())   # DATABASE_URL, set by the fixture, with the psycopg2 driver
    with engine.begin() as con:
        con.execute(text("DROP SCHEMA public CASCADE"))
        con.execute(text("CREATE SCHEMA public"))
    engine.dispose()
    run.wcd("init-db --no-indexes", "init-db", "--no-indexes")


def test_01_stage0_load(run: Run):
    run.wcd("stage0-load", "stage0-load", "--domain", DOMAIN, "--metadata", str(SHARD / "meta.parquet"),
            "--parquet-dir", str(run.parquet))
    run.wcd("gate 0", "gate", "0", "--domain", DOMAIN, "--parquet-dir", str(run.parquet))


def test_02_extract_all(run: Run):
    run.wcd("extract-all", "extract-all", "-d", str(SHARD), "-o", str(run.staging), "--domain", DOMAIN,
            "--parquet-dir", str(run.parquet), "--jobs", JOBS)


def test_03_dedup_load(run: Run):
    run.wcd("dedup", "dedup", "-d", str(run.staging))
    run.wcd("load", "load", "-d", str(run.staging))
    run.wcd("gate 3", "gate", "3", "--domain", DOMAIN, "--parquet-dir", str(run.parquet), "--all-buckets")
    run.wcd("init-db --add-indexes", "init-db", "--add-indexes")


def test_04_parts(run: Run):
    run.wcd("parts", "parts", "--domain", DOMAIN)
    run.wcd("gate 4", "gate", "4", "--domain", DOMAIN, "--parquet-dir", str(run.parquet))


def test_05_denorm_export_and_ref_name_links(run: Run):
    run.wcd("denorm-export", "denorm-export", "--domain", DOMAIN, "--parquet-dir", str(run.parquet))
    run.wcd("ref-name-links", "ref-name-links", "--domain", DOMAIN, "--parquet-dir", str(run.parquet))
    run.wcd("gate 5", "gate", "5", "--domain", DOMAIN, "--parquet-dir", str(run.parquet))


def test_06_match(run: Run):
    run.wcd("match", "match", "--domain", DOMAIN, "--parquet-dir", str(run.parquet), "--jobs", JOBS)
    run.wcd("gate 6", "gate", "6", "--domain", DOMAIN, "--parquet-dir", str(run.parquet))


def test_07_materialize(run: Run):
    run.wcd("materialize", "materialize", "--domain", DOMAIN, "--parquet-dir", str(run.parquet))
    run.wcd("gate 7", "gate", "7", "--domain", DOMAIN, "--parquet-dir", str(run.parquet))


def test_08_revision_stats(run: Run):
    run.wcd("revision-stats", "revision-stats", "--domain", DOMAIN, "--parquet-dir", str(run.parquet),
            "--jobs", JOBS)
    run.wcd("gate 8", "gate", "8", "--domain", DOMAIN, "--parquet-dir", str(run.parquet))


# ---------------------------------------------------------------------------
# Snapshots: every Postgres table and Parquet dataset in canonical form
# ---------------------------------------------------------------------------

def _table(con, sql: str) -> pa.Table:
    """A DuckDB query result as a pyarrow Table, across DuckDB versions."""
    res = con.execute(sql)
    for method in ("to_arrow_table", "fetch_arrow_table", "arrow"):
        if hasattr(res, method):
            out = getattr(res, method)()
            break
    return out.read_all() if hasattr(out, "read_all") else out


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _plain(v):
    if isinstance(v, float):
        return round(v, 9)
    if isinstance(v, decimal.Decimal):
        return round(float(v), 9)
    if isinstance(v, (dict, list)):
        return json.dumps(v, sort_keys=True, ensure_ascii=False)
    if isinstance(v, memoryview):
        return bytes(v).hex()
    return v


def _arrow(columns: list[str], rows: list[tuple]) -> pa.Table:
    arrays = []
    for i, _ in enumerate(columns):
        values = [r[i] for r in rows]
        arr = pa.array(values) if values else pa.array([], pa.string())
        if pa.types.is_null(arr.type):
            arr = arr.cast(pa.string())
        arrays.append(arr)
    return pa.table(arrays, names=columns)


def _fk_target(table, column) -> str | None:
    for fk in column.foreign_keys:
        target = fk.column.table.name
        # citation_source_resolution → normalized_citation_parts(normalized_id, part_index)
        if target == "normalized_citation_parts" and column.name == "normalized_id":
            return "normalized_citations"
        return target
    if column.name == "normalized_id":
        return "normalized_citations"
    return None


def pg_snapshots(dsn: str) -> dict[str, pa.Table]:
    import psycopg2

    con = psycopg2.connect(dsn)
    try:
        cur = con.cursor()
        lookups: dict[str, dict] = {}
        for table, key in NATURAL_KEY.items():
            cur.execute(f"SELECT id, {key} FROM {table}")
            lookups[table] = dict(cur.fetchall())
        out = {}
        for name in PG_TABLES:
            table = Base.metadata.tables[name]
            cols, mappers = [], []
            for c in table.columns:
                if c.name in DROP_COLUMNS.get(name, ()):
                    continue
                if name in NATURAL_KEY and c.name == "id":
                    continue                     # the natural key column is already in the row
                target = _fk_target(table, c)
                if target in NATURAL_KEY:
                    cols.append((c.name, f"{c.name}__{NATURAL_KEY[target]}"))
                    mappers.append(lookups[target])
                else:
                    cols.append((c.name, c.name))
                    mappers.append(None)
            cur.execute(f"SELECT {', '.join(src for src, _ in cols)} FROM {name}")
            rows = [tuple(_plain(m.get(v, f"<dangling {v}>") if (m is not None and v is not None) else v)
                          for v, m in zip(r, mappers)) for r in cur.fetchall()]
            out[f"pg.{name}"] = _arrow([dst for _, dst in cols], rows)
        return out
    finally:
        con.close()


def pq_snapshot(con, base: Path, dataset: str) -> pa.Table | None:
    if not layout.dataset_dir(base, dataset).exists():
        return None
    src = f"read_parquet('{layout.dataset_glob(base, dataset)}', hive_partitioning=true, union_by_name=true)"
    desc = con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()
    drop = PARTITION_COLUMNS | PQ_DROP.get(dataset, set())
    select = [f"round({_q(n)}, 9) AS {_q(n)}" if t in ("DOUBLE", "FLOAT") else _q(n)
              for n, t, *_ in desc if n not in drop]
    return _table(con, f"SELECT {', '.join(select)} FROM {src}")


def _group_expr(columns) -> str:
    for c in ("page_id", "appears_on_page_id", "numeric_page_id"):
        if c in columns:
            return f"CAST({_q(c)} AS VARCHAR)"
    return "substr(h, 1, 3)"


def fingerprint(con, view: str, columns: list[str]) -> pa.Table:
    packed = ", ".join(f"{_q(c)} := {_q(c)}" for c in columns)
    return _table(con, f"""
        WITH r AS (SELECT *, md5(CAST(struct_pack({packed}) AS VARCHAR)) AS h FROM {view})
        SELECT {_group_expr(columns)} AS grp, count(*) AS n, md5(string_agg(h, '' ORDER BY h)) AS digest
        FROM r GROUP BY 1 ORDER BY 1""")


@pytest.fixture(scope="module")
def snapshots(run: Run) -> dict[str, pa.Table]:
    run.require_complete()
    snaps = pg_snapshots(os.environ["WCD_TEST_DATABASE_URL"])
    con = duckdb.connect()
    for ds in PQ_DATASETS:
        t = pq_snapshot(con, run.parquet, ds)
        if t is not None:
            snaps["pq." + ds.replace("/", ".")] = t
    return snaps


@pytest.fixture(scope="module")
def duck():
    return duckdb.connect()


# ---------------------------------------------------------------------------
# Coverage: the fixture still holds what it was selected for
# ---------------------------------------------------------------------------

def coverage(con) -> dict[str, int]:
    def one(sql):
        return con.execute(sql).fetchone()[0]
    c = {
        "pages": one("SELECT count(*) FROM pg_page_metadata"),
        "revisions": one("SELECT count(*) FROM pg_revisions"),
        "citation_instances": one("SELECT count(*) FROM pg_citation_instances"),
        "normalized_citations": one("SELECT count(*) FROM pg_normalized_citations"),
        "named_ref_uses": one("SELECT count(*) FROM pq_named_ref_uses"),
        "global_sources": one("SELECT count(*) FROM pg_global_sources"),
        "global_sources_multi_page": one("SELECT count(*) FROM pg_global_sources WHERE n_pages >= 2"),
    }
    for t, n in con.execute("SELECT citation_type, count(*) FROM pg_normalized_citations GROUP BY 1").fetchall():
        c[f"type:{t}"] = n
    for ident in ("doi", "isbn", "pmid", "pmc", "arxiv", "wikidata_qid"):
        c[f"parts_with_{ident}"] = one(
            f"SELECT count(*) FROM pg_normalized_citation_parts WHERE extracted_{ident} IS NOT NULL")
    c["parts_sfn"] = one("SELECT count(*) FROM pg_normalized_citation_parts WHERE norm_subtype = 'shorthand_template'")
    for v, n in con.execute("SELECT verdict, count(*) FROM pg_global_sources GROUP BY 1").fetchall():
        c[f"verdict:{v}"] = n
    return dict(sorted(c.items()))


@pytest.fixture(scope="module")
def cov(snapshots, duck) -> dict[str, int]:
    for name, t in snapshots.items():
        duck.register(name.replace(".", "_"), t)
    return coverage(duck)


def test_10_fixture_matches_export(run: Run, cov):
    export = json.loads(EXPORT.read_text())["outputs"]["fixture-ci.xml.bz2"]
    ci_pages = {int(l.split()[0]) for l in PAGES_CI.read_text().splitlines() if l.strip() and not l.startswith("#")}
    assert cov["pages"] == export["pages"] == len(ci_pages)
    assert cov["revisions"] == export["revisions"]


def test_11_coverage(cov):
    """What the ci tier was chosen to exercise. A failure here means the fixture (or the
    extraction of it) lost a whole kind of citation, not that a number moved a little."""
    print(json.dumps(cov, indent=2))
    for t in ("template", "ref_tag", "shorthand"):
        assert cov.get(f"type:{t}", 0) > 0, f"no {t} citations"
    assert cov["named_ref_uses"] > 0, "no <ref name=…/> reuse"
    assert cov["parts_sfn"] > 0, "no {{sfn}} parts"
    for ident in ("doi", "isbn", "pmid"):
        assert cov[f"parts_with_{ident}"] > 0, f"no parts with a {ident.upper()}"
    assert cov["global_sources_multi_page"] > 0, "no source is cited on two or more pages"


# ---------------------------------------------------------------------------
# Goldens
# ---------------------------------------------------------------------------

def _write_golden(duck, name: str, t: pa.Table) -> dict:
    duck.register("snap", t)
    if t.num_rows > FINGERPRINT_ROWS:
        fp = fingerprint(duck, "snap", t.column_names)
        pq.write_table(fp, GOLDEN / f"{name}.fp.parquet", compression="zstd")
        mode = "fingerprint"
    else:
        ordered = _table(duck, "SELECT * FROM snap ORDER BY ALL")
        pq.write_table(ordered, GOLDEN / f"{name}.parquet", compression="zstd")
        mode = "full"
    duck.unregister("snap")
    return {"rows": t.num_rows, "columns": t.column_names, "stored": mode}


def test_20_update_goldens(run: Run, snapshots, cov, duck):
    if not UPDATE:
        pytest.skip("set UPDATE_GOLDEN=1 to record fixtures/golden/ci/")
    if GOLDEN.exists():
        shutil.rmtree(GOLDEN)
    GOLDEN.mkdir(parents=True)
    tables = {name: _write_golden(duck, name, t) for name, t in sorted(snapshots.items())}
    summary = {"shard": str(SHARD.relative_to(REPO_ROOT)), "coverage": cov, "snapshots": tables,
               "timings_seconds": run.timings,
               "recorded": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}
    (GOLDEN / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n")


SNAPSHOT_NAMES = [f"pg.{t}" for t in PG_TABLES] + ["pq." + d.replace("/", ".") for d in PQ_DATASETS]


@pytest.mark.parametrize("name", SNAPSHOT_NAMES)
def test_30_matches_golden(name: str, snapshots, duck):
    if UPDATE:
        pytest.skip("goldens were just recorded")
    full, fp = GOLDEN / f"{name}.parquet", GOLDEN / f"{name}.fp.parquet"
    t = snapshots.get(name)
    if not full.exists() and not fp.exists():
        if t is None:
            pytest.skip(f"{name}: not produced and no golden")
        pytest.fail(f"no golden for {name}; record with UPDATE_GOLDEN=1 (see the module docstring)")
    assert t is not None, f"{name} has a golden but the run did not produce it"
    duck.register("cur", t)
    try:
        if full.exists():
            gold = f"read_parquet('{full}')"
            g_cols = [r[0] for r in duck.execute(f"DESCRIBE SELECT * FROM {gold}").fetchall()]
            assert g_cols == t.column_names, f"{name}: columns changed\n golden: {g_cols}\n now:    {t.column_names}"
            n_g = duck.execute(f"SELECT count(*) FROM {gold}").fetchone()[0]
            only_g = duck.execute(f"SELECT * FROM {gold} EXCEPT ALL SELECT * FROM cur LIMIT 5").fetchall()
            only_c = duck.execute(f"SELECT * FROM cur EXCEPT ALL SELECT * FROM {gold} LIMIT 5").fetchall()
            assert not only_g and not only_c, (
                f"{name}: golden {n_g} rows, now {t.num_rows}\n only in golden: {only_g}\n only now: {only_c}")
        else:
            duck.register("cur_fp", fingerprint(duck, "cur", t.column_names))
            diff = duck.execute(f"""
                SELECT coalesce(g.grp, c.grp) AS grp, g.n AS golden_rows, c.n AS rows_now
                FROM read_parquet('{fp}') g FULL OUTER JOIN cur_fp c ON g.grp = c.grp
                WHERE g.digest IS DISTINCT FROM c.digest ORDER BY 1""").fetchall()
            duck.unregister("cur_fp")
            key = "page" if _group_expr(t.column_names) != "substr(h, 1, 3)" else "hash prefix"
            assert not diff, (f"{name}: {len(diff)} {key} groups differ (group, golden rows, rows now): "
                              f"{diff[:10]}{' …' if len(diff) > 10 else ''}")
    finally:
        duck.unregister("cur")
