"""DuckDB connections opened by the stages take their limits from the environment."""
from wcd_pipeline.config import duckdb_connect


def test_duckdb_connect_applies_limits(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "1GB")
    monkeypatch.setenv("DUCKDB_TEMP_DIRECTORY", str(tmp_path))
    monkeypatch.setenv("DUCKDB_THREADS", "2")
    con = duckdb_connect()
    mem, tmp, threads = con.execute("SELECT current_setting('memory_limit'), current_setting('temp_directory'), "
                                    "current_setting('threads')").fetchone()
    assert mem in ("1.0 GiB", "953.6 MiB", "1.0 GB") and int(threads) == 2
    # each connection spills into its own directory under DUCKDB_TEMP_DIRECTORY (DuckDB's
    # spill file names are the same in every process)
    other = duckdb_connect().execute("SELECT current_setting('temp_directory')").fetchone()[0]
    assert tmp.startswith(str(tmp_path) + "/") and other.startswith(str(tmp_path) + "/") and tmp != other


def test_duckdb_connect_defaults_when_unset(monkeypatch):
    monkeypatch.delenv("DUCKDB_MEMORY_LIMIT", raising=False)
    monkeypatch.delenv("DUCKDB_TEMP_DIRECTORY", raising=False)
    assert duckdb_connect().execute("SELECT 1").fetchone() == (1,)
