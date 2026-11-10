"""DuckDB connections opened by the stages take their limits from the environment."""
from wcd_pipeline.config import duckdb_connect


def test_duckdb_connect_applies_limits(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "1GB")
    monkeypatch.setenv("DUCKDB_TEMP_DIRECTORY", str(tmp_path))
    con = duckdb_connect()
    mem, tmp = con.execute("SELECT current_setting('memory_limit'), current_setting('temp_directory')").fetchone()
    assert mem in ("1.0 GiB", "953.6 MiB", "1.0 GB") and tmp == str(tmp_path)


def test_duckdb_connect_defaults_when_unset(monkeypatch):
    monkeypatch.delenv("DUCKDB_MEMORY_LIMIT", raising=False)
    monkeypatch.delenv("DUCKDB_TEMP_DIRECTORY", raising=False)
    assert duckdb_connect().execute("SELECT 1").fetchone() == (1,)
