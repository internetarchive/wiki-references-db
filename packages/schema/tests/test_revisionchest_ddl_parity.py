"""RevisionChest ships a copy of the Stage-0 DDL
(``crates/revisionchest-meta/tests/schema.sql``) so its Postgres sink can be
tested without the Python workspace. The models here are authoritative; this
test keeps the copy's tables, columns and nullability in step."""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from wcd_schema.postgres import Base

SCHEMA_SQL = Path(__file__).resolve().parents[3] / "rust" / "revisionchest" / "crates" / "revisionchest-meta" / "tests" / "schema.sql"

_TABLE = re.compile(r"CREATE TABLE (\w+) \((.*?)\);", re.S)


def _parse(sql: str) -> dict[str, dict[str, bool]]:
    """table -> {column: nullable}"""
    out = {}
    for name, body in _TABLE.findall(sql):
        cols = {}
        for line in body.splitlines():
            line = line.strip().rstrip(",")
            if not line or line.startswith(("PRIMARY KEY", "UNIQUE", "FOREIGN KEY", "--")):
                continue
            col = line.split()[0]
            nullable = "NOT NULL" not in line and "PRIMARY KEY" not in line
            cols[col] = nullable
        out[name] = cols
    return out


@pytest.mark.skipif(not SCHEMA_SQL.exists(), reason="RevisionChest submodule not checked out")
def test_stage0_ddl_matches_models():
    rust = _parse(SCHEMA_SQL.read_text())
    assert rust, "no CREATE TABLE statements parsed"
    for table, cols in rust.items():
        model = Base.metadata.tables[table]
        assert set(cols) == {c.name for c in model.columns}, table
        for c in model.columns:
            assert cols[c.name] == bool(c.nullable), f"{table}.{c.name} nullability"
