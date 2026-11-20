"""``rust/wikidata-refs`` writes the Wikibase staging tables from Rust and keeps
a copy of their columns (``tests/schema.json``) that its own tests hold its
Arrow schemas to. ``wcd_schema.staging.WIKIBASE_SCHEMAS`` is authoritative; this
test keeps the copy's tables, columns, order and types in step."""
from __future__ import annotations

import json
from pathlib import Path

from wcd_schema.staging import WIKIBASE_SCHEMAS

SCHEMA_JSON = Path(__file__).resolve().parents[3] / "rust" / "wikidata-refs" / "tests" / "schema.json"


def test_wikibase_staging_schemas_match_the_rust_copy():
    rust = json.loads(SCHEMA_JSON.read_text())
    assert set(rust) == set(WIKIBASE_SCHEMAS)
    for table, schema in WIKIBASE_SCHEMAS.items():
        assert [[f.name, str(f.type)] for f in schema] == rust[table], table
