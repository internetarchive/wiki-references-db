"""Deferring a table's foreign keys while a stage bulk-loads it.

Each foreign-key check on insert locks the referenced row (``FOR KEY SHARE``),
which writes into the referenced table's heap. On enwiki that turned
normalized_citations' 68 GB heap into random writes while citation_instances
loaded, and the load stalled (plan §21–§22). A stage that fills an empty table
drops its keys first (``drop_if_empty``) and afterwards adds them ``NOT VALID``
and validates them, one set-based check per key (``restore``). ``restore`` runs
after every such phase, so a run killed in between restores the keys on its
re-run; gate 3 and gate 4 fail while any key is missing or unvalidated
(``problems``).
"""
from __future__ import annotations

import time

from sqlalchemy import text

from wcd_schema.postgres import Base


def declared(table: str) -> list[tuple[str, str, str, str]]:
    """The foreign keys the schema declares on ``table``, as (constraint name,
    column, referenced table, referenced column); names are Postgres'
    defaults, which is what ``init-db`` creates."""
    t = Base.metadata.tables[table]
    return sorted((f"{table}_{fk.parent.name}_fkey", fk.parent.name, fk.column.table.name, fk.column.name)
                  for fk in t.foreign_keys)


def present(session, table: str) -> dict[str, bool]:
    """{constraint name: validated} for the foreign keys ``table`` has now."""
    return dict(session.execute(text(
        "SELECT conname, convalidated FROM pg_constraint WHERE conrelid = CAST(:t AS regclass) AND contype = 'f'"
    ), {"t": table}).all())


def drop_if_empty(session, table: str, log=print) -> None:
    """Drop ``table``'s declared foreign keys if it has no rows."""
    if session.execute(text(f"SELECT EXISTS (SELECT 1 FROM {table})")).scalar_one():
        session.commit()
        return
    have = present(session, table)
    dropped = [name for name, *_ in declared(table) if name in have]
    for name in dropped:
        session.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT {name}"))
    session.commit()
    log(f"{table}: foreign keys dropped for the load: {', '.join(dropped) or '(none)'}")


def restore(session, table: str, log=print) -> None:
    """Add back any of ``table``'s declared foreign keys that are missing
    (``NOT VALID``, which takes no row locks) and validate every one that is
    not yet validated."""
    have = present(session, table)
    for name, col, ref_table, ref_col in declared(table):
        if name not in have:
            session.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT {name} FOREIGN KEY ({col}) "
                                 f"REFERENCES {ref_table} ({ref_col}) NOT VALID"))
            session.commit()
            have[name] = False
        if not have[name]:
            t0 = time.time()
            session.execute(text(f"ALTER TABLE {table} VALIDATE CONSTRAINT {name}"))
            session.commit()
            log(f"{table}: validated {name} in {time.time() - t0:.0f}s")


def problems(session, tables) -> list[str]:
    """Gate messages for declared foreign keys that are missing or unvalidated."""
    out = []
    for table in tables:
        names = {name for name, *_ in declared(table)}
        have = present(session, table)
        missing = sorted(names - have.keys())
        unvalidated = sorted(n for n in names & have.keys() if not have[n])
        if missing:
            out.append(f"{table}: foreign key(s) missing (an interrupted load? re-run it): {', '.join(missing)}")
        if unvalidated:
            out.append(f"{table}: foreign key(s) not validated (re-run the load): {', '.join(unvalidated)}")
    return out
