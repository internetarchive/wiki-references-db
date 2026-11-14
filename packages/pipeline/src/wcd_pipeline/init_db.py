import argparse
import os
import time
from sqlalchemy import create_engine
from wcd_schema.postgres import Base
from .config import database_url as _db_url


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Create database tables.")
    parser.add_argument(
        "--table", nargs="+", metavar="TABLE",
        help="Only these tables (SQLAlchemy model table names): create them, or with "
             "--add-indexes / --drop-indexes, only their secondary indexes. If omitted, all tables.",
    )
    parser.add_argument(
        "--no-indexes", action="store_true",
        help="Create tables without secondary indexes (for bulk loading).",
    )
    parser.add_argument(
        "--add-indexes", action="store_true",
        help="Create only the secondary indexes on existing tables (run after bulk loading).",
    )
    parser.add_argument(
        "--drop-indexes", action="store_true",
        help="Drop secondary (non-unique) indexes from existing tables.",
    )
    args = parser.parse_args(argv)

    if sum([args.no_indexes, args.add_indexes, args.drop_indexes]) > 1:
        parser.error("Only one of --no-indexes, --add-indexes, --drop-indexes may be specified.")

    engine = create_engine(
        _db_url(),
        pool_pre_ping=True,
        pool_recycle=int(os.getenv('DB_POOL_RECYCLE', '1800')),
        # Avoid dumping huge bound-parameter payloads in exception text when a statement fails.
        hide_parameters=True,
    )

    if args.table:
        unknown = sorted(set(args.table) - Base.metadata.tables.keys())
        if unknown:
            parser.error(f"Unknown table(s) {', '.join(unknown)}. "
                         f"Available tables: {', '.join(sorted(Base.metadata.tables))}")
    tables = [Base.metadata.tables[t] for t in args.table] if args.table else list(Base.metadata.tables.values())

    if args.add_indexes:
        # After a bulk load, build only the loaded tables' indexes (--table), so
        # tables a later stage fills are not loaded through secondary indexes.
        count = 0
        for table in tables:
            for idx in table.indexes:
                if not idx.unique:
                    t0 = time.time()
                    try:
                        idx.create(bind=engine)
                        print(f"Created index {idx.name} in {time.time() - t0:.0f}s", flush=True)
                        count += 1
                    except Exception as e:
                        print(f"Skipping index {idx.name}: {e}", flush=True)
        print(f"Done. Created {count} index(es).")
        return

    if args.drop_indexes:
        count = 0
        for table in tables:
            for idx in table.indexes:
                if not idx.unique:
                    try:
                        idx.drop(bind=engine)
                        print(f"Dropped index {idx.name}")
                        count += 1
                    except Exception as e:
                        print(f"Skipping index {idx.name}: {e}")
        print(f"Done. Dropped {count} index(es).")
        return

    # Collect secondary indexes to omit when --no-indexes is set
    saved_indexes = {}
    if args.no_indexes:
        for table_name, table in Base.metadata.tables.items():
            secondary = [idx for idx in table.indexes if not idx.unique]
            saved_indexes[table_name] = secondary
            for idx in secondary:
                table.indexes.remove(idx)

    try:
        if args.table:
            for table in tables:
                table.create(bind=engine, checkfirst=True)
        else:
            Base.metadata.create_all(bind=engine)
    finally:
        # Restore indexes on metadata so the module stays consistent
        for table_name, indexes in saved_indexes.items():
            for idx in indexes:
                Base.metadata.tables[table_name].indexes.add(idx)


if __name__ == "__main__":
    main()
