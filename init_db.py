import argparse
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from models import Base


def _db_url() -> str:
    required = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASS']
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        raise RuntimeError(
            f"Missing required environment variable(s): {', '.join(missing)}. "
            f"Check your .env file (see example.env)."
        )
    return (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Create database tables.")
    parser.add_argument(
        "--table",
        help="Create only the specified table (by SQLAlchemy model table name). "
             "If omitted, all tables are created.",
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
    args = parser.parse_args()

    if sum([args.no_indexes, args.add_indexes, args.drop_indexes]) > 1:
        parser.error("Only one of --no-indexes, --add-indexes, --drop-indexes may be specified.")

    # Load environment variables and create all tables defined in models.Base
    load_dotenv()
    engine = create_engine(
        _db_url(),
        pool_pre_ping=True,
        pool_recycle=int(os.getenv('DB_POOL_RECYCLE', '1800')),
        # Avoid dumping huge bound-parameter payloads in exception text when a statement fails.
        hide_parameters=True,
    )

    if args.add_indexes:
        count = 0
        for table in Base.metadata.tables.values():
            for idx in table.indexes:
                if not idx.unique:
                    try:
                        idx.create(bind=engine)
                        print(f"Created index {idx.name}")
                        count += 1
                    except Exception as e:
                        print(f"Skipping index {idx.name}: {e}")
        print(f"Done. Created {count} index(es).")
        return

    if args.drop_indexes:
        count = 0
        for table in Base.metadata.tables.values():
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
            table = Base.metadata.tables.get(args.table)
            if table is None:
                available = sorted(Base.metadata.tables.keys())
                parser.error(f"Unknown table '{args.table}'. Available tables: {', '.join(available)}")
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
