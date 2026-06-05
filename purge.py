import argparse
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from models import Base


def main() -> None:
    parser = argparse.ArgumentParser(description="Drop database tables.")
    parser.add_argument(
        "--table",
        help="Drop only the specified table (by SQLAlchemy model table name). "
             "If omitted, all tables are dropped.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete all rows instead of dropping the table(s).",
    )
    args = parser.parse_args()

    load_dotenv()
    required = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASS']
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        raise RuntimeError(
            f"Missing required environment variable(s): {', '.join(missing)}. "
            f"Check your .env file (see example.env)."
        )
    DB = (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    Engine = create_engine(
        DB,
        pool_pre_ping=True,
        pool_recycle=int(os.getenv('DB_POOL_RECYCLE', '1800')),
        # Avoid dumping huge bound-parameter payloads in exception text when a statement fails.
        hide_parameters=True,
    )

    if args.table:
        table = Base.metadata.tables.get(args.table)
        if table is None:
            available = sorted(Base.metadata.tables.keys())
            parser.error(f"Unknown table '{args.table}'. Available tables: {', '.join(available)}")
        if args.truncate:
            with Engine.begin() as conn:
                conn.execute(table.delete())
        else:
            table.drop(bind=Engine, checkfirst=True)
    else:
        if args.truncate:
            with Engine.begin() as conn:
                for table in reversed(Base.metadata.sorted_tables):
                    conn.execute(table.delete())
        else:
            Base.metadata.drop_all(Engine)


if __name__ == "__main__":
    main()
