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
    args = parser.parse_args()

    # Load environment variables and create all tables defined in models.Base
    load_dotenv()
    engine = create_engine(
        _db_url(),
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
        table.create(bind=engine, checkfirst=True)
    else:
        Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    main()
