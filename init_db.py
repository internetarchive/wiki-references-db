import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from models import Base


def _db_url() -> str:
    return (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )


def main() -> None:
    # Load environment variables and create all tables defined in models.Base
    load_dotenv()
    engine = create_engine(
        _db_url(),
        pool_pre_ping=True,
        pool_recycle=int(os.getenv('DB_POOL_RECYCLE', '1800')),
        # Avoid dumping huge bound-parameter payloads in exception text when a statement fails.
        hide_parameters=True,
    )
    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    main()
