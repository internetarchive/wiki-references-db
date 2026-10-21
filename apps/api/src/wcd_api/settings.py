"""Environment-driven settings (``.env`` is loaded like the pipeline's)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()

_DB_PARTS = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASS")


def database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        missing = [v for v in _DB_PARTS if not os.getenv(v)]
        if missing:
            raise RuntimeError(f"Set DATABASE_URL, or all of {', '.join(_DB_PARTS)} (missing: {', '.join(missing)})")
        url = (f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")
    for prefix in ("postgresql://", "postgres://"):
        if url.startswith(prefix):
            return "postgresql+psycopg2://" + url[len(prefix):]
    return url


@dataclass
class Settings:
    database_url: str
    parquet_dir: str
    default_domain: str = "en.wikipedia.org"
    cors_origins: list[str] = field(default_factory=lambda: ["*"])
    #: MediaWiki API User-Agent for title → page id resolution (``/article?url=`` title form)
    user_agent: str = "WikipediaCitationsDatabase/2.0"
    contact_email: str | None = None
    pool_size: int = 5

    @classmethod
    def from_env(cls) -> "Settings":
        parquet_dir = os.getenv("PARQUET_DIR")
        if not parquet_dir:
            raise RuntimeError("Set PARQUET_DIR (the Parquet tier base directory)")
        origins = os.getenv("API_CORS_ORIGINS", "*")
        return cls(
            database_url=database_url(),
            parquet_dir=parquet_dir,
            default_domain=os.getenv("API_DEFAULT_DOMAIN", "en.wikipedia.org"),
            cors_origins=[o.strip() for o in origins.split(",") if o.strip()],
            user_agent=os.getenv("WIKIPEDIA_API_USER_AGENT", "WikipediaCitationsDatabase/2.0"),
            contact_email=os.getenv("WIKIPEDIA_API_CONTACT_EMAIL") or os.getenv("CONTACT_EMAIL"),
            pool_size=int(os.getenv("API_POOL_SIZE", "5")),
        )
