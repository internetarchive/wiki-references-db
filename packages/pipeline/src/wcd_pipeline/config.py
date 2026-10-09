"""Environment-driven configuration shared by every stage.

Values are read from the process environment after loading a ``.env`` file
from the current directory (python-dotenv); see ``example.env`` at the
repository root.

Postgres: ``DATABASE_URL`` is preferred. The historical ``DB_HOST`` /
``DB_PORT`` / ``DB_NAME`` / ``DB_USER`` / ``DB_PASS`` variables are still
accepted and assembled into a URL when ``DATABASE_URL`` is unset.
"""
from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

_DB_PARTS = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASS")


def database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    missing = [v for v in _DB_PARTS if not os.getenv(v)]
    if missing:
        raise RuntimeError(
            "Set DATABASE_URL, or all of "
            f"{', '.join(_DB_PARTS)} (missing: {', '.join(missing)}). "
            "Check your .env file (see example.env)."
        )
    return (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )


def staging_dir(default: str = "./staging") -> str:
    return os.getenv("STAGING_DIR", default)
