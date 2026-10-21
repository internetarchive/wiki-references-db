"""API tests run against the synthetic pipeline run (``wcd_pipeline.synthetic.build_database``):
two pages, five revisions, every stage 0–8. Needs ``WCD_TEST_DATABASE_URL`` (a scratch
Postgres database whose ``public`` schema is dropped); otherwise the tests are skipped."""
from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

from wcd_api.main import create_app
from wcd_api.settings import Settings

DSN = os.environ.get("WCD_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not DSN, reason="set WCD_TEST_DATABASE_URL to a scratch Postgres database")


@pytest.fixture(scope="session")
def synthetic(tmp_path_factory):
    if not DSN:
        pytest.skip("set WCD_TEST_DATABASE_URL to a scratch Postgres database")
    from wcd_pipeline.synthetic import build_database

    work = tmp_path_factory.mktemp("api")
    info = build_database(DSN, work)
    return info


@pytest.fixture(scope="session")
def client(synthetic):
    settings = Settings(database_url=("postgresql+psycopg2://" + DSN.split("://", 1)[1]), parquet_dir=synthetic["parquet_dir"],
                        contact_email="test@example.org")
    titles = {("en.wikipedia.org", "Example Island"): 101, ("en.wikipedia.org", "Second Page"): 202}

    def fake_resolver(domain, title, user_agent, contact_email):
        return titles.get((domain, title))

    app = create_app(settings, title_resolver=fake_resolver, frontend_dir=None)
    with TestClient(app) as c:
        yield c
