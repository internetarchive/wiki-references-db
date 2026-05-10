import os


def _reset_wikiapi_env_state(wikiapi, monkeypatch):
    # Ensure each test starts from a clean state even though wikiapi caches `.env` loading.
    monkeypatch.setattr(wikiapi, "_ENV_LOADED", False)
    monkeypatch.delenv("CONTACT_EMAIL", raising=False)
    monkeypatch.delenv("SECONDARY_USER_AGENT", raising=False)


def test_get_contact_email_loads_from_dotenv_in_cwd(monkeypatch, tmp_path):
    from refs_extractor import wikiapi

    _reset_wikiapi_env_state(wikiapi, monkeypatch)

    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("CONTACT_EMAIL=test@example.com\n", encoding="utf-8")

    assert wikiapi.get_contact_email() == "test@example.com"


def test_get_contact_email_loads_from_dotenv_in_parent_dir(monkeypatch, tmp_path):
    from refs_extractor import wikiapi

    _reset_wikiapi_env_state(wikiapi, monkeypatch)

    (tmp_path / ".env").write_text("CONTACT_EMAIL=parent@example.com\n", encoding="utf-8")
    child = tmp_path / "child"
    child.mkdir()
    monkeypatch.chdir(child)

    assert wikiapi.get_contact_email() == "parent@example.com"


def test_env_var_overrides_dotenv(monkeypatch, tmp_path):
    from refs_extractor import wikiapi

    _reset_wikiapi_env_state(wikiapi, monkeypatch)

    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("CONTACT_EMAIL=fromfile@example.com\n", encoding="utf-8")
    monkeypatch.setenv("CONTACT_EMAIL", "fromenv@example.com")

    assert wikiapi.get_contact_email() == "fromenv@example.com"


def test_get_header_includes_secondary_user_agent_from_dotenv(monkeypatch, tmp_path):
    from refs_extractor import wikiapi

    _reset_wikiapi_env_state(wikiapi, monkeypatch)

    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "CONTACT_EMAIL=test@example.com\nSECONDARY_USER_AGENT=MyApp/2.3\n",
        encoding="utf-8",
    )

    headers = wikiapi.get_header()
    assert "User-Agent" in headers
    assert "test@example.com" in headers["User-Agent"]
    assert headers["User-Agent"].endswith(" MyApp/2.3")
