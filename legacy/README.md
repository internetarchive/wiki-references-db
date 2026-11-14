# legacy — v1 API and explorer

The Flask application that serves `/api/v1` (`api_v1.py`, spec in
`openapi.yaml`) and the `/explorer/` UI (`explorer.py`, `templates/`), frozen
at the state of wiki-references-db v2. `models.py` is a frozen copy of the v1
SQLAlchemy models; the live models are in `packages/schema`.

It keeps serving the existing v1 database until the v2 API (`apps/api`) and
the dashboard (`apps/frontend`) are live, after which this directory is
deleted. The explorer is not carried forward; the UVA dashboard replaces it.

```
cd legacy
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
# .env with DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASS as before
gunicorn --bind 0.0.0.0:12121 app:app
```
