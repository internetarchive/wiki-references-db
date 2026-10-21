"""wcd_api — the Wikipedia Citations Database API (``/api/v2``).

``create_app()`` builds the FastAPI application; ``wcd-api`` (or
``uvicorn wcd_api.main:app``) serves it. Endpoints and their table mapping
are in docs/design/proposed_api.md; the query layer is ``wcd_api.queries``.
"""
