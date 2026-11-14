"""wcd_pipeline — the Wikipedia Citations Database build pipeline.

Stages (numbering per docs/design/):

  1   extract      ``wcd extract`` / ``wcd extract-all``   .mwrev.zst → staging Parquet
  2   normalize    (inside extract; ``citation_normalizer``)
  3   dedup, load  ``wcd dedup``, ``wcd load``             staging → deduped Parquet → Postgres
  4–8              not yet implemented (see docs/design/executive_summary.md)

Utilities: ``wcd init-db``, ``wcd purge``.
"""
