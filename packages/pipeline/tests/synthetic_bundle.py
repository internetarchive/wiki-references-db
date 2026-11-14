"""Shim: the synthetic bundle lives in the package now (``wcd_pipeline.synthetic``)."""
from wcd_pipeline.synthetic import *  # noqa: F401,F403
from wcd_pipeline.synthetic import PAGE_A, PAGE_B, PAGE_TITLES, REVISIONS, REV_A1, REV_A2, REV_A3, REV_B1, REV_B2, build_database, metadata_rows, render, write_metadata  # noqa: F401
