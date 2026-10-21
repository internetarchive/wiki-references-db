"""The query layer: plain functions taking the ``Database`` and returning
response models. Raise ``NotFound`` / ``BadRequest`` for the HTTP errors."""
from __future__ import annotations


class NotFound(Exception):
    pass


class BadRequest(Exception):
    pass


class Unavailable(Exception):
    """A Parquet dataset the endpoint needs has not been built."""
