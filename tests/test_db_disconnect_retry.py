import pytest

from sqlalchemy.exc import DBAPIError, OperationalError


def test_retryable_disconnect_by_message() -> None:
    # Build an OperationalError with an orig that stringifies to a known retryable substring.
    exc = OperationalError(
        statement=None,
        params=None,
        orig=Exception('SSL SYSCALL error: Socket is not connected'),
    )

    from build_db import _is_retryable_db_disconnect

    assert _is_retryable_db_disconnect(exc) is True


def test_retryable_disconnect_by_connection_invalidated_flag() -> None:
    # SQLAlchemy uses connection_invalidated=True for disconnect-related errors.
    exc = DBAPIError.instance(
        statement=None,
        params=None,
        orig=Exception('anything'),
        dbapi_base_err=Exception,
        connection_invalidated=True,
    )

    from build_db import _is_retryable_db_disconnect

    assert _is_retryable_db_disconnect(exc) is True


def test_non_retryable_db_error() -> None:
    exc = OperationalError(statement=None, params=None, orig=Exception('syntax error'))

    from build_db import _is_retryable_db_disconnect

    assert _is_retryable_db_disconnect(exc) is False
