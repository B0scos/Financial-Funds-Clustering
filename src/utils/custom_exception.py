"""Custom exception class that logs details on instantiation.

When raised, this exception records the original exception and logs a full
traceback into the project's `logs/` folder using the project's logger.
"""
from __future__ import annotations

import traceback
from typing import Any, Optional

from .custom_logger import get_logger

_logger = get_logger(__name__)


class CustomException(Exception):
    """A project-specific exception that logs the error and traceback.

    Parameters
    ----------
    message: str
        Human-readable message describing the error.
    original_exception: Optional[Any]
        The original caught exception (if any). This is attached to the
        instance and included in the log output.
    """

    def __init__(self, message: str, original_exception: Optional[Any] = None) -> None:
        self.message = message
        self.original_exception = original_exception

        tb = None
        # If there's an active exception, get its formatted traceback
        try:
            tb = traceback.format_exc()
        except Exception:
            tb = None

        full_message = message
        if original_exception is not None:
            full_message = f"{full_message} | Original: {repr(original_exception)}"
        if tb and tb != "None\n":
            full_message = f"{full_message}\nTraceback:\n{tb}"

        # Log the error with traceback
        _logger.error(full_message)

        # Initialize the base Exception with the full message
        super().__init__(full_message)


def raise_from_exception(message: str, exc: Optional[BaseException] = None) -> None:
    """Convenience helper that logs and raises a CustomException.

    Example:
        try:
            ...
        except Exception as e:
            raise_from_exception("Failed to do thing", e)
    """
    raise CustomException(message, original_exception=exc)
