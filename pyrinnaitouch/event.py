"""Module to capture event handling"""
import logging

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

_LOGGER = logging.getLogger(__name__)


class Event():
    """Simple event class."""

    def __init__(self) -> None:
        self.__eventhandlers = []

    def __iadd__(self, handler) -> Self:
        """Add event handler."""
        if handler not in self.__eventhandlers:
            self.__eventhandlers.append(handler)
        return self

    def __isub__(self, handler) -> Self:
        """Remove event handler."""
        if handler in self.__eventhandlers:
            self.__eventhandlers.remove(handler)
        return self

    def __call__(self, *args, **keywargs) -> None:
        """Call every handler, isolating them from each other's failures."""
        for eventhandler in list(self.__eventhandlers):
            if eventhandler is None:
                continue
            try:
                eventhandler(*args, **keywargs)
            except Exception:  # pylint: disable=broad-except
                _LOGGER.exception("Update handler %r failed", eventhandler)
