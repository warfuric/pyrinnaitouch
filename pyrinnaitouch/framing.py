"""Framing of the unit's byte stream into (sequence number, JSON payload) messages.

The unit sends a ``*HELLO*`` banner on connect and then frames every status as ``N``,
a six digit sequence number and a JSON array. Nothing separates frames, TCP splits
them anywhere, and a flaky link can drop the tail of one. This parser is pure: feed
it bytes, get back complete messages, and it never spins, raises on encoding, or
grows without bound.
"""

from __future__ import annotations

import logging
import re
from typing import List, Tuple

_LOGGER = logging.getLogger(__name__)

MESSAGE_HEADER = re.compile(rb"N(\d{6})\[")
_HEADER_PREFIX = re.compile(rb"N\d{0,6}")
HELLO = b"*HELLO*"
_WHITESPACE = frozenset(b" \r\n\t")
# Shortest buffer worth parsing: a full hello, or a header minus its opening bracket.
_MIN_PARSE_LENGTH = 7
# A single status is a couple of kilobytes. Anything larger with no complete message in it
# is not going to parse, no matter how long we wait.
MAX_BUFFER_BYTES = 64 * 1024


class FrameOverflow(Exception):
    """More than MAX_BUFFER_BYTES arrived without a complete message; buffer discarded."""


def _scan_json(buffer: bytes, start: int) -> Tuple[int, int]:
    """Scan the JSON value that opens at ``start``.

    Returns ``(end, next_header)``: ``end`` is the index of the bracket that closes the
    value, or -1 while it is incomplete; ``next_header`` is the index of the first
    message header found *outside a string* after ``start``, or -1. A header outside a
    string can never be part of valid JSON, so it proves the current message was cut
    short and the unit has moved on to the next one.
    """
    depth = 0
    in_string = False
    escaped = False
    next_header = -1
    for index in range(start, len(buffer)):
        byte = buffer[index]
        if in_string:
            if escaped:
                escaped = False
            elif byte == 0x5C:  # backslash
                escaped = True
            elif byte == 0x22:  # double quote
                in_string = False
        elif byte == 0x22:
            in_string = True
        elif byte in (0x5B, 0x7B):  # [ or {
            depth += 1
        elif byte in (0x5D, 0x7D):  # ] or }
            depth -= 1
            if depth <= 0:
                return index, -1
        elif (
            byte == 0x4E  # N
            and next_header < 0
            and index > start
            and MESSAGE_HEADER.match(buffer, index)
        ):
            next_header = index
    return -1, next_header


def _skip_whitespace(buffer: bytearray) -> None:
    """Drop any leading whitespace in one slice, so a noisy peer is O(n) not O(n²)."""
    skip = 0
    while skip < len(buffer) and buffer[skip] in _WHITESPACE:
        skip += 1
    if skip:
        del buffer[:skip]


class FrameParser:
    """Accumulate bytes from the unit and hand back complete messages."""

    def __init__(self) -> None:
        self._buffer = bytearray()
        self.hellos = 0

    def __len__(self) -> int:
        return len(self._buffer)

    def reset(self) -> None:
        """Forget everything; call on a new connection."""
        self._buffer.clear()
        self.hellos = 0

    def feed(self, data: bytes) -> List[Tuple[int, bytes]]:
        """Append ``data`` and return every complete ``(sequence, payload)`` in order.

        Every pass either consumes bytes or waits for more, so it cannot spin on the
        same data. Raises FrameOverflow (after clearing the buffer) when more than
        MAX_BUFFER_BYTES have accumulated without a complete message.
        """
        buffer = self._buffer
        buffer.extend(data)
        messages: List[Tuple[int, bytes]] = []
        while True:
            _skip_whitespace(buffer)
            if len(buffer) < _MIN_PARSE_LENGTH:
                break

            if buffer.startswith(HELLO):
                self.hellos += 1
                if self.hellos > 1:
                    _LOGGER.warning(
                        "Hello message received more than once. Has the unit reset?"
                    )
                else:
                    _LOGGER.info("Hello message successfully received from unit")
                del buffer[: len(HELLO)]
                continue

            header = MESSAGE_HEADER.match(buffer)
            if header is None:
                if _HEADER_PREFIX.fullmatch(buffer):
                    # e.g. b"N000012" with the opening bracket still in flight.
                    break
                if not self._resync():
                    break
                continue

            json_start = header.end() - 1
            json_end, next_header = _scan_json(buffer, json_start)
            if json_end < 0:
                # Message is incomplete (partial TCP packet). If the unit has already
                # started a new message, the tail of this one is never coming.
                if next_header > 0:
                    _LOGGER.warning(
                        "Discarding %d bytes of truncated message", next_header
                    )
                    del buffer[:next_header]
                    continue
                if len(buffer) > MAX_BUFFER_BYTES:
                    size = len(buffer)
                    buffer.clear()
                    raise FrameOverflow(
                        f"No complete message in {size} bytes of data"
                    )
                break

            messages.append((int(header.group(1)), bytes(buffer[json_start : json_end + 1])))
            del buffer[: json_end + 1]
        return messages

    def _resync(self) -> bool:
        """Drop unparseable bytes from the front of the buffer.

        Returns True when a hello or message header was found further along and the
        buffer now starts with it, False when there is nothing left worth parsing yet.
        """
        buffer = self._buffer
        candidates = [buffer.find(HELLO, 1)]
        if (header := MESSAGE_HEADER.search(buffer, 1)) is not None:
            candidates.append(header.start())
        candidates = [index for index in candidates if index > 0]

        if candidates:
            cut = min(candidates)
            _LOGGER.warning("Discarding %d unparseable bytes to resynchronise", cut)
            _LOGGER.debug("Discarded %r", bytes(buffer[:cut]))
            del buffer[:cut]
            return True

        # Keep a tail that could still turn out to be the start of a header or hello
        # once the next packet arrives.
        discard = len(buffer) - _MIN_PARSE_LENGTH
        if discard > 0:
            _LOGGER.warning("Discarding %d unparseable bytes", discard)
            _LOGGER.debug("Discarded %r", bytes(buffer[:discard]))
            del buffer[:discard]
        return False
