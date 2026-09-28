"""Tests for the frame parser, fed byte by byte the way TCP delivers them."""

import json
import logging

import pytest

from pyrinnaitouch.framing import FrameOverflow, FrameParser
from tests.test_system_parse import get_test_json

STATUS = get_test_json()


def message(seq: int, payload: str = STATUS) -> bytes:
    """Frame a status payload the way the unit does: N + 6-digit sequence + JSON."""
    return f"N{seq:06d}{payload}".encode()


def decoded(frames):
    return [json.loads(payload) for _, payload in frames]


@pytest.fixture(name="parser")
def fixture_parser():
    return FrameParser()


def test_complete_message_is_parsed(parser):
    frames = parser.feed(message(12))
    assert frames == [(12, STATUS.encode())]
    assert len(parser) == 0


def test_message_split_at_every_offset(parser):
    data = b"*HELLO*" + message(1) + message(2)
    for cut in range(1, len(data)):
        parser.reset()
        frames = parser.feed(data[:cut]) + parser.feed(data[cut:])
        assert [seq for seq, _ in frames] == [1, 2], f"cut at {cut}"
        assert len(parser) == 0


def test_byte_at_a_time(parser):
    frames = []
    for byte in message(3):
        frames += parser.feed(bytes([byte]))
    assert decoded(frames) == [json.loads(STATUS)]


def test_hello_is_consumed_every_time(parser):
    assert len(parser.feed(b"*HELLO*" + message(1))) == 1
    assert len(parser.feed(b"*HELLO*" + message(2))) == 1  # unit reset mid-session
    assert parser.hellos == 2
    assert len(parser) == 0


def test_nested_array_and_brackets_in_strings_are_not_truncated(parser):
    payload = '[{"SYST": {"X": [1, 2, {"Y": ["]", "[", "\\"]"]}]}}]'
    assert decoded(parser.feed(message(5, payload))) == [json.loads(payload)]
    assert len(parser) == 0


def test_split_multibyte_utf8_does_not_raise(parser):
    payload = '[{"SYST": {"CFG": {"ZA": "café"}}}]'
    data = message(6, payload)
    cut = data.index("é".encode()) + 1  # split inside the two-byte character
    assert parser.feed(data[:cut]) == []
    assert decoded(parser.feed(data[cut:])) == [json.loads(payload)]


def test_invalid_json_is_returned_for_the_caller_to_reject(parser):
    frames = parser.feed(message(7, "[{bad}]") + message(8))
    assert [seq for seq, _ in frames] == [7, 8]
    with pytest.raises(ValueError):
        json.loads(frames[0][1])


def test_leading_garbage_resyncs_to_next_message(parser):
    assert decoded(parser.feed(b"\x00\xff junk" + message(9))) == [json.loads(STATUS)]
    assert len(parser) == 0


def test_marker_followed_by_non_digits_does_not_wedge(parser):
    assert decoded(parser.feed(b"Nxxxxxx[{}]" + message(10))) == [json.loads(STATUS)]
    assert len(parser) == 0


def test_truncated_message_is_abandoned_when_next_header_arrives(parser):
    truncated = message(11)[:-20]
    frames = parser.feed(truncated + message(12))
    assert [seq for seq, _ in frames] == [12]
    assert len(parser) == 0


def test_header_inside_json_string_is_not_a_resync_point(parser):
    payload = '[{"SYST": {"CFG": {"ZA": "N000002[room"}}}]'
    data = message(1, payload)
    cut = data.index(b"[room") + 3
    assert parser.feed(data[:cut]) == []
    frames = parser.feed(data[cut:] + message(2))
    assert decoded(frames) == [json.loads(payload), json.loads(STATUS)]


def test_garbage_without_marker_stays_bounded(parser):
    for _ in range(100):
        parser.feed(b"\x01\x02\x03\x04\x05\x06\x07\x08" * 100)
    assert len(parser) <= 7
    assert decoded(parser.feed(message(13))) == [json.loads(STATUS)]


def test_oversized_incomplete_message_raises_overflow_and_clears(parser):
    parser.feed(b"N000001[")
    with pytest.raises(FrameOverflow):
        for _ in range(100):
            parser.feed(b"x" * 1000)
    assert len(parser) == 0
    assert decoded(parser.feed(message(14))) == [json.loads(STATUS)]


def test_whitespace_between_frames_is_ignored(parser, caplog):
    caplog.set_level(logging.WARNING)
    frames = parser.feed(b"*HELLO*\r\n" + message(1) + b"\r\n" + message(2) + b"\n")
    assert [seq for seq, _ in frames] == [1, 2]
    assert len(parser) == 0
    assert not caplog.records


def test_sequence_keeps_all_six_digits(parser):
    frames = parser.feed(message(123456))
    assert frames[0][0] == 123456
