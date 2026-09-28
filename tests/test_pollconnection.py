"""Tests for the message framing and lifecycle of RinnaiPollConnection.

These exercise ``_process_received_data`` with an injected buffer (no sockets), the
``clients`` connection counter, and the stop/start behaviour of the socket thread.
"""

import json
import logging
import socket
import time
from queue import Empty, SimpleQueue

import pytest

from pyrinnaitouch.pollconnection import RinnaiConnectionState, RinnaiPollConnection

from tests.test_system_parse import get_test_json

STATUS = get_test_json()


def message(seq: int, payload: str = STATUS) -> bytes:
    """Frame a status payload the way the unit does: N + 6-digit sequence + JSON."""
    return f"N{seq:06d}{payload}".encode()


def drain(queue: SimpleQueue) -> list:
    """Return everything currently on the queue."""
    items = []
    while True:
        try:
            items.append(queue.get_nowait())
        except Empty:
            return items


@pytest.fixture(name="conn")
def fixture_conn():
    """A connection object with no thread started, cleaned up afterwards."""
    RinnaiPollConnection.clients.clear()
    queue = SimpleQueue()
    conn = RinnaiPollConnection("10.0.0.1", queue)
    yield conn, queue
    conn.stop_thread()
    RinnaiPollConnection.clients.clear()


def feed(conn: RinnaiPollConnection, data: bytes) -> None:
    """Append bytes to the read buffer and run the parser, as the socket thread does."""
    conn._readbuffer.extend(data)  # pylint: disable=protected-access
    conn._process_received_data()  # pylint: disable=protected-access


# --- framing -----------------------------------------------------------------


def test_complete_message_is_parsed(conn):
    conn, queue = conn
    feed(conn, message(12))
    assert drain(queue) == [json.loads(STATUS)]
    assert conn._readbuffer == b""  # pylint: disable=protected-access
    assert conn._last_received_sequence_num == 12  # pylint: disable=protected-access


def test_message_split_across_packets_is_reassembled(conn):
    conn, queue = conn
    data = message(13)
    for cut in (3, 7, 8, 40, len(data) - 1):
        feed(conn, data[:cut])
        assert drain(queue) == [], f"emitted early at cut {cut}"
        assert conn._readbuffer == data[:cut]  # pylint: disable=protected-access
        conn._readbuffer.clear()  # pylint: disable=protected-access
    feed(conn, data[:50])
    feed(conn, data[50:])
    assert drain(queue) == [json.loads(STATUS)]
    assert conn._readbuffer == b""  # pylint: disable=protected-access


def test_two_messages_in_one_packet(conn):
    conn, queue = conn
    feed(conn, message(1) + message(2))
    assert len(drain(queue)) == 2
    assert conn._last_received_sequence_num == 2  # pylint: disable=protected-access


def test_hello_is_consumed_every_time(conn):
    conn, queue = conn
    feed(conn, b"*HELLO*" + message(1))
    assert len(drain(queue)) == 1
    feed(conn, b"*HELLO*" + message(2))  # unit reset: second hello must not wedge
    assert len(drain(queue)) == 1
    assert conn._readbuffer == b""  # pylint: disable=protected-access


def test_nested_array_in_payload_is_not_truncated(conn):
    conn, queue = conn
    payload = '[{"SYST": {"X": [1, 2, {"Y": ["]", "["]}]}}]'
    feed(conn, message(5, payload))
    assert drain(queue) == [json.loads(payload)]
    assert conn._readbuffer == b""  # pylint: disable=protected-access


def test_split_multibyte_utf8_does_not_raise(conn):
    conn, queue = conn
    payload = '[{"SYST": {"CFG": {"ZA": "café"}}}]'
    data = message(6, payload)
    cut = data.index("é".encode()) + 1  # split inside the two-byte character
    feed(conn, data[:cut])
    assert drain(queue) == []
    feed(conn, data[cut:])
    assert drain(queue) == [json.loads(payload)]


def test_invalid_json_is_skipped_not_looped(conn):
    conn, queue = conn
    feed(conn, message(7, "[{bad}]") + message(8))
    assert drain(queue) == [json.loads(STATUS)]
    assert conn._readbuffer == b""  # pylint: disable=protected-access


def test_leading_garbage_resyncs_to_next_message(conn):
    conn, queue = conn
    feed(conn, b"\x00\xff junk" + message(9))
    assert drain(queue) == [json.loads(STATUS)]
    assert conn._readbuffer == b""  # pylint: disable=protected-access
    assert conn.socket_state() is RinnaiConnectionState.IDLE  # no reconnect forced


def test_marker_followed_by_non_digits_does_not_wedge(conn):
    conn, queue = conn
    feed(conn, b"Nxxxxxx[{}]" + message(10))
    assert drain(queue) == [json.loads(STATUS)]
    assert conn._readbuffer == b""  # pylint: disable=protected-access


def test_truncated_message_is_abandoned_when_next_header_arrives(conn):
    conn, queue = conn
    truncated = message(11)[:-20]  # unit dropped the tail of a status
    feed(conn, truncated + message(12))
    assert drain(queue) == [json.loads(STATUS)]
    assert conn._last_received_sequence_num == 12  # pylint: disable=protected-access
    assert conn._readbuffer == b""  # pylint: disable=protected-access


def test_oversized_incomplete_message_resets_connection(conn):
    conn, queue = conn
    feed(conn, b"N000001[")  # a header whose JSON never closes...
    for chunks in range(1, 100):
        feed(conn, b"x" * 1000)  # ...followed by nothing useful
        if conn.socket_state() is RinnaiConnectionState.ERROR:
            break
    assert conn.socket_state() is RinnaiConnectionState.ERROR
    assert 64 < chunks < 70  # tripped just past the 64 KiB cap, not before
    assert conn._readbuffer == b""  # pylint: disable=protected-access
    assert drain(queue) == []


def test_header_inside_json_string_is_not_a_resync_point(conn):
    conn, queue = conn
    payload = '[{"SYST": {"CFG": {"ZA": "N000002[room"}}}]'
    data = message(1, payload)
    cut = data.index(b"[room") + 3  # split inside the string, right after the fake header
    feed(conn, data[:cut])
    assert drain(queue) == []
    feed(conn, data[cut:] + message(2))
    assert drain(queue) == [json.loads(payload), json.loads(STATUS)]
    assert conn._readbuffer == b""  # pylint: disable=protected-access


def test_whitespace_between_frames_is_ignored(conn, caplog):
    conn, queue = conn
    caplog.set_level(logging.WARNING)
    feed(conn, b"*HELLO*\r\n" + message(1) + b"\r\n" + message(2) + b"\n")
    assert len(drain(queue)) == 2
    assert conn._readbuffer == b""  # pylint: disable=protected-access
    assert not caplog.records


def test_watchdog_counts_parsed_frames_not_raw_bytes(conn):
    conn, queue = conn
    conn._last_received_time = 0.0  # pylint: disable=protected-access
    feed(conn, b"\x00" * 20)  # noise must not keep the connection alive
    assert conn._last_received_time == 0.0  # pylint: disable=protected-access
    feed(conn, message(3))
    assert conn._last_received_time > 0.0  # pylint: disable=protected-access
    drain(queue)


def test_command_wait_is_cleared_only_by_the_echoed_sequence(conn):
    conn, _ = conn
    conn._command_wait = True  # pylint: disable=protected-access
    conn._command_sequence = 20  # pylint: disable=protected-access
    feed(conn, message(19))
    assert conn._command_wait is True  # pylint: disable=protected-access
    feed(conn, message(21))  # a higher number is not an acknowledgement either
    assert conn._command_wait is True  # pylint: disable=protected-access
    feed(conn, message(20))
    assert conn._command_wait is False  # pylint: disable=protected-access


class _FakeSocket:
    """Accepts every byte; enough for framing tests."""

    def __init__(self):
        self.sent = bytearray()

    def send(self, data):
        self.sent.extend(data)
        return len(data)

    def shutdown(self, _how):
        pass

    def close(self):
        pass


def test_sequence_wraps_from_255_to_0(conn):
    conn, _ = conn
    conn._socket = _FakeSocket()  # pylint: disable=protected-access
    conn._command_sequence = 254  # pylint: disable=protected-access
    conn._last_received_sequence_num = 254  # pylint: disable=protected-access
    conn._queue_frame(b"NA", "idle command")  # pylint: disable=protected-access
    assert conn._command_sequence == 255  # pylint: disable=protected-access
    feed(conn, message(255))
    assert conn._command_wait is False  # pylint: disable=protected-access
    conn._queue_frame(b"NA", "idle command")  # pylint: disable=protected-access
    assert conn._command_sequence == 0  # pylint: disable=protected-access
    assert conn._socket.sent.endswith(b"N000000NA")  # pylint: disable=protected-access
    feed(conn, message(255))  # still echoing the previous command
    assert conn._command_wait is True  # pylint: disable=protected-access
    feed(conn, message(0))
    assert conn._command_wait is False  # pylint: disable=protected-access


# --- lifecycle ---------------------------------------------------------------


def test_stop_thread_is_idempotent(conn):
    conn, queue = conn
    conn.stop_thread()
    conn.stop_thread()
    assert RinnaiPollConnection.clients["10.0.0.1"] == 0
    assert drain(queue) == ["sys.exit"]


def test_duplicate_connection_does_not_leak_counter():
    RinnaiPollConnection.clients.clear()
    first = RinnaiPollConnection("10.0.0.2", SimpleQueue())
    with pytest.raises(RuntimeError):
        RinnaiPollConnection("10.0.0.2", SimpleQueue())
    assert RinnaiPollConnection.clients["10.0.0.2"] == 1
    first.stop_thread()
    assert RinnaiPollConnection.clients["10.0.0.2"] == 0
    third = RinnaiPollConnection("10.0.0.2", SimpleQueue())
    third.stop_thread()
    RinnaiPollConnection.clients.clear()


def test_send_command_before_start_does_not_raise(conn):
    conn, _ = conn
    conn.send_command('{"SYST": {"OSS": {"MD": "H"}}}')


def test_failed_construction_releases_the_client_slot(monkeypatch):
    RinnaiPollConnection.clients.clear()

    def no_fds():
        raise OSError("Too many open files")

    monkeypatch.setattr(socket, "socketpair", no_fds)
    with pytest.raises(OSError):
        RinnaiPollConnection("10.0.0.6", SimpleQueue())
    assert RinnaiPollConnection.clients["10.0.0.6"] == 0
    monkeypatch.undo()
    conn = RinnaiPollConnection("10.0.0.6", SimpleQueue())  # slot is free again
    conn.stop_thread()
    RinnaiPollConnection.clients.clear()


def test_state_handler_exception_does_not_reach_the_caller(conn, caplog):
    conn, _ = conn
    caplog.set_level(logging.ERROR)
    conn.register_socket_state_handler(lambda state: 1 / 0)  # raises immediately
    assert "Socket state handler" in caplog.text
    conn._update_socket_state(RinnaiConnectionState.CONNECTING)  # pylint: disable=protected-access


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _closed_tcp_port() -> int:
    """A port nothing listens on, so a connect is refused rather than answered."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_thread_is_daemon_and_stops_promptly_while_connecting():
    """Stopping must work while the thread is blocked in discovery/connect sleeps."""
    RinnaiPollConnection.clients.clear()
    queue = SimpleQueue()
    conn = RinnaiPollConnection("127.0.0.1", queue, udp_port=_free_udp_port())
    conn.start_thread()
    thread = conn._socketthread  # pylint: disable=protected-access
    assert thread.daemon
    time.sleep(0.5)
    started = time.monotonic()
    conn.stop_thread()  # must not raise even though the thread is mid-wait
    assert not thread.is_alive(), "thread still alive after stop_thread"
    assert time.monotonic() - started < 2  # the wake socket interrupts discovery
    assert RinnaiPollConnection.clients["127.0.0.1"] == 0
    assert "sys.exit" in drain(queue)
    RinnaiPollConnection.clients.clear()


def test_stop_thread_from_stale_state_after_connect_refused():
    """A refused TCP connect (no unit listening) must keep retrying, then stop cleanly."""
    RinnaiPollConnection.clients.clear()
    conn = RinnaiPollConnection(
        "127.0.0.1", SimpleQueue(), udp_port=_free_udp_port(), tcp_port=_closed_tcp_port()
    )
    conn._socketstate = RinnaiConnectionState.TIMEOUT  # skip discovery, go straight to TCP  # pylint: disable=protected-access
    conn.start_thread()
    deadline = time.monotonic() + 5
    while conn.socket_state() is not RinnaiConnectionState.REFUSED and time.monotonic() < deadline:
        time.sleep(0.05)
    assert conn.socket_state() is RinnaiConnectionState.REFUSED
    started = time.monotonic()
    conn.stop_thread()
    assert time.monotonic() - started < 8
    assert conn._socketthread is None or not conn._socketthread.is_alive()  # pylint: disable=protected-access
    RinnaiPollConnection.clients.clear()


def test_parser_exception_does_not_kill_thread(monkeypatch):
    """Any unexpected exception in the monitor loop must reconnect, not end the thread."""
    RinnaiPollConnection.clients.clear()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    queue = SimpleQueue()
    conn = RinnaiPollConnection("127.0.0.1", queue, udp_port=_free_udp_port(), tcp_port=port)
    conn._socketstate = RinnaiConnectionState.TIMEOUT  # pylint: disable=protected-access
    calls = []

    def boom():
        calls.append(1)
        raise ValueError("synthetic parser failure")

    monkeypatch.setattr(conn, "_process_received_data", boom)
    conn.start_thread()
    listener.settimeout(5)
    server_side, _ = listener.accept()
    server_side.sendall(b"*HELLO*")
    deadline = time.monotonic() + 5
    while not calls and time.monotonic() < deadline:
        time.sleep(0.05)
    assert calls, "parser was never invoked"
    time.sleep(0.5)
    assert conn._socketthread.is_alive()  # pylint: disable=protected-access
    conn.stop_thread()
    server_side.close()
    listener.close()
    RinnaiPollConnection.clients.clear()
