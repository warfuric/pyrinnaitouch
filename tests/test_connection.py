"""Connection behaviour against the in-process simulator."""

import asyncio
import logging
import socket
import time

import pytest

from pyrinnaitouch.connection import RinnaiConnection, RinnaiConnectionState
from pyrinnaitouch.simulator import FakeRinnaiUnit

FAST = dict(
    udp_timeout=0.2,
    udp_fallback_attempts=2,
    connect_timeout=1.0,
    retry_delay=0.2,
    keepalive_interval=0.3,
    ack_timeout=0.5,
    watchdog_timeout=1.0,
)


def free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def closed_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def wait_for(predicate, timeout=5.0, what="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"{what} not met within {timeout}s")


@pytest.fixture(autouse=True)
def _clean_registry():
    RinnaiConnection._active.clear()  # pylint: disable=protected-access
    yield
    RinnaiConnection._active.clear()  # pylint: disable=protected-access


@pytest.fixture(name="unit")
async def fixture_unit():
    unit = await FakeRinnaiUnit(udp_port=free_udp_port(), status_interval=0.1,
                                broadcast_interval=0.1).start()
    yield unit
    await unit.stop()


_OPEN_CONNECTIONS = []


@pytest.fixture(autouse=True)
async def _stop_leftover_connections():
    """A failed assertion must not leave a connection running into the next test."""
    yield
    for conn in _OPEN_CONNECTIONS:
        await conn.stop()
    _OPEN_CONNECTIONS.clear()


class Client:
    """A connection wired to a simulator, collecting statuses and states."""

    def __init__(self, unit: FakeRinnaiUnit, **overrides):
        self.statuses = []
        self.states = []
        options = dict(FAST, tcp_port=unit.tcp_port, udp_port=unit.udp_port)
        options.update(overrides)
        self.conn = RinnaiConnection(unit.host, self.statuses.append, **options)
        self.conn.register_state_handler(self.states.append)
        _OPEN_CONNECTIONS.append(self.conn)


async def test_connects_via_broadcast_and_delivers_status(unit):
    client = Client(unit)
    await client.conn.start()
    assert await client.conn.wait_connected(3)
    await wait_for(lambda: len(client.statuses) >= 2, what="two statuses")
    assert client.statuses[0] == unit.status
    assert client.states[:3] == [
        RinnaiConnectionState.IDLE,
        RinnaiConnectionState.CONNECTING,
        RinnaiConnectionState.CONNECTED,
    ]
    assert unit.connections == 1
    await client.conn.stop()
    assert client.conn.state is RinnaiConnectionState.IDLE
    assert not client.conn.is_running


async def test_falls_back_to_direct_connect_without_broadcast():
    unit = await FakeRinnaiUnit(status_interval=0.1).start()  # no UDP at all
    try:
        client = Client(unit, udp_port=free_udp_port())
        started = time.monotonic()
        await client.conn.start()
        assert await client.conn.wait_connected(3)
        assert 0.3 < time.monotonic() - started < 2  # two 0.2 s timeouts, then TCP
        await client.conn.stop()
    finally:
        await unit.stop()


async def test_commands_are_sent_in_order_and_paced_by_echo(unit):
    client = Client(unit)
    await client.conn.start()
    assert await client.conn.wait_connected(3)
    for command in ('{"a": 1}', '{"b": 2}', '{"c": 3}'):
        client.conn.send_command(command)
    await wait_for(lambda: len([c for c in unit.commands if c[1] != "NA"]) == 3)
    sent = [c for c in unit.commands if c[1] != "NA"]
    assert [json_payload for _, json_payload in sent] == ['{"a": 1}', '{"b": 2}', '{"c": 3}']
    sequences = [seq for seq, _ in sent]
    assert sequences == sorted(sequences) and len(set(sequences)) == 3
    await client.conn.stop()


async def test_unacknowledged_commands_wait_for_the_ack_timeout(unit):
    unit.echo = False
    client = Client(unit, ack_timeout=0.3)
    await client.conn.start()
    assert await client.conn.wait_connected(3)
    started = time.monotonic()
    for command in ('{"a": 1}', '{"b": 2}', '{"c": 3}'):
        client.conn.send_command(command)
    await wait_for(lambda: len([c for c in unit.commands if c[1] != "NA"]) == 3)
    assert time.monotonic() - started >= 0.55  # each waited out its 0.3 s ack timeout
    await client.conn.stop()


async def test_queued_commands_are_never_starved_by_keepalives(unit):
    """Regression: with keepalive shorter than the ack timeout the old loop only sent NA."""
    unit.echo = False
    client = Client(unit, keepalive_interval=0.1, ack_timeout=0.3)
    await client.conn.start()
    assert await client.conn.wait_connected(3)
    await asyncio.sleep(0.5)  # let a few keep-alives go out first
    for command in ('{"a": 1}', '{"b": 2}', '{"c": 3}'):
        client.conn.send_command(command)
    await wait_for(lambda: len([c for c in unit.commands if c[1] != "NA"]) == 3, timeout=3)
    await client.conn.stop()


async def test_keepalive_is_sent_when_idle(unit):
    client = Client(unit, keepalive_interval=0.2)
    await client.conn.start()
    assert await client.conn.wait_connected(3)
    await wait_for(lambda: len([c for c in unit.commands if c[1] == "NA"]) >= 2)
    await client.conn.stop()


async def test_reconnects_after_unit_closes_connection(unit):
    client = Client(unit)
    await client.conn.start()
    assert await client.conn.wait_connected(3)
    await unit.disconnect_clients()
    await wait_for(lambda: unit.connections == 2 and client.conn.is_connected, what="reconnect")
    assert RinnaiConnectionState.IDLE in client.states[3:]
    await client.conn.stop()


async def test_watchdog_reconnects_when_status_stops(unit):
    client = Client(unit, watchdog_timeout=0.5)
    await client.conn.start()
    assert await client.conn.wait_connected(3)
    unit.send_status = False
    await wait_for(lambda: RinnaiConnectionState.TIMEOUT in client.states, what="watchdog")
    unit.send_status = True
    await wait_for(lambda: unit.connections == 2 and client.conn.is_connected, what="reconnect")
    await client.conn.stop()


async def test_overflow_resets_connection(unit):
    client = Client(unit)
    await client.conn.start()
    assert await client.conn.wait_connected(3)
    unit.send_raw(b"N000001[" + b"x" * 70000)
    await wait_for(lambda: RinnaiConnectionState.ERROR in client.states, what="overflow")
    await wait_for(lambda: unit.connections == 2 and client.conn.is_connected, what="reconnect")
    await client.conn.stop()


async def test_stop_is_immediate_during_discovery():
    conn = RinnaiConnection("127.0.0.1", lambda s: None, tcp_port=closed_tcp_port(),
                            udp_port=free_udp_port(), udp_timeout=10, udp_fallback_attempts=6)
    await conn.start()
    await asyncio.sleep(0.2)
    started = time.monotonic()
    await conn.stop()
    assert time.monotonic() - started < 0.5
    assert conn.state is RinnaiConnectionState.IDLE


async def test_stop_is_immediate_during_refused_backoff():
    states = []
    conn = RinnaiConnection("127.0.0.1", lambda s: None, tcp_port=closed_tcp_port(),
                            udp_port=free_udp_port(), udp_timeout=0.1,
                            udp_fallback_attempts=1, retry_delay=10)
    conn.register_state_handler(states.append)
    await conn.start()
    await wait_for(lambda: RinnaiConnectionState.REFUSED in states, what="refused")
    started = time.monotonic()
    await conn.stop()
    assert time.monotonic() - started < 0.5


async def test_stop_is_immediate_while_connected(unit):
    client = Client(unit)
    await client.conn.start()
    assert await client.conn.wait_connected(3)
    started = time.monotonic()
    await client.conn.stop()
    assert time.monotonic() - started < 0.5
    await wait_for(lambda: not unit._clients, what="unit saw the disconnect")  # pylint: disable=protected-access


async def test_second_connection_to_same_unit_is_refused(unit):
    first = Client(unit)
    second = Client(unit)
    await first.conn.start()
    with pytest.raises(RuntimeError):
        await second.conn.start()
    await first.conn.stop()
    await second.conn.start()  # free again
    assert await second.conn.wait_connected(3)
    await second.conn.stop()


async def test_send_before_start_is_dropped_with_an_error(caplog):
    caplog.set_level(logging.ERROR)
    conn = RinnaiConnection("127.0.0.1", lambda s: None)
    conn.send_command("{}")
    assert "not started" in caplog.text


async def test_wait_connected_times_out_when_nothing_answers():
    conn = RinnaiConnection("127.0.0.1", lambda s: None, tcp_port=closed_tcp_port(),
                            udp_port=free_udp_port(), udp_timeout=0.1,
                            udp_fallback_attempts=1, retry_delay=0.1)
    await conn.start()
    assert not await conn.wait_connected(0.5)
    await conn.stop()


async def test_status_handler_exception_is_isolated(unit, caplog):
    caplog.set_level(logging.ERROR)
    calls = []

    def broken(status):
        calls.append(status)
        raise RuntimeError("consumer broke")

    conn = RinnaiConnection(unit.host, broken, tcp_port=unit.tcp_port, udp_port=unit.udp_port, **FAST)
    await conn.start()
    assert await conn.wait_connected(3)
    await wait_for(lambda: len(calls) >= 2)  # still delivering after the first failure
    assert "Status handler failed" in caplog.text
    await conn.stop()


async def test_sender_failure_ends_the_session_and_reconnects(unit):
    """A dead sender must not leave a session that looks connected but cannot send."""
    client = Client(unit)
    await client.conn.start()
    assert await client.conn.wait_connected(3)

    def broken_write(_data):
        raise BrokenPipeError("half-closed link")

    client.conn._writer.write = broken_write  # pylint: disable=protected-access
    client.conn.send_command('{"a": 1}')
    await wait_for(lambda: unit.connections == 2 and client.conn.is_connected, what="reconnect")
    client.conn.send_command('{"b": 2}')
    await wait_for(lambda: any(payload == '{"b": 2}' for _, payload in unit.commands))
    await client.conn.stop()


async def test_backoff_applies_after_a_watchdog_timeout(unit):
    client = Client(unit, watchdog_timeout=0.3, retry_delay=0.6)
    await client.conn.start()
    assert await client.conn.wait_connected(3)
    unit.send_status = False
    await wait_for(lambda: RinnaiConnectionState.TIMEOUT in client.states, what="watchdog")
    timed_out_at = time.monotonic()
    unit.send_status = True
    await wait_for(lambda: unit.connections == 2, what="reconnect")
    assert time.monotonic() - timed_out_at >= 0.5  # waited retry_delay, no tight loop
    await client.conn.stop()


async def test_keepalive_prevents_the_units_idle_drop():
    unit = await FakeRinnaiUnit(udp_port=free_udp_port(), status_interval=0.1,
                                broadcast_interval=0.1, idle_drop_seconds=0.5).start()
    try:
        client = Client(unit, keepalive_interval=0.15)
        await client.conn.start()
        assert await client.conn.wait_connected(3)
        await asyncio.sleep(1.5)
        assert unit.connections == 1 and client.conn.is_connected
        await client.conn.stop()

        silent = Client(unit, keepalive_interval=5)
        await silent.conn.start()
        assert await silent.conn.wait_connected(3)
        await wait_for(lambda: unit.connections >= 3, what="unit hung up on the silent client")
        await silent.conn.stop()
    finally:
        await unit.stop()
