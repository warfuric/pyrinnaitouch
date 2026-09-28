"""RinnaiSystem lifecycle against the simulator."""

import asyncio
import logging
import socket
import time

import pytest

from pyrinnaitouch import RinnaiSystem, RinnaiSystemMode
from pyrinnaitouch.connection import RinnaiConnection
from pyrinnaitouch.event import Event
from pyrinnaitouch.simulator import FakeRinnaiUnit

FAST = dict(udp_timeout=0.2, udp_fallback_attempts=2, connect_timeout=1.0,
            retry_delay=0.2, keepalive_interval=0.3, ack_timeout=0.5, watchdog_timeout=1.0)


def free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
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
async def _clean_registry():
    RinnaiConnection._active.clear()  # pylint: disable=protected-access
    RinnaiSystem.instances.clear()
    yield
    for ip_address in list(RinnaiSystem.instances):
        await RinnaiSystem.async_remove_instance(ip_address)
    RinnaiConnection._active.clear()  # pylint: disable=protected-access


@pytest.fixture(name="unit")
async def fixture_unit():
    unit = await FakeRinnaiUnit(udp_port=free_udp_port(), status_interval=0.1,
                                broadcast_interval=0.1).start()
    yield unit
    await unit.stop()


def test_event_keeps_calling_handlers_after_one_raises():
    event = Event()
    seen = []
    event += lambda: (_ for _ in ()).throw(RuntimeError("first handler broke"))
    event += lambda: seen.append("second")
    event()
    assert seen == ["second"]


def test_event_unsubscribe_unknown_handler_is_harmless():
    event = Event()
    event -= print


async def test_status_reaches_subscribers_and_ready_wait(unit):
    system = RinnaiSystem.get_instance(unit.host, tcp_port=unit.tcp_port, udp_port=unit.udp_port, **FAST)
    updates = []
    system.subscribe_updates(lambda: updates.append(system.get_stored_status().mode))
    await system.async_start()
    assert await system.async_wait_ready(3)
    assert system.is_connected
    assert system.get_stored_status().mode is RinnaiSystemMode.HEATING
    assert updates and updates[-1] is RinnaiSystemMode.HEATING
    assert await system.async_wait_ready(0)  # already ready


async def test_status_is_reset_while_the_link_is_down(unit):
    system = RinnaiSystem.get_instance(unit.host, tcp_port=unit.tcp_port, udp_port=unit.udp_port, **FAST)
    modes = []
    system.subscribe_updates(lambda: modes.append(system.get_stored_status().mode))
    await system.async_start()
    assert await system.async_wait_ready(3)
    await unit.disconnect_clients()
    await wait_for(lambda: RinnaiSystemMode.NONE in modes, what="reset on disconnect")
    assert not system.is_connected or system.get_stored_status().mode is RinnaiSystemMode.NONE
    await wait_for(lambda: modes[-1] is RinnaiSystemMode.HEATING and system.is_connected, what="recovery")


async def test_commands_are_validated_against_mode_and_reach_the_unit(unit):
    system = RinnaiSystem.get_instance(unit.host, tcp_port=unit.tcp_port, udp_port=unit.udp_port, **FAST)
    await system.async_start()
    assert await system.async_wait_ready(3)
    assert await system.set_unit_temp(21)  # heater mode: allowed
    assert not await system.turn_evap_pump_on()  # evap-only command in heater mode: rejected
    await wait_for(lambda: any('"SP": "21"' in payload for _, payload in unit.commands), what="command delivered")


async def test_subscriber_exception_and_bad_payload_are_isolated(unit, caplog):
    caplog.set_level(logging.ERROR)
    system = RinnaiSystem.get_instance(unit.host, tcp_port=unit.tcp_port, udp_port=unit.udp_port, **FAST)
    seen = []
    system.subscribe_updates(lambda: (_ for _ in ()).throw(RuntimeError("entity broke")))
    system.subscribe_updates(lambda: seen.append(1))
    system._handle_status_message(42)  # pylint: disable=protected-access
    system._handle_status_message([{"WAT": {}}])  # pylint: disable=protected-access
    await system.async_start()
    assert await system.async_wait_ready(3)
    await wait_for(lambda: len(seen) >= 2)
    assert "entity broke" in caplog.text


async def test_remove_instance_stops_and_forgets(unit):
    system = RinnaiSystem.get_instance(unit.host, tcp_port=unit.tcp_port, udp_port=unit.udp_port, **FAST)
    await system.async_start()
    assert await system.async_wait_ready(3)
    await RinnaiSystem.async_remove_instance(unit.host)
    assert unit.host not in RinnaiSystem.instances
    assert not system.is_connected
    await system.async_stop()  # idempotent
    await RinnaiSystem.async_remove_instance(unit.host)  # unknown: logged, not raised


async def test_wait_ready_without_start_is_false():
    system = RinnaiSystem("127.0.0.1")
    assert not await system.async_wait_ready(0.1)
