"""Tests for RinnaiSystem lifecycle and the Event helper."""

import json
import time
from queue import SimpleQueue

import pytest

from pyrinnaitouch.event import Event
from pyrinnaitouch.pollconnection import RinnaiPollConnection
from pyrinnaitouch.system import RinnaiSystem
from tests.test_pollconnection import STATUS


@pytest.fixture(autouse=True)
def _clean_registry():
    RinnaiPollConnection.clients.clear()
    RinnaiSystem.instances.clear()
    yield
    for ip_address in list(RinnaiSystem.instances):
        RinnaiSystem.remove_instance(ip_address)
    RinnaiPollConnection.clients.clear()


def test_event_keeps_calling_handlers_after_one_raises():
    event = Event()
    seen = []
    event += lambda: (_ for _ in ()).throw(RuntimeError("first handler broke"))
    event += lambda: seen.append("second")
    event()  # must not raise
    assert seen == ["second"]


def test_event_unsubscribe_unknown_handler_is_harmless():
    event = Event()
    event -= print


def test_shutdown_accepts_home_assistant_event_argument():
    system = RinnaiSystem.get_instance("10.0.0.3")
    system.shutdown(object())  # hass.bus.async_listen_once passes an Event
    system.shutdown()  # and a second call is harmless


def test_remove_instance_drops_instance_even_if_shutdown_fails(monkeypatch):
    system = RinnaiSystem.get_instance("10.0.0.4")

    def broken():
        raise RuntimeError("thread refused to stop")

    monkeypatch.setattr(system._connection, "stop_thread", broken)  # pylint: disable=protected-access
    RinnaiSystem.remove_instance("10.0.0.4")
    assert "10.0.0.4" not in RinnaiSystem.instances


def test_poll_loop_survives_bad_payload_and_raising_subscriber():
    system = RinnaiSystem.get_instance("10.0.0.5")
    updates = []
    system.subscribe_updates(lambda: (_ for _ in ()).throw(RuntimeError("entity broke")))
    system.subscribe_updates(lambda: updates.append(1))
    queue: SimpleQueue = system._receiverqueue  # pylint: disable=protected-access
    queue.put(42)  # not a list/dict: would raise TypeError on the membership test
    queue.put([{"WAT": {}}])  # unknown mode: handle_status returns False
    queue.put(json.loads(STATUS))
    deadline = time.monotonic() + 3
    while not updates and time.monotonic() < deadline:
        time.sleep(0.02)
    assert updates == [1]
    assert system.get_stored_status().mode.name == "COOLING"
