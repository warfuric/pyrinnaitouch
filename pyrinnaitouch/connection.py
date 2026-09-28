"""Asyncio connection to the Rinnai Touch wifi module.

One task owns the whole life of the link: wait for the unit's UDP broadcast, open the
TCP session, read frames, pace commands by the unit's echo of their sequence number,
send a keep-alive when idle, and reconnect on any failure. Everything is cancellable,
so ``stop()`` returns immediately whatever the task is doing, and every callback runs
on the event loop.

Protocol summary (NBW2 API): the module accepts one TCP client on port 27847 and
announces itself on UDP port 50000. On connect it sends ``*HELLO*``, then a status
about once a second framed as ``N`` + six-digit sequence number + JSON array. A
client sends ``N`` + six-digit sequence number + JSON command, or the same header
followed by ``NA`` as a keep-alive; the module echoes the last sequence number it
processed in its following status and drops a silent session after a few minutes.
"""

from __future__ import annotations

import asyncio
import enum
import json
import logging
import socket
import time
from typing import Any, Callable, Dict, List, Optional

from .framing import FrameOverflow, FrameParser

_LOGGER = logging.getLogger(__name__)

DEFAULT_TCP_PORT = 27847
DEFAULT_UDP_PORT = 50000
_BROADCAST_PREFIX = b"Rinnai_NBW2_Module"
# Sequence numbers are 0..255 (NBW2 API 4.1) and the unit echoes ours back.
_SEQUENCE_MODULUS = 256
# Cancellation is honoured at every await, so this only guards against a bug.
_STOP_TIMEOUT = 10
_CLOSE_TIMEOUT = 2

StatusHandler = Callable[[Any], None]
StateHandler = Callable[["RinnaiConnectionState"], None]


class RinnaiConnectionState(enum.Enum):
    """Possible connection states."""

    IDLE = 1
    CONNECTING = 2
    CONNECTED = 3
    REFUSED = 4
    TIMEOUT = 5
    ERROR = 6


async def _finish(task: "asyncio.Task", timeout: Optional[float] = None) -> None:
    """Wait for a subtask we no longer need, without raising its outcome.

    Awaiting a cancelled subtask directly raises its CancelledError, which cannot be
    told apart from our own cancellation arriving at the same moment; swallowing that
    would leave this task unstoppable. ``asyncio.wait`` reports the outcome instead.
    """
    await asyncio.wait({task}, timeout=timeout)
    if task.done() and not task.cancelled():
        exc = task.exception()
        if exc is not None:
            _LOGGER.debug("Subtask %s ended with %r", task.get_name(), exc)


class _BroadcastListener(asyncio.DatagramProtocol):
    """Resolve a future when the unit we want announces itself."""

    def __init__(self, ip_address: str) -> None:
        self._ip_address = ip_address
        self.found: Optional[asyncio.Future] = None
        self.foreign = 0

    def datagram_received(self, data: bytes, addr) -> None:
        if not data.startswith(_BROADCAST_PREFIX):
            return
        if addr[0] != self._ip_address:
            # Another unit on the network. Counted so discovery can't wait forever.
            self.foreign += 1
            return
        _LOGGER.debug("Broadcast received from %s: %s", addr[0], data.hex())
        if self.found is not None and not self.found.done():
            self.found.set_result(True)


class RinnaiConnection:  # pylint: disable=too-many-instance-attributes
    """Maintain the connection to one unit and deliver its status messages."""

    # Units in use, so two objects cannot fight over hardware that only takes one
    # client. Keyed by the configured address, holding the connection that started.
    _active: Dict[str, "RinnaiConnection"] = {}

    def __init__(  # pylint: disable=too-many-arguments
        self,
        ip_address: str,
        on_status: StatusHandler,
        *,
        tcp_port: Optional[int] = None,
        udp_port: Optional[int] = None,
        udp_timeout: float = 5.0,
        udp_fallback_attempts: int = 6,
        connect_timeout: float = 5.0,
        retry_delay: float = 5.0,
        keepalive_interval: float = 10.0,
        ack_timeout: float = 5.0,
        watchdog_timeout: float = 30.0,
    ) -> None:
        self._host = ip_address
        self._resolved_ip = ip_address  # replaced by the resolved address on start
        self._on_status = on_status
        self._tcp_port = tcp_port
        self._udp_port = udp_port
        self._udp_timeout = udp_timeout
        self._udp_fallback_attempts = udp_fallback_attempts
        self._connect_timeout = connect_timeout
        self._retry_delay = retry_delay
        self._keepalive_interval = keepalive_interval
        self._ack_timeout = ack_timeout
        self._watchdog_timeout = watchdog_timeout

        self._state = RinnaiConnectionState.IDLE
        self._state_handlers: List[StateHandler] = []
        self._connected_event: Optional[asyncio.Event] = None
        self._commands: Optional["asyncio.Queue[str]"] = None
        self._task: Optional[asyncio.Task] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._parser = FrameParser()

        self._command_sequence = 1
        self._last_received_sequence = 0
        self._awaiting_ack: Optional[int] = None
        self._ack_event: Optional[asyncio.Event] = None
        self._last_frame_at = 0.0
        self._last_send_at = 0.0

    # ----------------------------------------------------------------- public API

    @property
    def state(self) -> RinnaiConnectionState:
        """Current connection state."""
        return self._state

    @property
    def is_connected(self) -> bool:
        """True while the TCP session to the unit is up."""
        return self._state is RinnaiConnectionState.CONNECTED

    @property
    def is_running(self) -> bool:
        """True between start() and stop()."""
        return self._task is not None and not self._task.done()

    def register_state_handler(self, handler: StateHandler) -> None:
        """Call ``handler(state)`` now and on every change. Duplicates are ignored."""
        if handler in self._state_handlers:
            return
        self._state_handlers.append(handler)
        self._call_handler(handler, self._state)

    def unregister_state_handler(self, handler: StateHandler) -> None:
        """Stop calling ``handler`` on state changes."""
        if handler in self._state_handlers:
            self._state_handlers.remove(handler)

    async def start(self) -> None:
        """Start maintaining the connection. Returns at once; idempotent."""
        if self.is_running:
            return
        active = RinnaiConnection._active.get(self._host)
        if active is not None and active is not self and active.is_running:
            _LOGGER.error(
                "Attempting duplicate connection to unit at %s, which the hardware "
                "will not support",
                self._host,
            )
            raise RuntimeError("Cannot have two connections to the same address")
        RinnaiConnection._active[self._host] = self
        self._connected_event = asyncio.Event()
        self._commands = asyncio.Queue()
        self._ack_event = asyncio.Event()
        self._task = asyncio.get_running_loop().create_task(
            self._run(), name=f"RinnaiConnection({self._host})"
        )

    async def stop(self) -> None:
        """Close the connection and stop reconnecting. Idempotent.

        The bookkeeping runs even if the caller is cancelled while waiting.
        """
        task, self._task = self._task, None
        try:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.wait({task}, timeout=_STOP_TIMEOUT)
                if not task.done():
                    _LOGGER.error(
                        "Connection task for %s ignored cancellation for %ss; "
                        "abandoning it",
                        self._host,
                        _STOP_TIMEOUT,
                    )
                elif not task.cancelled() and task.exception() is not None:
                    _LOGGER.error(
                        "Connection task for %s ended with an error",
                        self._host,
                        exc_info=task.exception(),
                    )
        finally:
            if RinnaiConnection._active.get(self._host) is self:
                del RinnaiConnection._active[self._host]
            self._set_state(RinnaiConnectionState.IDLE)

    async def wait_connected(self, timeout: float) -> bool:
        """Wait up to ``timeout`` seconds for the session to come up."""
        if self._connected_event is None:
            return False
        try:
            async with asyncio.timeout(timeout):
                await self._connected_event.wait()
        except TimeoutError:
            return False
        return True

    def send_command(self, command: str) -> None:
        """Queue a command string for the unit. Sent in order, one at a time."""
        if self._commands is None:
            _LOGGER.error("Dropping command, connection not started: %s", command)
            return
        self._commands.put_nowait(command)

    # --------------------------------------------------------------- state changes

    def _set_state(self, state: RinnaiConnectionState) -> None:
        if state is self._state:
            return
        self._state = state
        _LOGGER.debug("Connection to %s is now %s", self._host, state.name)
        if self._connected_event is not None:
            if state is RinnaiConnectionState.CONNECTED:
                self._connected_event.set()
            else:
                self._connected_event.clear()
        for handler in list(self._state_handlers):
            self._call_handler(handler, state)

    @staticmethod
    def _call_handler(handler: StateHandler, state: RinnaiConnectionState) -> None:
        try:
            handler(state)
        except Exception:  # pylint: disable=broad-exception-caught
            _LOGGER.exception("Connection state handler %r failed", handler)

    # ----------------------------------------------------------------- main task

    async def _run(self) -> None:
        await self._resolve_host()
        while True:
            # One funnel: every outcome except a clean peer close (IDLE, which goes
            # back through discovery) waits retry_delay before the next attempt.
            try:
                if self._state is RinnaiConnectionState.IDLE:
                    await self._wait_for_broadcast()
                next_state = await self._session()
            except Exception:  # pylint: disable=broad-exception-caught
                # CancelledError is a BaseException, so stop() still gets through.
                _LOGGER.exception("Unexpected error on the connection to %s", self._host)
                next_state = RinnaiConnectionState.ERROR
            self._set_state(next_state)
            if next_state is not RinnaiConnectionState.IDLE:
                await asyncio.sleep(self._retry_delay)

    async def _resolve_host(self) -> None:
        """Resolve a hostname once so broadcasts can be matched by source address."""
        try:
            infos = await asyncio.get_running_loop().getaddrinfo(
                self._host, None, family=socket.AF_INET, type=socket.SOCK_STREAM
            )
        except OSError as err:
            _LOGGER.warning("Could not resolve %s (%s); using it as given", self._host, err)
            return
        if infos:
            self._resolved_ip = infos[0][4][0]

    async def _wait_for_broadcast(self) -> None:
        """Block until the unit announces itself, or fall back to a direct connect."""
        loop = asyncio.get_running_loop()
        listener = _BroadcastListener(self._resolved_ip)
        try:
            # SO_REUSEADDR lets several connections (one per unit) share the port; on
            # Linux each bound socket receives every broadcast.
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setblocking(False)
            sock.bind(("0.0.0.0", self._udp_port or DEFAULT_UDP_PORT))
            transport, _ = await loop.create_datagram_endpoint(lambda: listener, sock=sock)
        except OSError as err:
            _LOGGER.warning(
                "Cannot listen for the unit's broadcast (%s), connecting directly", err
            )
            self._set_state(RinnaiConnectionState.CONNECTING)
            return

        attempts = 0
        try:
            while attempts < self._udp_fallback_attempts:
                listener.found = loop.create_future()
                try:
                    async with asyncio.timeout(self._udp_timeout):
                        await listener.found
                except TimeoutError:
                    # A timeout, or packets from some other unit: both count, or
                    # discovery could wait forever on a network with two units.
                    attempts += 1 + listener.foreign
                    listener.foreign = 0
                    _LOGGER.debug(
                        "No broadcast from %s yet (%d/%d)",
                        self._host, attempts, self._udp_fallback_attempts,
                    )
                    continue
                self._set_state(RinnaiConnectionState.CONNECTING)
                return
            _LOGGER.warning(
                "No broadcast from %s after %d timeouts or foreign packets, "
                "connecting directly",
                self._host,
                attempts,
            )
            self._set_state(RinnaiConnectionState.CONNECTING)
        finally:
            transport.close()

    async def _session(self) -> RinnaiConnectionState:
        """Open the TCP session and run it until it fails; returns the next state."""
        try:
            async with asyncio.timeout(self._connect_timeout):
                reader, writer = await asyncio.open_connection(
                    self._host, self._tcp_port or DEFAULT_TCP_PORT
                )
        except ConnectionRefusedError:
            return RinnaiConnectionState.REFUSED
        except TimeoutError:
            return RinnaiConnectionState.TIMEOUT
        except OSError as err:
            _LOGGER.warning("Could not connect to %s: %s", self._host, err)
            return RinnaiConnectionState.ERROR

        self._writer = writer
        self._parser.reset()
        self._command_sequence = 1
        self._last_received_sequence = 0
        self._awaiting_ack = None
        self._ack_event.clear()
        self._last_frame_at = time.monotonic()
        self._last_send_at = time.monotonic()
        self._set_state(RinnaiConnectionState.CONNECTED)

        loop = asyncio.get_running_loop()
        receiver = loop.create_task(self._receive_loop(reader), name="rinnai-receive")
        sender = loop.create_task(self._send_loop(writer), name="rinnai-send")
        next_state = RinnaiConnectionState.IDLE
        try:
            # The session ends when either side ends: a dead sender must not leave a
            # session that looks connected but can no longer send anything.
            await asyncio.wait({receiver, sender}, return_when=asyncio.FIRST_COMPLETED)
            if receiver.done():
                next_state = receiver.result()
            elif not sender.cancelled() and sender.exception() is not None:
                _LOGGER.warning(
                    "Sending to %s failed: %r. Reconnecting", self._host, sender.exception()
                )
        finally:
            try:
                receiver.cancel()
                sender.cancel()
                await _finish(receiver, _CLOSE_TIMEOUT)
                await _finish(sender, _CLOSE_TIMEOUT)
            finally:
                # A second cancellation cannot skip this: close() is synchronous.
                self._writer = None
                writer.close()
                await _finish(loop.create_task(writer.wait_closed()), _CLOSE_TIMEOUT)
        return next_state

    async def _receive_loop(self, reader: asyncio.StreamReader) -> RinnaiConnectionState:
        """Read frames until the link drops; returns the state to enter next."""
        read = asyncio.ensure_future(reader.read(8192))
        try:
            while True:
                remaining = self._watchdog_timeout - (time.monotonic() - self._last_frame_at)
                if remaining <= 0:
                    _LOGGER.warning(
                        "No status from %s for %d seconds, reconnecting",
                        self._host, self._watchdog_timeout,
                    )
                    return RinnaiConnectionState.TIMEOUT
                done, _ = await asyncio.wait({read}, timeout=remaining)
                if read not in done:
                    continue  # watchdog tick; the read stays pending
                try:
                    data = read.result()
                except OSError as err:
                    _LOGGER.warning("Read from %s failed: %s. Reconnecting", self._host, err)
                    return RinnaiConnectionState.IDLE
                if not data:
                    _LOGGER.info("Unit at %s closed the connection. Reconnecting", self._host)
                    return RinnaiConnectionState.IDLE
                read = asyncio.ensure_future(reader.read(8192))

                hellos_before = self._parser.hellos
                try:
                    frames = self._parser.feed(data)
                except FrameOverflow as err:
                    _LOGGER.error("%s from %s; reconnecting", err, self._host)
                    return RinnaiConnectionState.ERROR
                if self._parser.hellos != hellos_before:
                    self._last_frame_at = time.monotonic()
                for sequence, payload in frames:
                    self._handle_frame(sequence, payload)
        finally:
            read.cancel()
            await _finish(read, _CLOSE_TIMEOUT)

    def _handle_frame(self, sequence: int, payload: bytes) -> None:
        self._last_frame_at = time.monotonic()
        self._last_received_sequence = sequence
        # The unit echoes the sequence number of the last command it processed.
        if sequence == self._awaiting_ack:
            self._awaiting_ack = None
            self._ack_event.set()
        try:
            status = json.loads(payload)
        except ValueError as err:
            # Covers JSONDecodeError and UnicodeDecodeError alike.
            _LOGGER.error("Could not parse status JSON (%s): %r", err, payload[:120])
            return
        try:
            self._on_status(status)
        except Exception:  # pylint: disable=broad-exception-caught
            _LOGGER.exception("Status handler failed")

    async def _send_loop(self, writer: asyncio.StreamWriter) -> None:
        """Send queued commands one at a time, and a keep-alive when idle.

        Queued commands always go first; a keep-alive is only sent when the queue
        stayed empty for the whole idle interval.
        """
        while True:
            try:
                command = self._commands.get_nowait()
            except asyncio.QueueEmpty:
                command = None
                wait = self._keepalive_interval - (time.monotonic() - self._last_send_at)
                if wait > 0:
                    try:
                        async with asyncio.timeout(wait):
                            command = await self._commands.get()
                    except TimeoutError:
                        command = None
            if command is None:
                await self._send_frame(writer, b"NA", "idle command")
            else:
                await self._send_frame(writer, command.encode(), "command")

    async def _send_frame(self, writer: asyncio.StreamWriter, payload: bytes, label: str) -> None:
        self._command_sequence = max(
            self._command_sequence + 1, self._last_received_sequence + 1
        ) % _SEQUENCE_MODULUS
        _LOGGER.debug("Sending %s %d to %s", label, self._command_sequence, self._host)
        self._ack_event.clear()
        self._awaiting_ack = self._command_sequence
        writer.write(b"N%06d" % self._command_sequence + payload)
        await writer.drain()
        self._last_send_at = time.monotonic()
        # Pace commands by the unit's acknowledgement; it copes badly with a pile-up.
        try:
            async with asyncio.timeout(self._ack_timeout):
                await self._ack_event.wait()
        except TimeoutError:
            _LOGGER.debug(
                "No acknowledgement of %d within %ss",
                self._command_sequence,
                self._ack_timeout,
            )
            self._awaiting_ack = None
