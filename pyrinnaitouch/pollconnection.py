"""Handle connectivity with non-blocking sockets and connection reporting."""

from collections import defaultdict
import enum
import json
import logging
from queue import Empty, SimpleQueue
import re
import selectors
import socket
import threading
import time
from typing import Tuple

_LOGGER = logging.getLogger(__name__)

# The unit frames every status as "N" + a six digit sequence number + a JSON array.
_MESSAGE_HEADER = re.compile(rb"N(\d{6})\[")
_HEADER_PREFIX = re.compile(rb"N\d{0,6}")
_HELLO = b"*HELLO*"
_BROADCAST_PREFIX = b"Rinnai_NBW2_Module"
_WHITESPACE = frozenset(b" \r\n\t")
# Shortest buffer worth parsing: a full hello, or a header minus its opening bracket.
_MIN_PARSE_LENGTH = 7
# A single status is a couple of kilobytes. Anything larger with no complete message in it
# is not going to parse, no matter how long we wait.
_MAX_BUFFER_BYTES = 64 * 1024
# The unit sends a status roughly every second; silence this long means the session is gone.
_NO_DATA_TIMEOUT_SECONDS = 30
# Sequence numbers are 0..255 (NBW2 API 4.1) and the unit echoes ours back.
_SEQUENCE_MODULUS = 256
_STOP_JOIN_SECONDS = 15
# socket.timeout only became an alias of TimeoutError in Python 3.10.
_TIMEOUT_ERRORS = (TimeoutError, socket.timeout)


class RinnaiConnectionState(enum.Enum):
    """Possible connection states for this class."""

    IDLE = 1
    CONNECTING = 2
    CONNECTED = 3
    REFUSED = 4
    TIMEOUT = 5
    ERROR = 6


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
            and _MESSAGE_HEADER.match(buffer, index)
        ):
            next_header = index
    return -1, next_header


class RinnaiPollConnection:  # pylint: disable=too-many-instance-attributes
    """Manage the non-blocking connection to the unit."""

    # Global map of IP addresses currently in use. Only used to track when multiple
    # connections are attempted, since we know how poorly the hardware handles this.
    clients = defaultdict(int)
    _clients_lock = threading.Lock()

    def __init__(
        self,
        ip_address: str,
        status_queue: SimpleQueue,
        *,
        tcp_port: int = 27847,
        udp_port: int = 50000,
    ) -> None:
        """Initialise the connection object."""
        # Set first so __del__ and stop_thread are safe on a half-built object.
        self._stopped = True
        self._stop_lock = threading.Lock()
        self._ip_address = ip_address

        with RinnaiPollConnection._clients_lock:
            if RinnaiPollConnection.clients[ip_address] > 0:
                _LOGGER.error(
                    "Attempting duplicate connection to unit at %s, which the hardware "
                    "will not support",
                    ip_address,
                )
                raise RuntimeError("Cannot have two connections to the same address")
            RinnaiPollConnection.clients[ip_address] += 1

        try:
            self._port = tcp_port
            self._command_sequence = 1
            self._last_command_time = 0.0
            self._last_received_time = 0.0
            self._command_timeout_seconds = 10
            self._hello_received = False
            self._last_received_sequence_num = 0
            self._command_wait = False
            self._command_wait_timeout_seconds = 5
            self._udp_address = "0.0.0.0"
            self._udp_port = udp_port
            self._udp_timeout_seconds = 5
            # Try TCP directly after this many timeouts or foreign packets (~30 s).
            self._udp_fallback_attempts = 6
            self._select_timeout_seconds = 1.0

            # Queue of commands (each as a string) to send to the unit.
            self._sendqueue = SimpleQueue()

            # Set on shutdown only. The event makes every wait interruptible.
            self._thread_exit_flag = False
            self._exit_event = threading.Event()

            # Writing to the sender wakes the selector immediately, so the thread
            # does not have to poll the command queue on a short timer.
            self._wake_receiver, self._wake_sender = socket.socketpair()
            self._wake_receiver.setblocking(False)
            self._wake_sender.setblocking(False)

            # These don't get created until start_thread is called
            self._socket: socket.socket = None
            self._socketthread: threading.Thread = None
            self._socketstate = RinnaiConnectionState.IDLE

            self._readbuffer = bytearray()
            self._writebuffer = bytearray()

            # List of functions to call whenever _socketstate changes
            # Provides a single argument, RinnaiConnectionState
            self._connection_state_handlers = []

            # Outbound queue of JSON status
            self._status_queue = status_queue
        except BaseException:  # pylint: disable=broad-exception-caught
            # Whatever went wrong, the slot must not stay taken for the process lifetime.
            self._release_client_slot()
            raise
        self._stopped = False
        _LOGGER.debug("Poll connection inited")

    def _release_client_slot(self) -> None:
        with RinnaiPollConnection._clients_lock:
            RinnaiPollConnection.clients[self._ip_address] -= 1
            if RinnaiPollConnection.clients[self._ip_address] < 0:
                _LOGGER.error(
                    "Somehow we have a negative number of connections; something has "
                    "gone very wrong"
                )
                # Try to restore some sanity
                RinnaiPollConnection.clients[self._ip_address] = 0

    def send_command(self, command: str) -> None:
        """Queue a command to be sent to the unit."""
        self._sendqueue.put(command)
        self._wake()

    def __del__(self):
        """Destructor to ensure the thread is stopped and the socket closed."""
        if not getattr(self, "_stopped", True):
            self.stop_thread()

    def stop_thread(self) -> None:
        """Stop the thread, close the socket, and decrement the connection tracker.

        Safe to call more than once and from any thread. The socket thread is a daemon,
        so if it somehow fails to stop in time it is abandoned rather than blocking
        interpreter exit.
        """
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True

        try:
            self._thread_exit_flag = True
            self._exit_event.set()
            self._wake()

            thread = self._socketthread
            if (
                thread is not None
                and thread.is_alive()
                and thread is not threading.current_thread()
            ):
                thread.join(_STOP_JOIN_SECONDS)
                if thread.is_alive():
                    _LOGGER.error(
                        "Monitoring thread did not stop within %d seconds; abandoning it",
                        _STOP_JOIN_SECONDS,
                    )
                else:
                    _LOGGER.debug("Monitoring thread confirmed stopped")
            self._socketthread = None

            self._close_socket()
            for wake_socket in (self._wake_receiver, self._wake_sender):
                try:
                    wake_socket.close()
                except OSError:
                    pass
        finally:
            # Let anybody listening to the status know that we're exiting.
            self._status_queue.put("sys.exit")
            self._release_client_slot()

    def _wake(self) -> None:
        """Interrupt the selector so the thread notices queued commands or shutdown."""
        try:
            self._wake_sender.send(b"\x00")
        except (BlockingIOError, OSError):
            # Full (already pending) or closed: either way the thread will get there.
            pass

    def _drain_wake(self) -> None:
        try:
            while self._wake_receiver.recv(4096):
                pass
        except (BlockingIOError, OSError):
            pass

    def _close_socket(self) -> None:
        """Best-effort shutdown and close of the TCP socket."""
        sock, self._socket = self._socket, None
        if sock is None:
            return
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            _LOGGER.debug("Socket shutdown failed, likely was not connected")
        try:
            sock.close()
        except OSError:
            _LOGGER.debug("Socket close failed, likely was not open")

    def _update_socket_state(self, socketstate: RinnaiConnectionState) -> None:
        """Update the connection state and call all registered handlers."""
        if not isinstance(socketstate, RinnaiConnectionState):
            raise TypeError("Invalid socket state")

        # Ignore unchanged states.
        if self._socketstate == socketstate:
            return

        self._socketstate = socketstate
        for handler in list(self._connection_state_handlers):
            try:
                handler(self._socketstate)
            except Exception:  # pylint: disable=broad-exception-caught
                _LOGGER.exception("Socket state handler %r failed", handler)
        _LOGGER.debug("Socket state is now %s", self._socketstate)

    def socket_state(self) -> RinnaiConnectionState:
        """Return the current state of the socket."""
        return self._socketstate

    def register_socket_state_handler(self, handler) -> None:
        """Register a new handler interested in socket state updates.

        The handler is called immediately with the current state and on every change
        after that. A failure in the handler is logged, never raised into the caller.
        Duplicate handlers are ignored.
        """
        if handler in self._connection_state_handlers:
            return
        self._connection_state_handlers.append(handler)
        try:
            handler(self._socketstate)
        except Exception:  # pylint: disable=broad-exception-caught
            _LOGGER.exception("Socket state handler %r failed", handler)

    def unregister_socket_state_handler(self, handler) -> None:
        """Unregister a socket state handler."""
        if handler in self._connection_state_handlers:
            self._connection_state_handlers.remove(handler)

    def start_thread(self) -> None:
        """Attempt connection to the unit. Results are reflected via connection_state
        property."""
        if self._stopped:
            _LOGGER.error("Cannot start the connection thread after it was stopped")
            return

        if self._socketthread is None or not self._socketthread.is_alive():
            _LOGGER.debug("Starting connection thread")
            self._socketthread = threading.Thread(
                target=self._event_loop, name="RinnaiPollConnection", daemon=True
            )
            self._socketthread.start()
        else:
            _LOGGER.error("Cannot start multiple connection threads")

    def _event_loop(self) -> None:
        """Thread that polls the socket and command queue and reacts accordingly."""
        _LOGGER.debug("Starting event loop within thread")
        while not self._thread_exit_flag:
            # Connect, then monitor. Repeat ad infinitum, unless we've been told
            # to exit. Nothing raised in here may end the thread: there is nobody
            # to restart it.
            try:
                self._create_socket_and_connect()
                if self._socketstate == RinnaiConnectionState.CONNECTED:
                    self._monitor_socket_and_queue()
            except Exception:  # pylint: disable=broad-exception-caught
                _LOGGER.exception(
                    "Unexpected error in the connection thread, reconnecting"
                )
                self._update_socket_state(RinnaiConnectionState.ERROR)
                self._exit_event.wait(5)
        _LOGGER.debug("Connection thread exiting")

    def _monitor_socket_and_queue(self) -> None:
        # Select for read events on the socket and the wake-up socket. Write events on
        # the socket aren't selected for until we have something left to say.
        with selectors.DefaultSelector() as selector:
            selector.register(self._socket, selectors.EVENT_READ)
            selector.register(self._wake_receiver, selectors.EVENT_READ)

            self._readbuffer.clear()
            self._writebuffer.clear()
            self._hello_received = False
            self._command_wait = False

            while (
                not self._thread_exit_flag
                and self._socketstate == RinnaiConnectionState.CONNECTED
            ):
                wanted = selectors.EVENT_READ
                if len(self._writebuffer) > 0:
                    wanted |= selectors.EVENT_WRITE
                    _LOGGER.debug("Selecting for write")
                selector.modify(self._socket, wanted)

                for key, mask in selector.select(self._select_timeout_seconds):
                    if key.fileobj is self._wake_receiver:
                        self._drain_wake()
                        continue
                    if mask & selectors.EVENT_READ:
                        self._receive()
                    if mask & selectors.EVENT_WRITE:
                        # We are able to write to the socket, and have something to say.
                        self._attempt_send()

                self._service_send_queue()
                self._process_received_data()

                if (
                    time.monotonic() - self._last_received_time
                    > _NO_DATA_TIMEOUT_SECONDS
                ):
                    _LOGGER.warning(
                        "No status received from the unit for %d seconds, reconnecting",
                        _NO_DATA_TIMEOUT_SECONDS,
                    )
                    self._update_socket_state(RinnaiConnectionState.TIMEOUT)

    def _receive(self) -> None:
        """Read whatever is available on the socket into the receive buffer."""
        try:
            newbytes = self._socket.recv(8192)
        except OSError as ose:
            _LOGGER.warning("Socket error on recv: %s. Reconnecting", ose)
            self._update_socket_state(RinnaiConnectionState.IDLE)
            return

        if len(newbytes) == 0:
            # The socket has disconnected. Reconnection is attempted by the event loop.
            _LOGGER.info("Socket disconnected. Reconnecting")
            self._update_socket_state(RinnaiConnectionState.IDLE)
            return

        self._readbuffer.extend(newbytes)
        _LOGGER.debug(
            "Read %d bytes from socket, receive buffer now has %d bytes to process",
            len(newbytes),
            len(self._readbuffer),
        )

    def _service_send_queue(self) -> None:
        """Move queued commands into the write buffer, or send a keep-alive if idle.

        We don't wait for anything to arrive here; the waiting happens in select.
        """
        while not self._thread_exit_flag:
            if self._command_wait and (
                (time.monotonic() - self._last_command_time)
                < self._command_wait_timeout_seconds
            ):
                # The unit has not acknowledged the previous command yet.
                return
            try:
                command = self._sendqueue.get_nowait()
            except Empty:
                # Nothing in the queue for now. Send an empty command if it's been
                # long enough, so the unit knows we're still here.
                if (
                    time.monotonic() - self._last_command_time
                    > self._command_timeout_seconds
                ):
                    self._queue_frame(b"NA", "idle command")
                return
            self._queue_frame(command.encode(), "command")

    def _queue_frame(self, payload: bytes, label: str) -> None:
        """Frame a payload with the next sequence number and attempt to send it."""
        self._command_sequence = max(
            self._command_sequence + 1, self._last_received_sequence_num + 1
        )
        self._command_sequence %= _SEQUENCE_MODULUS
        header = b"N%06d" % self._command_sequence
        self._writebuffer.extend(header)
        self._writebuffer.extend(payload)
        _LOGGER.debug("Sending %s %d", label, self._command_sequence)
        self._attempt_send()

        # Update the time here in case the socket doesn't become write available
        # quickly, so we don't queue the same keep-alive again next loop.
        self._last_command_time = time.monotonic()
        self._command_wait = True

    def _attempt_send(self) -> None:
        # Attempt to send the contents of the write buffer. Only remove bytes that are
        # successfully sent,  which may not be all that we requested. Any bytes
        # remaining in the buffer will be caught in the next select call.
        try:
            num_sent = self._socket.send(self._writebuffer)
            _LOGGER.debug("Sent %d of %d bytes", num_sent, len(self._writebuffer))
            del self._writebuffer[:num_sent]

            self._last_command_time = time.monotonic()
            if len(self._writebuffer) > 0:
                _LOGGER.warning(
                    "There are %d bytes remaining to send. There may be network "
                    "congestion, or the connection is about to fail",
                    len(self._writebuffer),
                )
        except OSError as ose:
            _LOGGER.warning("Socket error on send: %s. Reconnecting", ose)
            self._update_socket_state(RinnaiConnectionState.IDLE)

    def _process_received_data(self) -> None:
        """Parse every complete message in the receive buffer.

        Works on bytes throughout, so a packet boundary in the middle of a multi-byte
        character cannot raise. Every pass either consumes bytes, waits for more, or
        resets the connection, so it cannot spin on the same data.
        """
        buffer = self._readbuffer
        while True:
            skip = 0
            while skip < len(buffer) and buffer[skip] in _WHITESPACE:
                skip += 1
            if skip:
                del buffer[:skip]
            if len(buffer) < _MIN_PARSE_LENGTH:
                break

            if buffer.startswith(_HELLO):
                if self._hello_received:
                    _LOGGER.warning(
                        "Hello message received more than once. Has the unit reset?"
                    )
                else:
                    _LOGGER.info("Hello message successfully received from unit")
                    self._hello_received = True
                self._last_received_time = time.monotonic()
                del buffer[: len(_HELLO)]
                continue

            header = _MESSAGE_HEADER.match(buffer)
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
                if len(buffer) > _MAX_BUFFER_BYTES:
                    _LOGGER.error(
                        "No complete message in %d bytes of data; discarding it and "
                        "reconnecting",
                        len(buffer),
                    )
                    buffer.clear()
                    self._update_socket_state(RinnaiConnectionState.ERROR)
                    break
                _LOGGER.debug("Incomplete message in buffer, waiting for more data")
                break

            self._handle_message(
                int(header.group(1)), bytes(buffer[json_start : json_end + 1])
            )
            del buffer[: json_end + 1]

    def _resync(self) -> bool:
        """Drop unparseable bytes from the front of the buffer.

        Returns True when a hello or message header was found further along and the
        buffer now starts with it, False when there is nothing left worth parsing yet.
        """
        buffer = self._readbuffer
        candidates = [buffer.find(_HELLO, 1)]
        if (header := _MESSAGE_HEADER.search(buffer, 1)) is not None:
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

    def _handle_message(self, sequence: int, payload: bytes) -> None:
        """Record a message's sequence number and queue its status for the consumer."""
        self._last_received_time = time.monotonic()
        self._last_received_sequence_num = sequence
        _LOGGER.debug("Received sequence number %d", sequence)
        # The unit echoes the sequence number of the last command it processed.
        if self._command_wait and sequence == self._command_sequence:
            self._command_wait = False
            _LOGGER.debug("Command wait end")

        try:
            json_status = json.loads(payload)
        except ValueError as err:
            # Covers JSONDecodeError and UnicodeDecodeError alike.
            _LOGGER.error("Could not parse JSON data (%s): %r", err, payload[:120])
            return
        self._status_queue.put(json_status)

    def _wait_for_broadcast(self) -> None:
        """Wait for the unit's UDP broadcast before connecting.

        Falls back to a direct TCP attempt after enough timeouts or packets from other
        units. Returns immediately on shutdown, thanks to the wake-up socket.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udpsock, (
            selectors.DefaultSelector()
        ) as selector:
            try:
                udpsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                udpsock.setblocking(False)
                udpsock.bind((self._udp_address, self._udp_port))
            except OSError as err:
                _LOGGER.warning(
                    "Cannot listen for the unit's broadcast (%s), connecting directly",
                    err,
                )
                self._update_socket_state(RinnaiConnectionState.CONNECTING)
                return
            selector.register(udpsock, selectors.EVENT_READ)
            selector.register(self._wake_receiver, selectors.EVENT_READ)

            attempts = 0
            while (
                self._socketstate == RinnaiConnectionState.IDLE
                and not self._thread_exit_flag
            ):
                ready = {
                    key.fileobj for key, _ in selector.select(self._udp_timeout_seconds)
                }
                if self._wake_receiver in ready:
                    self._drain_wake()
                    if udpsock not in ready:
                        continue
                if udpsock in ready:
                    try:
                        data, addr = udpsock.recvfrom(1024)
                    except OSError as err:
                        _LOGGER.warning("Broadcast receive failed: %s", err)
                        data, addr = b"", ("", 0)
                    if data.startswith(_BROADCAST_PREFIX) and addr[0] == self._ip_address:
                        _LOGGER.debug("Broadcast received from %s: %s", addr[0], data.hex())
                        self._update_socket_state(RinnaiConnectionState.CONNECTING)
                        return
                # A timeout, or a packet from some other unit: both count, or discovery
                # could wait forever on a network with two units.
                attempts += 1
                if attempts < self._udp_fallback_attempts:
                    _LOGGER.debug(
                        "No broadcast from %s yet, retrying (%d/%d)",
                        self._ip_address,
                        attempts,
                        self._udp_fallback_attempts,
                    )
                    continue
                _LOGGER.warning(
                    "No broadcast from %s after %d timeouts or foreign packets, "
                    "connecting directly",
                    self._ip_address,
                    attempts,
                )
                self._update_socket_state(RinnaiConnectionState.CONNECTING)

    def _create_socket_and_connect(self) -> None:
        self._wait_for_broadcast()

        # If an old socket exists, try and clean it up.
        self._close_socket()

        while (
            self._socketstate != RinnaiConnectionState.CONNECTED
            and not self._thread_exit_flag
        ):
            try:
                # Set up the socket and update the state
                self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._socket.settimeout(5)
                self._socket.connect((self._ip_address, self._port))

                # If we've made it to here, we connected successfully.
                self._update_socket_state(RinnaiConnectionState.CONNECTED)

                # Reset the timestamps and command sequence number
                self._last_command_time = time.monotonic()
                self._last_received_time = self._last_command_time
                self._command_sequence = 1

                # Switch to non-blocking mode.
                self._socket.settimeout(0)

            except ConnectionRefusedError:
                self._update_socket_state(RinnaiConnectionState.REFUSED)
                self._exit_event.wait(5)
            except _TIMEOUT_ERRORS:
                self._update_socket_state(RinnaiConnectionState.TIMEOUT)
                self._exit_event.wait(5)
            except (ConnectionError, BlockingIOError, InterruptedError):
                # All of these things could be transient, so try again after a
                # small wait.
                self._exit_event.wait(10)
            except OSError as e:
                self._update_socket_state(RinnaiConnectionState.ERROR)
                _LOGGER.warning('Unexpected connection error: "%s", will retry', e)
                self._exit_event.wait(10)
