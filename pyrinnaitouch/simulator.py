"""An in-process stand-in for the Rinnai Touch wifi module, for tests and development.

It speaks just enough of the NBW2 protocol to exercise a client: a UDP broadcast
announcing itself, ``*HELLO*`` on connect, a status frame every ``status_interval``
seconds carrying the echo of the last command's sequence number, and a log of every
command received. Nothing here is a faithful model of the heater's behaviour.
"""

from __future__ import annotations

import asyncio
import json
import re
import socket
from typing import Any, List, Optional, Tuple

_COMMAND = re.compile(rb"N(\d{6})")

DEFAULT_STATUS: List[Any] = [
    {
        "SYST": {
            "CFG": {"MTSP": "N", "NC": "00", "DF": "N", "TU": "C", "CF": "1", "VR": "0183",
                    "CV": "0010", "CC": "043", "ZA": " ", "ZB": " ", "ZC": " ", "ZD": " "},
            "AVM": {"HG": "Y", "EC": "N", "CG": "Y", "RA": "N", "RH": "N", "RC": "N"},
            "OSS": {"DY": "TUE", "TM": "16:45", "BP": "Y", "RG": "Y", "ST": "N", "MD": "H",
                    "DE": "N", "DU": "N", "AT": "999", "LO": "N"},
            "FLT": {"AV": "N", "C3": "000"},
        }
    },
    {
        "HGOM": {
            "CFG": {"ZUIS": "N", "ZAIS": "Y", "ZBIS": "Y", "ZCIS": "N", "ZDIS": "N", "CF": "N",
                    "PS": "Y", "DG": "W"},
            "OOP": {"ST": "N", "CF": "N", "FL": "08", "SN": "Y"},
            "GSO": {"OP": "M", "SP": "22", "AO": "N"},
            "GSS": {"HC": "Y", "FS": "Y", "GV": "Y", "PH": "N", "AT": "N"},
            "APS": {"AV": "N"},
            "ZUS": {"AE": "N", "MT": "999"},
            "ZAS": {"AE": "N", "MT": "215"},
            "ZBS": {"AE": "N", "MT": "999"},
            "ZCS": {"AE": "N", "MT": "999"},
            "ZDS": {"AE": "N", "MT": "999"},
        }
    },
]


class FakeRinnaiUnit:  # pylint: disable=too-many-instance-attributes
    """TCP server plus optional UDP broadcaster on the loopback interface."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        host: str = "127.0.0.1",
        tcp_port: int = 0,
        udp_port: Optional[int] = None,
        status: Optional[List[Any]] = None,
        status_interval: float = 0.2,
        broadcast_interval: float = 0.2,
    ) -> None:
        self.host = host
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.status: List[Any] = status if status is not None else DEFAULT_STATUS
        self.status_interval = status_interval
        self.broadcast_interval = broadcast_interval

        self.send_status = True
        self.echo = True
        self.hello = True
        self.commands: List[Tuple[int, str]] = []
        self.connections = 0

        self._server: Optional[asyncio.AbstractServer] = None
        self._broadcaster: Optional[asyncio.Task] = None
        self._clients: List[asyncio.StreamWriter] = []
        self._last_sequence = 0

    async def start(self) -> "FakeRinnaiUnit":
        """Bind the TCP port (and start broadcasting if a UDP port was given)."""
        self._server = await asyncio.start_server(self._serve, self.host, self.tcp_port)
        self.tcp_port = self._server.sockets[0].getsockname()[1]
        if self.udp_port is not None:
            self._broadcaster = asyncio.get_running_loop().create_task(self._broadcast())
        return self

    async def stop(self) -> None:
        """Close everything."""
        if self._broadcaster is not None:
            self._broadcaster.cancel()
            try:
                await self._broadcaster
            except asyncio.CancelledError:
                pass
            self._broadcaster = None
        server, self._server = self._server, None
        if server is not None:
            server.close()  # stop accepting first, or a client can reconnect in the gap
        await self.disconnect_clients()
        if server is not None:
            await asyncio.wait({asyncio.ensure_future(server.wait_closed())}, timeout=2)

    async def disconnect_clients(self) -> None:
        """Drop every connected client, as the unit does when it reboots."""
        clients, self._clients = self._clients, []
        for writer in clients:
            writer.close()
        for writer in clients:
            try:
                await writer.wait_closed()
            except (OSError, asyncio.CancelledError):
                pass

    def send_raw(self, data: bytes) -> None:
        """Push arbitrary bytes to every client, to simulate corruption."""
        for writer in self._clients:
            writer.write(data)

    async def _broadcast(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind((self.host, 0))
            sock.setblocking(False)
            while True:
                try:
                    # A datagram to loopback never blocks; no need for loop.sock_sendto
                    # (which only exists from Python 3.11).
                    sock.sendto(b"Rinnai_NBW2_Module\x00", (self.host, self.udp_port))
                except OSError:
                    pass
                await asyncio.sleep(self.broadcast_interval)

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connections += 1
        self._clients.append(writer)
        if self.hello:
            writer.write(b"*HELLO*")
        pump = asyncio.get_running_loop().create_task(self._pump_status(writer))
        try:
            buffer = b""
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                buffer += data
                buffer = self._consume_commands(buffer)
        except OSError:
            pass
        finally:
            pump.cancel()
            await asyncio.wait({pump})  # never re-raises the pump's own cancellation
            if writer in self._clients:
                self._clients.remove(writer)
            writer.close()

    def _consume_commands(self, buffer: bytes) -> bytes:
        while True:
            match = _COMMAND.match(buffer)
            if match is None:
                return buffer
            sequence = int(match.group(1))
            rest = buffer[match.end():]
            if rest.startswith(b"NA"):
                payload, consumed = "NA", 2
            else:
                try:
                    decoded, consumed = json.JSONDecoder().raw_decode(rest.decode())
                except ValueError:
                    return buffer  # incomplete, wait for more
                payload = json.dumps(decoded)
            self.commands.append((sequence, payload))
            if self.echo:
                self._last_sequence = sequence
            buffer = rest[consumed:]

    async def _pump_status(self, writer: asyncio.StreamWriter) -> None:
        while True:
            await asyncio.sleep(self.status_interval)
            if not self.send_status:
                continue
            frame = b"N%06d" % self._last_sequence + json.dumps(self.status).encode()
            writer.write(frame)
            try:
                await writer.drain()
            except OSError:
                return
