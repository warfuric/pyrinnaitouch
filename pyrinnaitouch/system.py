"""Main system control"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from .const import RinnaiSystemMode, RinnaiUnitId
from .connection import RinnaiConnection, RinnaiConnectionState
from .event import Event
from .system_status import RinnaiSystemStatus
from .commands import (
    EVAP_ON_CMD,
    EVAP_OFF_CMD,
    EVAP_PUMP_ON,
    EVAP_PUMP_OFF,
    EVAP_FAN_ON,
    EVAP_FAN_OFF,
    EVAP_FAN_SPEED,
    EVAP_SET_COMFORT,
    EVAP_ZONE_ON,
    EVAP_ZONE_OFF,
    EVAP_ZONE_SET_MANUAL,
    EVAP_ZONE_SET_AUTO,
    MODE_COOL_CMD,
    MODE_EVAP_CMD,
    MODE_HEAT_CMD,
    EVAP_COMMANDS,
    MODE_COMMANDS,
    SYSTEM_ENTER_TIME_SETTING,
    SYSTEM_SAVE_TIME,
    SYSTEM_SET_TIME,
    UNIT_ADVANCE,
    UNIT_ADVANCE_CANCEL,
    UNIT_CIRC_FAN_ON,
    UNIT_CIRC_FAN_SPEED,
    UNIT_COMMANDS,
    UNIT_OFF_CMD,
    UNIT_ON_CMD,
    UNIT_SET_AUTO,
    UNIT_SET_MANUAL,
    UNIT_SET_TEMP,
    UNIT_ZONE_ADVANCE,
    UNIT_ZONE_ADVANCE_CANCEL,
    UNIT_ZONE_OFF,
    UNIT_ZONE_ON,
    UNIT_ZONE_SET_AUTO,
    UNIT_ZONE_SET_MANUAL,
    UNIT_ZONE_SET_TEMP,
)


_LOGGER = logging.getLogger(__name__)


class RinnaiSystem:
    """Main controller class to interact with the Rinnai Touch Wifi unit.

    Create it inside a running event loop, ``await async_start()``, and read
    ``get_stored_status()`` whenever an update callback fires. Every callback runs on
    the event loop.
    """

    # pylint: disable=too-many-instance-attributes,too-many-public-methods

    instances: Dict[str, "RinnaiSystem"] = {}

    def __init__(self, ip_address: str, **connection_options: Any) -> None:
        self._ip_address = ip_address
        self._connection = RinnaiConnection(
            ip_address, self._handle_status_message, **connection_options
        )
        self._status = RinnaiSystemStatus()
        self._have_status = False
        self._status_event: Optional[asyncio.Event] = None
        self._on_updated = Event()
        self._connection.register_state_handler(self._handle_connection_state)

    @classmethod
    def get_instance(cls, ip_address: str, **connection_options: Any) -> "RinnaiSystem":
        """Get the single instance for a unit, creating it on first use."""
        system = cls.instances.get(ip_address)
        if system is None:
            system = cls.instances[ip_address] = cls(ip_address, **connection_options)
        return system

    @classmethod
    async def async_remove_instance(cls, ip_address: str) -> None:
        """Stop and forget the instance for a unit."""
        system = cls.instances.pop(ip_address, None)
        if system is None:
            _LOGGER.warning("No instance found for IP: %s", ip_address)
            return
        await system.async_stop()
        _LOGGER.debug("Removed instance for IP: %s", ip_address)

    # ------------------------------------------------------------- lifecycle

    async def async_start(self) -> None:
        """Start connecting to the unit and keep the connection up. Idempotent."""
        if self._status_event is None:
            self._status_event = asyncio.Event()
        await self._connection.start()

    async def async_stop(self) -> None:
        """Disconnect from the unit and stop reconnecting. Idempotent."""
        await self._connection.stop()

    async def async_wait_ready(self, timeout: float) -> bool:
        """Wait up to ``timeout`` seconds for the first status from the unit."""
        if self._have_status:
            return True
        if self._status_event is None:
            return False
        try:
            async with asyncio.timeout(timeout):
                await self._status_event.wait()
        except TimeoutError:
            return False
        return True

    @property
    def is_connected(self) -> bool:
        """True while the TCP session to the unit is up."""
        return self._connection.is_connected

    def socket_state(self) -> RinnaiConnectionState:
        """Current connection state."""
        return self._connection.state

    def subscribe_updates(self, obj_method: Any) -> None:
        """Subscribe to updates when the system status refreshes."""
        self._on_updated += obj_method

    def unsubscribe_updates(self, obj_method: Any) -> None:
        """Unsubscribe from updates received when the system status refreshes."""
        self._on_updated -= obj_method

    def register_socket_state_handler(self, socket_handler: Any) -> None:
        """Register a socket state handler to receive updates."""
        self._connection.register_state_handler(socket_handler)

    def unregister_socket_state_handler(self, socket_handler: Any) -> None:
        """Unregister a socket state handler."""
        self._connection.unregister_state_handler(socket_handler)

    # --------------------------------------------------------- incoming data

    def _handle_connection_state(self, state: RinnaiConnectionState) -> None:
        if state is RinnaiConnectionState.CONNECTED or not self._have_status:
            return
        # The unit is gone: stop reporting its last status as if it were live.
        self._have_status = False
        self._status = RinnaiSystemStatus()
        self._on_updated()

    def _handle_status_message(self, new_status_json: Any) -> None:
        """Turn one decoded status message into a status object and notify."""
        if not new_status_json:
            return
        if (
            isinstance(new_status_json, list)
            and isinstance(new_status_json[0], dict)
            and "SYST" in new_status_json[0]
            and "STM" in new_status_json[0]["SYST"]
        ):
            self._status.set_timesetting(True)
            self._on_updated()
            return

        status = RinnaiSystemStatus()
        if not status.handle_status(new_status_json):
            _LOGGER.error("JSON Error: %s", new_status_json)
            return
        self._status = status
        self._have_status = True
        if self._status_event is not None:
            self._status_event.set()
        self._on_updated()

    # --------------------------------------------------------------- commands

    async def set_cooling_mode(self) -> bool:
        """Set system to cooling mode."""
        return self.validate_and_send(MODE_COOL_CMD)

    async def set_evap_mode(self) -> bool:
        """Set system to evap mode."""
        return self.validate_and_send(MODE_EVAP_CMD)

    async def set_heater_mode(self) -> bool:
        """Set system to heater mode."""
        return self.validate_and_send(MODE_HEAT_CMD)

    async def turn_unit_on(self) -> bool:
        """Turn unit on (and system)."""
        cmd = UNIT_ON_CMD
        if self.validate_command(cmd):
            self.send_command(cmd.format(unit_id=self._status.unit_status.unit_id))
            return True
        return False

    async def turn_heater_on(self) -> bool:
        """Turn unit on (and system)."""
        cmd = UNIT_ON_CMD
        if self.validate_command(cmd):
            self.send_command(cmd.format(unit_id=str(RinnaiUnitId.HEATER)))
            return True
        return False

    async def turn_cooler_on(self) -> bool:
        """Turn unit on (and system)."""
        cmd = UNIT_ON_CMD
        if self.validate_command(cmd):
            self.send_command(cmd.format(unit_id=str(RinnaiUnitId.COOLER)))
            return True
        return False

    async def turn_unit_off(self) -> bool:
        """Turn unit off (and system)."""
        cmd = UNIT_OFF_CMD
        if self.validate_command(cmd):
            self.send_command(cmd.format(unit_id=self._status.unit_status.unit_id))
            return True
        return False

    async def turn_unit_fan_only(self) -> bool:
        """Turn circ fan on in while system is off."""
        cmd = UNIT_CIRC_FAN_ON
        if self.validate_command(cmd):
            self.send_command(cmd.format(unit_id=self._status.unit_status.unit_id))
            return True
        return False

    async def set_unit_temp(self, temp: int) -> bool:
        """Set target temperature."""
        cmd = UNIT_SET_TEMP
        if self.validate_command(cmd):
            self.send_command(
                cmd.format(unit_id=self._status.unit_status.unit_id, temp=f"{temp:02d}")
            )
            return True
        return False

    async def set_unit_auto(self) -> bool:
        """Set to auto mode."""
        cmd = UNIT_SET_AUTO
        if self.validate_command(cmd):
            self.send_command(cmd.format(unit_id=self._status.unit_status.unit_id))
            return True
        return False

    async def set_unit_manual(self) -> bool:
        """Set to manual mode."""
        cmd = UNIT_SET_MANUAL
        if self.validate_command(cmd):
            self.send_command(cmd.format(unit_id=self._status.unit_status.unit_id))
            return True
        return False

    async def unit_advance(self) -> bool:
        """Press advance button."""
        cmd = UNIT_ADVANCE
        if self.validate_command(cmd):
            self.send_command(cmd.format(unit_id=self._status.unit_status.unit_id))
            return True
        return False

    async def unit_advance_cancel(self) -> bool:
        """Press advance cancel button."""
        cmd = UNIT_ADVANCE_CANCEL
        if self.validate_command(cmd):
            self.send_command(cmd.format(unit_id=self._status.unit_status.unit_id))
            return True
        return False

    async def turn_unit_zone_on(self, zone: str) -> bool:
        """Turn a zone on."""
        cmd = UNIT_ZONE_ON
        if self.validate_command(cmd):
            self.send_command(
                cmd.format(unit_id=self._status.unit_status.unit_id, zone=zone)
            )
            return True
        return False

    async def turn_unit_zone_off(self, zone: str) -> bool:
        """Turn a zone off."""
        cmd = UNIT_ZONE_OFF
        if self.validate_command(cmd):
            self.send_command(
                cmd.format(unit_id=self._status.unit_status.unit_id, zone=zone)
            )
            return True
        return False

    async def set_unit_zone_temp(self, zone: str, temp: int) -> bool:
        """Set target temperature for a zone."""
        cmd = UNIT_ZONE_SET_TEMP
        if self.validate_command(cmd):
            self.send_command(
                cmd.format(
                    unit_id=self._status.unit_status.unit_id,
                    zone=zone,
                    temp=f"{temp:02d}",
                )
            )
            return True
        return False

    async def set_unit_zone_auto(self, zone: str) -> bool:
        """Set zone to auto mode."""
        cmd = UNIT_ZONE_SET_AUTO
        if self.validate_command(cmd):
            self.send_command(
                cmd.format(unit_id=self._status.unit_status.unit_id, zone=zone)
            )
            return True
        return False

    async def set_unit_zone_manual(self, zone: str) -> bool:
        """Set zone to manual mode."""
        cmd = UNIT_ZONE_SET_MANUAL
        if self.validate_command(cmd):
            self.send_command(
                cmd.format(unit_id=self._status.unit_status.unit_id, zone=zone)
            )
            return True
        return False

    async def set_unit_zone_advance(self, zone: str) -> bool:
        """Press zone advance button."""
        cmd = UNIT_ZONE_ADVANCE
        if self.validate_command(cmd):
            self.send_command(
                cmd.format(unit_id=self._status.unit_status.unit_id, zone=zone)
            )
            return True
        return False

    async def set_unit_zone_advance_cancel(self, zone: str) -> bool:
        """Press zone advance cacnel button."""
        cmd = UNIT_ZONE_ADVANCE_CANCEL
        if self.validate_command(cmd):
            self.send_command(
                cmd.format(unit_id=self._status.unit_status.unit_id, zone=zone)
            )
            return True
        return False

    async def turn_evap_on(self) -> bool:
        """Turn on evap (and system)."""
        return self.validate_and_send(EVAP_ON_CMD)

    async def turn_evap_off(self) -> bool:
        """Turn off evap (and system)."""
        return self.validate_and_send(EVAP_OFF_CMD)

    async def turn_evap_pump_on(self) -> bool:
        """Turn water pump on in evap mode."""
        return self.validate_and_send(EVAP_PUMP_ON)

    async def turn_evap_pump_off(self) -> bool:
        """Turn water pump off in evap mode."""
        return self.validate_and_send(EVAP_PUMP_OFF)

    async def turn_evap_fan_on(self) -> bool:
        """Turn fan on in evap mode."""
        return self.validate_and_send(EVAP_FAN_ON)

    async def turn_evap_fan_off(self) -> bool:
        """Turn fan off in evap mode."""
        return self.validate_and_send(EVAP_FAN_OFF)

    async def set_evap_fanspeed(self, speed: int) -> bool:
        """Set fan speed in evap mode."""
        cmd = EVAP_FAN_SPEED
        if self.validate_command(cmd):
            self.send_command(cmd.format(speed=f"{speed:02d}"))
            return True
        return False

    async def set_unit_fanspeed(self, speed: int) -> bool:
        """Set fan speed."""
        cmd = UNIT_CIRC_FAN_SPEED
        if self.validate_command(cmd):
            self.send_command(
                cmd.format(
                    unit_id=self._status.unit_status.unit_id, speed=f"{speed:02d}"
                )
            )
            return True
        return False

    async def set_evap_comfort(self, comfort: int) -> bool:
        """Set comfort level in Evap auto mode."""
        cmd = EVAP_SET_COMFORT
        if self.validate_command(cmd):
            self.send_command(cmd.format(comfort=comfort))
            return True
        return False

    async def turn_evap_zone_on(self, zone: str) -> bool:
        """Turn zone off in Evap mode."""
        cmd = EVAP_ZONE_ON
        if self.validate_command(cmd):
            self.send_command(cmd.format(zone=zone))
            return True
        return False

    async def turn_evap_zone_off(self, zone: str) -> bool:
        """Turn zone off in Evap mode."""
        cmd = EVAP_ZONE_OFF
        if self.validate_command(cmd):
            self.send_command(cmd.format(zone=zone))
            return True
        return False

    async def set_evap_zone_auto(self, zone: str) -> bool:
        """Set zone to Auto mode on Evap."""
        cmd = EVAP_ZONE_SET_AUTO
        if self.validate_command(cmd):
            self.send_command(cmd.format(zone=zone))
            return True
        return False

    async def set_evap_zone_manual(self, zone: str) -> bool:
        """Set zone to manual mode on Evap."""
        cmd = EVAP_ZONE_SET_MANUAL
        if self.validate_command(cmd):
            self.send_command(cmd.format(zone=zone))
            return True
        return False

    async def set_system_time(self, set_datetime: datetime = None) -> bool:
        """Set system time."""
        now = datetime.now()
        if set_datetime is not None and isinstance(set_datetime, datetime):
            now = set_datetime
        set_time = now.strftime("%H:%M")
        set_day = now.strftime("%a").upper()
        result = self.validate_and_send(SYSTEM_ENTER_TIME_SETTING)
        cmd = SYSTEM_SET_TIME
        if self.validate_command(cmd):
            result = (
                self.send_command(cmd.format(day=set_day, time=set_time)) and result
            )
        return self.validate_and_send(SYSTEM_SAVE_TIME) and result

    def get_stored_status(self) -> RinnaiSystemStatus:
        """Get the current status without a refresh."""
        return self._status

    def validate_command(self, cmd: str) -> bool:
        """Validate a command is appropriate to the current operating mode."""
        if cmd in MODE_COMMANDS:
            return True
        if cmd in UNIT_COMMANDS and self._status.mode in (
            RinnaiSystemMode.HEATING,
            RinnaiSystemMode.COOLING,
        ):
            return True
        if cmd in EVAP_COMMANDS and self._status.mode == RinnaiSystemMode.EVAP:
            return True
        return False

    def send_command(self, cmd: str) -> None:
        """Send the command to the unit."""
        self._connection.send_command(cmd)

    def validate_and_send(self, cmd: str) -> bool:
        """Validate and send a command."""
        if self.validate_command(cmd):
            self.send_command(cmd)
            return True
        _LOGGER.error(
            "Validation of command failed. Not sending. CMD: %s, Mode: %s",
            cmd,
            self._status.mode,
        )
        return False
