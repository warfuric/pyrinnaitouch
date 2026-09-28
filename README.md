# pyrinnaitouch

A Python interface to the Rinnai Touch / Brivis Touch wifi controller (NBW2 module),
written for the Home Assistant integration but usable on its own.

The library is asyncio based. One task per unit keeps the connection up: it waits for
the unit's UDP broadcast, opens the TCP session, reads status frames, paces commands by
the unit's acknowledgement, sends a keep-alive when idle and reconnects on any
failure. Every callback runs on the event loop.

```python
import asyncio
from pyrinnaitouch import RinnaiSystem

async def main():
    system = RinnaiSystem("192.168.1.50")
    system.subscribe_updates(lambda: print(system.get_stored_status().mode))
    await system.async_start()
    if not await system.async_wait_ready(40):
        raise SystemExit("unit not reachable")
    await system.set_unit_temp(22)
    await asyncio.sleep(5)
    await system.async_stop()

asyncio.run(main())
```

The unit accepts a single client, so create one `RinnaiSystem` per unit
(`RinnaiSystem.get_instance(ip)` keeps a registry) and stop it before the app exits.

`pyrinnaitouch.simulator.FakeRinnaiUnit` is an in-process stand-in for the module,
enough to develop and test a client without the hardware.

## Development

```
pip install -r requirements_dev.txt
python -m pytest
pylint pyrinnaitouch
```
