# Third-party notices

Meshtastic Lab uses and interoperates with the projects below. Their licenses remain in force.

## Meshtastic firmware

- Source: <https://github.com/meshtastic/firmware>
- Runtime revision: `54e0d8d0ab2ff56b3a9ce967e53f79e49af560fb`
- License: GPL-3.0-or-later
- Use: native `meshtasticd` firmware, built with the upstream simulator collision preference enabled.

## Meshtastic Python

- Source: <https://github.com/meshtastic/python>
- Source revision inspected: `0539a9600fc6c15eda398494ffb0309322cfb089`
- Package: `meshtastic==2.7.11`
- License: GPL-3.0-only
- Use: official Client API protobufs, acceptance client, and compatibility tests.

## Meshtastic protobufs

- Source: <https://github.com/meshtastic/protobufs>
- Firmware submodule revision: `6b1ded439633cd03d4af85b44231b91d1d106278`
- License: project license applies
- Use: wire-format definitions distributed through the official Python package and native firmware.

## Meshtasticator

- Source: <https://github.com/meshtastic/Meshtasticator>
- Revision inspected and adapted: `17ceb8231079d87b070abc6132181e4c6b20202d`
- License: Creative Commons Attribution 4.0 International
- Attribution: Meshtasticator authors and contributors
- Use: the `SIMULATOR_APP` RF forwarding model, native-node launch behavior, and collision investigation informed Meshtastic Lab's independently written asyncio implementation.

Meshtasticator itself credits work derived from LoRaSim and the research named in its README. Meshtastic Lab does not copy its path-loss or discrete-event implementation.
