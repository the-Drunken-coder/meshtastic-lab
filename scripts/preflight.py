#!/usr/bin/env python3
"""Check the fixed loopback ports before Docker starts the product."""

from __future__ import annotations

import socket
from collections.abc import Iterable

HOST = "127.0.0.1"
REQUIRED_PORTS = (8080, *range(45001, 45011))


def unavailable_ports(host: str, ports: Iterable[int]) -> list[int]:
    unavailable: list[int] = []
    for port in ports:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((host, port))
        except OSError:
            unavailable.append(port)
        finally:
            listener.close()
    return unavailable


def main() -> None:
    unavailable = unavailable_ports(HOST, REQUIRED_PORTS)
    if unavailable:
        ports = ", ".join(str(port) for port in unavailable)
        raise SystemExit(
            f"Meshtastic Lab cannot bind {HOST}; unavailable ports: {ports}. "
            "Stop the conflicting process or stack, then retry."
        )


if __name__ == "__main__":
    main()
