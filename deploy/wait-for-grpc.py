#!/usr/bin/env python3
"""Wait until Sentinel accepts local gRPC connections before starting the indexer."""
import socket
import time


def wait_for_port(host="127.0.0.1", port=50052, retry_interval=5):
    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError:
            time.sleep(retry_interval)


if __name__ == "__main__":
    wait_for_port()
