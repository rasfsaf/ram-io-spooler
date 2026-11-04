"""Entrypoint for python -m spooler."""
import sys
from spooler.server import run_server

if __name__ == "__main__":
    try:
        run_server()
    except KeyboardInterrupt:
        sys.exit(0)
