"""Session API shim.

The robust transport runtime lives in `engine.py`.
"""

from .engine import P2PStreamer

__all__ = ["P2PStreamer"]
