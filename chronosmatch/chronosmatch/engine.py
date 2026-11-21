"""Pick the fastest available order-book backend.

Uses the compiled Cython extension when present, otherwise the pure-Python
reference. Force the fallback with CHRONOS_BACKEND=python.
"""
import os

OrderBook = None
BACKEND = "python"
if os.environ.get("CHRONOS_BACKEND", "").lower() != "python":
    try:
        from ._matcher import OrderBook  # type: ignore
        BACKEND = "cython"
    except ImportError:
        OrderBook = None
if OrderBook is None:
    from .matcher_py import OrderBook  # noqa: F811
