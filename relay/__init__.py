"""relay — a generic resource-management proxy platform.

Proxies traffic to upstream servers and manages their resources (power,
concurrency) with the goal of scaling to zero when traffic is absent.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("relay")
except PackageNotFoundError:  # pragma: no cover - uninstalled source checkout
    __version__ = "0.0.0"

__all__ = ["__version__"]
