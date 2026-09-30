"""Raft consensus with a deterministic fault-injection simulator."""

from .bugs import BUG_NAMES, Bugs
from .sim import RunResult, SimConfig, Simulator, run_simulation

__all__ = ["BUG_NAMES", "Bugs", "RunResult", "SimConfig", "Simulator", "run_simulation"]
__version__ = "0.1.0"
