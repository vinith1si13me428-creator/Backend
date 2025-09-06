"""
Tiny JSON-file persistence layer that exposes
save_strategy_state() / load_strategy_state()
so the strategy modules keep working even when a full
PostgreSQL DatabaseManager is not yet initialised.
"""

import json
import time
from pathlib import Path
from typing import Dict, Any

_STATE_DIR = Path(__file__).parent / ".state"
_STATE_DIR.mkdir(exist_ok=True)


def _file(symbol: str) -> Path:
    return _STATE_DIR / f"{symbol}.json"


def save_strategy_state(symbol: str, state: Dict[str, Any]) -> None:
    state = dict(state)
    state["_saved_at"] = time.time()
    _file(symbol).write_text(json.dumps(state, indent=2))


def load_strategy_state(symbol: str) -> Dict[str, Any]:
    fp = _file(symbol)
    if not fp.exists():
        return {}
    try:
        return json.loads(fp.read_text())
    except Exception:
        return {}
