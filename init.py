"""
Package stub so   import strategies.base   always resolves.
Only the light-weight Strategy shim lives here; each real strategy
(EMA, SMC, GridBot) remains in backend/.
"""
from .base import Strategy          # re-export for convenience

__all__ = ["Strategy"]
