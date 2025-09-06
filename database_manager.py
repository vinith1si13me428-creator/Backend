# database_manager.py - Local File Storage Manager (No External DB)

import json
import time
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

class DatabaseManager:
    """Local file-based database manager - no external PostgreSQL needed"""
    
    def __init__(self, storage_dir: str = None):
        self.storage_dir = Path(storage_dir or "./.state")
        self.storage_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        (self.storage_dir / "strategies").mkdir(exist_ok=True)
        (self.storage_dir / "trades").mkdir(exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
        
    async def init_database(self):
        """Initialize local storage directories"""
        try:
            # Ensure directories exist
            self.storage_dir.mkdir(exist_ok=True)
            (self.storage_dir / "strategies").mkdir(exist_ok=True)
            (self.storage_dir / "trades").mkdir(exist_ok=True)
            
            self.logger.info("✅ Local file storage initialized successfully")
        except Exception as e:
            self.logger.error(f"❌ Local storage initialization failed: {e}")
            raise

    def _strategy_file(self, symbol: str) -> Path:
        """Get strategy state file path"""
        return self.storage_dir / "strategies" / f"{symbol}.json"

    def _trades_file(self) -> Path:
        """Get trades file path"""
        return self.storage_dir / "trades" / "trades.json"

    # In your strategy class (replace the existing save_state method)
async def save_state(self):
    """Save strategy state using correct async method"""
    try:
        state = {
            'loop_count': getattr(self, 'loop_count', 0),
            'last_signal': getattr(self, 'last_signal', None),
            'last_trade_time': getattr(self, 'last_trade_time', 0),
            'instance_id': getattr(self, 'instance_id', 'unknown'),
            'saved_at': time.time()
        }

        # Use correct method name with await
        if hasattr(self, 'db_manager') and hasattr(self.db_manager, 'save_strategy_state'):
            await self.db_manager.save_strategy_state(self.config.symbol, state)
            print(f"[{self.config.symbol}] ✅ State saved successfully")
        else:
            print(f"[{self.config.symbol}] ⚠️ DatabaseManager missing save_strategy_state method")

    except Exception as e:
        print(f"[{self.config.symbol}] ❌ Save state error: {e}")

    async def load_strategy_state(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Load strategy state from local file"""
        try:
            file_path = self._strategy_file(symbol)
            
            if not file_path.exists():
                return None
                
            data = json.loads(file_path.read_text())
            self.logger.debug(f"✅ Strategy state loaded for {symbol}")
            return data
            
        except Exception as e:
            self.logger.error(f"❌ Failed to load strategy state for {symbol}: {e}")
            return None

    async def save_trade(self, trade_data: Dict[str, Any]):
        """Save trade record to local file"""
        try:
            trades_file = self._trades_file()
            
            # Load existing trades
            trades = []
            if trades_file.exists():
                try:
                    trades = json.loads(trades_file.read_text())
                except:
                    trades = []
            
            # Add new trade with timestamp
            trade_data = dict(trade_data)
            trade_data["timestamp"] = datetime.now().isoformat()
            trade_data["id"] = len(trades) + 1
            
            trades.append(trade_data)
            
            # Keep only last 1000 trades
            if len(trades) > 1000:
                trades = trades[-1000:]
            
            # Save back to file
            trades_file.write_text(json.dumps(trades, indent=2))
            
            self.logger.debug("✅ Trade record saved")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to save trade: {e}")

    async def get_recent_trades(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent trades from local file"""
        try:
            trades_file = self._trades_file()
            
            if not trades_file.exists():
                return []
                
            trades = json.loads(trades_file.read_text())
            
            # Return most recent trades
            return trades[-limit:] if len(trades) > limit else trades
            
        except Exception as e:
            self.logger.error(f"❌ Failed to get recent trades: {e}")
            return []

    async def get_trade_statistics(self) -> Dict[str, Any]:
        """Get trade statistics from local file"""
        try:
            trades = await self.get_recent_trades(limit=10000)  # Get all trades
            
            if not trades:
                return {
                    "total_trades": 0,
                    "winning_trades": 0,
                    "win_rate": 0.0,
                    "total_pnl": 0.0
                }
            
            total_trades = len(trades)
            winning_trades = sum(1 for trade in trades if float(trade.get("pnl", 0)) > 0)
            total_pnl = sum(float(trade.get("pnl", 0)) for trade in trades)
            win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
            
            return {
                "total_trades": total_trades,
                "winning_trades": winning_trades,
                "win_rate": round(win_rate, 2),
                "total_pnl": round(total_pnl, 2)
            }
            
        except Exception as e:
            self.logger.error(f"❌ Failed to get trade statistics: {e}")
            return {
                "total_trades": 0,
                "winning_trades": 0,
                "win_rate": 0.0,
                "total_pnl": 0.0
            }

    async def close(self):
        """Close file storage (no-op for file-based storage)"""
        self.logger.info("✅ Local file storage closed")

    # Additional utility methods
    def get_all_strategy_symbols(self) -> List[str]:
        """Get list of all symbols with saved states"""
        try:
            strategy_dir = self.storage_dir / "strategies"
            return [f.stem for f in strategy_dir.glob("*.json")]
        except:
            return []

    def clear_strategy_state(self, symbol: str) -> bool:
        """Clear strategy state for a symbol"""
        try:
            file_path = self._strategy_file(symbol)
            if file_path.exists():
                file_path.unlink()
                return True
            return False
        except:
            return False

    def backup_data(self, backup_path: str = None) -> bool:
        """Create backup of all data"""
        try:
            import shutil
            
            backup_path = backup_path or f"backup_{int(time.time())}"
            backup_dir = Path(backup_path)
            
            shutil.copytree(self.storage_dir, backup_dir, dirs_exist_ok=True)
            
            self.logger.info(f"✅ Data backed up to {backup_dir}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Backup failed: {e}")
            return False
