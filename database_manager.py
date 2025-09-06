import os
import json
import time
import logging
import psycopg
from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager

class DatabaseManager:
    """Database manager that supports both PostgreSQL and local files"""

    def __init__(self, storage_dir: str = None, force_local: bool = False):
        self.logger = logging.getLogger(__name__)
        
        # Check if PostgreSQL should be used
        self.database_url = os.getenv("DATABASE_URL")
        self.use_postgres = (not force_local and 
                           self.database_url is not None and 
                           self._test_postgres_connection())
        
        if self.use_postgres:
            self.logger.info("🐘 PostgreSQL DATABASE_URL found - will use external database")
        else:
            # Fallback to local file storage
            self.storage_dir = Path(storage_dir or "./.state")
            self.storage_dir.mkdir(exist_ok=True)
            (self.storage_dir / "strategies").mkdir(exist_ok=True)
            (self.storage_dir / "trades").mkdir(exist_ok=True)
            self.logger.info("💾 Using local file storage fallback")

    def _test_postgres_connection(self) -> bool:
        """Test PostgreSQL connection availability"""
        try:
            if not self.database_url:
                return False
            with psycopg.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    return True
        except Exception as e:
            self.logger.warning(f"⚠️ PostgreSQL connection test failed: {e}")
            return False

    @contextmanager
    def get_db_connection(self):
        """Get database connection with error handling"""
        if not self.use_postgres:
            raise Exception("PostgreSQL not available")
            
        try:
            conn = psycopg.connect(self.database_url)
            yield conn
        except Exception as e:
            self.logger.error(f"❌ Database connection error: {e}")
            raise
        finally:
            try:
                conn.close()
            except:
                pass

    async def init_database(self):
        """Initialize database connection and tables"""
        if self.use_postgres:
            try:
                with self.get_db_connection() as conn:
                    with conn.cursor() as cur:
                        # Strategy states table
                        cur.execute("""
                            CREATE TABLE IF NOT EXISTS strategy_states (
                                symbol VARCHAR(20) PRIMARY KEY,
                                state_data JSONB NOT NULL,
                                created_at TIMESTAMP DEFAULT NOW(),
                                updated_at TIMESTAMP DEFAULT NOW()
                            )
                        """)
                        
                        # Trades table
                        cur.execute("""
                            CREATE TABLE IF NOT EXISTS trades (
                                id SERIAL PRIMARY KEY,
                                symbol VARCHAR(20),
                                side VARCHAR(10),
                                size DECIMAL,
                                entry_price DECIMAL,
                                exit_price DECIMAL,
                                sl_price DECIMAL,
                                tp_price DECIMAL,
                                leverage INTEGER,
                                profit_loss DECIMAL,
                                strategy VARCHAR(50),
                                status VARCHAR(20),
                                instance_id VARCHAR(50),
                                trade_data JSONB,
                                created_at TIMESTAMP DEFAULT NOW()
                            )
                        """)
                        
                        # Create indexes
                        cur.execute("CREATE INDEX IF NOT EXISTS idx_strategy_states_symbol ON strategy_states(symbol)")
                        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)")
                        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_created_at ON trades(created_at)")
                        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy)")
                    
                    conn.commit()
                    self.logger.info("✅ PostgreSQL database initialized successfully")
                    
            except Exception as e:
                self.logger.error(f"❌ PostgreSQL initialization failed: {e}")
                self.logger.info("🔄 Falling back to local storage")
                self.use_postgres = False
                # Initialize local storage
                self.storage_dir = Path("./.state")
                self.storage_dir.mkdir(exist_ok=True)
                (self.storage_dir / "strategies").mkdir(exist_ok=True)
                (self.storage_dir / "trades").mkdir(exist_ok=True)
        
        if not self.use_postgres:
            self.logger.info("✅ Local file storage initialized")

    def _strategy_file(self, symbol: str) -> Path:
        """Get strategy state file path (for local storage)"""
        return self.storage_dir / "strategies" / f"{symbol}.json"

    def _trades_file(self) -> Path:
        """Get trades file path (for local storage)"""
        return self.storage_dir / "trades" / "trades.json"

    async def save_strategy_state(self, symbol: str, state_data: Dict[str, Any]):
        """Save strategy state to PostgreSQL or local file"""
        try:
            # Add metadata
            state_data = dict(state_data)
            state_data["_saved_at"] = time.time()
            state_data["_updated_at"] = datetime.now().isoformat()

            if self.use_postgres:
                # Save to PostgreSQL
                with self.get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO strategy_states (symbol, state_data, updated_at) 
                            VALUES (%s, %s, NOW())
                            ON CONFLICT (symbol) DO UPDATE SET 
                            state_data = EXCLUDED.state_data, updated_at = NOW()
                        """, (symbol, json.dumps(state_data)))
                    conn.commit()
                    
                self.logger.debug(f"✅ Strategy state saved to PostgreSQL for {symbol}")
            else:
                # Save to local file
                file_path = self._strategy_file(symbol)
                file_path.write_text(json.dumps(state_data, indent=2))
                self.logger.debug(f"✅ Strategy state saved locally for {symbol}")

        except Exception as e:
            self.logger.error(f"❌ Failed to save strategy state for {symbol}: {e}")
            # Don't raise - let strategy continue

    async def load_strategy_state(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Load strategy state from PostgreSQL or local file"""
        try:
            if self.use_postgres:
                # Load from PostgreSQL
                with self.get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT state_data FROM strategy_states WHERE symbol = %s", (symbol,))
                        result = cur.fetchone()
                        
                        if result:
                            state_data = result[0]
                            # Check if already a dict (from JSONB) or needs JSON parsing
                            if isinstance(state_data, dict):
                                return state_data  # Already parsed by psycopg
                            else:
                                return json.loads(state_data)  # Parse if string
                        return None
            else:
                # Load from local file
                file_path = self._strategy_file(symbol)
                if not file_path.exists():
                    return None
                
                data = json.loads(file_path.read_text())
                self.logger.debug(f"✅ Strategy state loaded locally for {symbol}")
                return data

        except Exception as e:
            self.logger.error(f"❌ Failed to load strategy state for {symbol}: {e}")
            return None

    async def save_trade(self, trade_data: Dict[str, Any]):
        """Save trade record to PostgreSQL or local file"""
        try:
            if self.use_postgres:
                # Save to PostgreSQL
                with self.get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO trades 
                            (symbol, side, size, entry_price, exit_price, sl_price, tp_price, leverage, 
                             profit_loss, strategy, status, instance_id, trade_data, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            trade_data.get("symbol"),
                            trade_data.get("side"),
                            float(trade_data.get("size", 0)) if trade_data.get("size") else None,
                            float(trade_data.get("entry_price", 0)) if trade_data.get("entry_price") else None,
                            float(trade_data.get("exit_price", 0)) if trade_data.get("exit_price") else None,
                            float(trade_data.get("sl_price", 0)) if trade_data.get("sl_price") else None,
                            float(trade_data.get("tp_price", 0)) if trade_data.get("tp_price") else None,
                            int(trade_data.get("leverage", 1)) if trade_data.get("leverage") else None,
                            float(trade_data.get("profit_loss", 0)) if trade_data.get("profit_loss") else None,
                            trade_data.get("strategy"),
                            trade_data.get("status"),
                            trade_data.get("instance_id"),
                            json.dumps(trade_data),
                            datetime.now()
                        ))
                    conn.commit()
                    
                self.logger.debug("✅ Trade record saved to PostgreSQL")
            else:
                # Save to local file (existing implementation)
                trades_file = self._trades_file()
                
                trades = []
                if trades_file.exists():
                    try:
                        trades = json.loads(trades_file.read_text())
                    except:
                        trades = []

                trade_data = dict(trade_data)
                trade_data["timestamp"] = datetime.now().isoformat()
                trade_data["id"] = len(trades) + 1
                trades.append(trade_data)

                if len(trades) > 1000:
                    trades = trades[-1000:]

                trades_file.write_text(json.dumps(trades, indent=2))
                self.logger.debug("✅ Trade record saved locally")

        except Exception as e:
            self.logger.error(f"❌ Failed to save trade: {e}")

    async def get_recent_trades(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent trades from PostgreSQL or local file"""
        try:
            if self.use_postgres:
                # Get from PostgreSQL
                with self.get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT * FROM trades 
                            ORDER BY created_at DESC 
                            LIMIT %s
                        """, (limit,))
                        
                        trades = cur.fetchall()
                        if trades:
                            # Get column names
                            columns = [desc[0] for desc in cur.description]
                            trade_records = []
                            
                            for trade in trades:
                                trade_dict = dict(zip(columns, trade))
                                
                                # Use the complete trade_data if available, otherwise build from columns
                                if trade_dict.get("trade_data"):
                                    if isinstance(trade_dict["trade_data"], dict):
                                        trade_record = trade_dict["trade_data"]
                                    else:
                                        trade_record = json.loads(trade_dict["trade_data"])
                                else:
                                    trade_record = {
                                        "symbol": trade_dict["symbol"],
                                        "side": trade_dict["side"],
                                        "size": float(trade_dict["size"]) if trade_dict["size"] else None,
                                        "entry_price": float(trade_dict["entry_price"]) if trade_dict["entry_price"] else None,
                                        "exit_price": float(trade_dict["exit_price"]) if trade_dict["exit_price"] else None,
                                        "sl_price": float(trade_dict["sl_price"]) if trade_dict["sl_price"] else None,
                                        "tp_price": float(trade_dict["tp_price"]) if trade_dict["tp_price"] else None,
                                        "leverage": int(trade_dict["leverage"]) if trade_dict["leverage"] else None,
                                        "profit_loss": float(trade_dict["profit_loss"]) if trade_dict["profit_loss"] else None,
                                        "strategy": trade_dict["strategy"],
                                        "status": trade_dict["status"],
                                        "instance_id": trade_dict["instance_id"]
                                    }
                                
                                trade_record["id"] = trade_dict["id"]
                                trade_record["timestamp"] = trade_dict["created_at"].isoformat() if trade_dict["created_at"] else None
                                trade_records.append(trade_record)
                            
                            return trade_records
                        else:
                            return []
            else:
                # Get from local file (existing implementation)
                trades_file = self._trades_file()
                if not trades_file.exists():
                    return []

                trades = json.loads(trades_file.read_text())
                return trades[-limit:] if len(trades) > limit else trades

        except Exception as e:
            self.logger.error(f"❌ Failed to get recent trades: {e}")
            return []

    async def get_trade_statistics(self) -> Dict[str, Any]:
        """Get trade statistics from PostgreSQL or local file"""
        try:
            if self.use_postgres:
                # Get all trades from PostgreSQL
                with self.get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT profit_loss FROM trades WHERE profit_loss IS NOT NULL")
                        results = cur.fetchall()
                        
                        if results:
                            profit_losses = [float(row[0]) for row in results]
                            total_trades = len(profit_losses)
                            winning_trades = sum(1 for pnl in profit_losses if pnl > 0)
                            total_pnl = sum(profit_losses)
                            win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0

                            return {
                                "total_trades": total_trades,
                                "winning_trades": winning_trades,
                                "win_rate": round(win_rate, 2),
                                "total_pnl": round(total_pnl, 2)
                            }
                        else:
                            return {
                                "total_trades": 0,
                                "winning_trades": 0,
                                "win_rate": 0.0,
                                "total_pnl": 0.0
                            }
            else:
                # Get from local file (existing implementation)
                trades = await self.get_recent_trades(limit=10000)
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
        """Close database connection"""
        if self.use_postgres:
            self.logger.info("✅ PostgreSQL connection closed")
        else:
            self.logger.info("✅ Local file storage closed")

# Test connection on import
def test_connection():
    """Test database connection"""
    try:
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            with psycopg.connect(database_url) as conn:
                print("✅ PostgreSQL database connection successful")
                return True
        else:
            print("⚠️ DATABASE_URL not found, will use local storage")
            return False
    except Exception as e:
        print(f"❌ PostgreSQL database connection failed: {e}")
        return False

# Test on import
test_connection()
