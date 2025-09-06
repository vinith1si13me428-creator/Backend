# main_app.py - Complete Modular FastAPI Trading Bot Application

import os
import asyncio
import time
import sys
import importlib
import inspect
import signal
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import logging
from typing import Dict, Any, List, Optional

# Import our modular components
from delta_client import DeltaAPIClient, DeltaCredentials
from strategy_engine import StrategyManager, StrategyConfig
from ema_strategy import EMAStrategy
from risk_manager import RiskManager
from order_executor import OrderExecutor
from websocket_manager import DeltaWebSocketManager

try:
    import psycopg
    from contextlib import contextmanager
    PSYCOPG_AVAILABLE = True
except ImportError:
    PSYCOPG_AVAILABLE = False
    print("⚠️ psycopg not available - PostgreSQL features disabled")
    
# Import current directory strategies (will be moved to strategies folder)
try:
    from SMC_Strategy import SMC_Strategy
    from GridBot_Strategy import GridBot_Strategy
    CURRENT_STRATEGIES_AVAILABLE = True
except ImportError:
    SMC_Strategy = None
    GridBot_Strategy = None
    CURRENT_STRATEGIES_AVAILABLE = False
    print("⚠️ Current directory strategies not found")

# Base strategy class import
try:
    from strategies.base import Strategy as BaseStrategy
    BASE_STRATEGY_AVAILABLE = True
except ImportError:
    try:
        from strategy_engine import BaseStrategy
        BASE_STRATEGY_AVAILABLE = True
    except ImportError:
        BaseStrategy = object
        BASE_STRATEGY_AVAILABLE = False

# Optional new components
try:
    from portfolio_manager import PortfolioManager, PositionRisk
    PORTFOLIO_MANAGER_AVAILABLE = True
except ImportError:
    PORTFOLIO_MANAGER_AVAILABLE = False
    print("⚠️ portfolio_manager.py not found - portfolio features disabled")

try:
    from health_monitor import SystemHealthMonitor
    HEALTH_MONITOR_AVAILABLE = True
except ImportError:
    HEALTH_MONITOR_AVAILABLE = False
    print("⚠️ health_monitor.py not found - health monitoring disabled")

# Optional dependencies
try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded
    RATE_LIMITING_AVAILABLE = True
except ImportError:
    RATE_LIMITING_AVAILABLE = False
    print("⚠️ slowapi not available - rate limiting disabled")

try:
    import redis
    from fastapi_cache import FastAPICache
    from fastapi_cache.backends.redis import RedisBackend
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("⚠️ Redis not available - caching disabled")

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("⚠️ psutil not available - system metrics disabled")

# Load environment variables
load_dotenv()

# Local Storage Database Manager Class (No External DB Dependencies)

# Create FastAPI app
app = FastAPI(title="VY Trading Bot API")


import json
from datetime import datetime

class DatabaseManager:
    """Database manager that supports both PostgreSQL and local files as fallback"""

    def __init__(self, storage_dir: str = None, force_local: bool = False):
        self.logger = logging.getLogger(__name__)
        
        # Check if PostgreSQL should be used
        self.database_url = os.getenv("DATABASE_URL")
        self.use_postgres = (not force_local and 
                           self.database_url is not None and 
                           self._test_postgres_connection())
        
        if self.use_postgres:
            self.logger.info("🐘 PostgreSQL DATABASE_URL found - using external database")
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
                                pnl DECIMAL,
                                trade_data JSONB,
                                created_at TIMESTAMP DEFAULT NOW()
                            )
                        """)
                        
                        # Create indexes for better performance
                        cur.execute("CREATE INDEX IF NOT EXISTS idx_strategy_states_symbol ON strategy_states(symbol)")
                        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)")
                        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_created_at ON trades(created_at)")
                        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy)")
                        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_pnl ON trades(pnl)")
                    
                    conn.commit()
                    self.logger.info("✅ PostgreSQL database initialized successfully")
                    
            except Exception as e:
                self.logger.error(f"❌ PostgreSQL initialization failed: {e}")
                self.logger.info("🔄 Falling back to local storage")
                self.use_postgres = False
                # Initialize local storage fallback
                self.storage_dir = Path("./.state")
                self.storage_dir.mkdir(exist_ok=True)
                (self.storage_dir / "strategies").mkdir(exist_ok=True)
                (self.storage_dir / "trades").mkdir(exist_ok=True)
        
        if not self.use_postgres:
            self.logger.info("✅ Local file storage initialized")

    def _strategy_file(self, symbol: str) -> Path:
        """Get strategy state file path (for local storage fallback)"""
        return self.storage_dir / "strategies" / f"{symbol}.json"

    def _trades_file(self) -> Path:
        """Get trades file path (for local storage fallback)"""
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
                                return state_data
                            else:
                                return json.loads(state_data)
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
                             profit_loss, strategy, status, instance_id, pnl, trade_data, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                            float(trade_data.get("pnl", 0)) if trade_data.get("pnl") else None,
                            json.dumps(trade_data),
                            datetime.now()
                        ))
                    conn.commit()
                    
                self.logger.debug("✅ Trade record saved to PostgreSQL")
            else:
                # Save to local file (fallback)
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
                                        "instance_id": trade_dict["instance_id"],
                                        "pnl": float(trade_dict["pnl"]) if trade_dict["pnl"] else None
                                    }
                                
                                trade_record["id"] = trade_dict["id"]
                                trade_record["timestamp"] = trade_dict["created_at"].isoformat() if trade_dict["created_at"] else None
                                trade_records.append(trade_record)
                            
                            return trade_records
                        else:
                            return []
            else:
                # Get from local file
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
                # Get statistics from PostgreSQL
                with self.get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pnl FROM trades WHERE pnl IS NOT NULL")
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
                # Get from local file
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

    # Additional utility methods
    def get_all_strategy_symbols(self) -> List[str]:
        """Get list of all symbols with saved states"""
        try:
            if self.use_postgres:
                with self.get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT symbol FROM strategy_states")
                        results = cur.fetchall()
                        return [row[0] for row in results]
            else:
                strategy_dir = self.storage_dir / "strategies"
                return [f.stem for f in strategy_dir.glob("*.json")]
        except:
            return []

    def clear_strategy_state(self, symbol: str) -> bool:
        """Clear strategy state for a symbol"""
        try:
            if self.use_postgres:
                with self.get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM strategy_states WHERE symbol = %s", (symbol,))
                    conn.commit()
                    return True
            else:
                file_path = self._strategy_file(symbol)
                if file_path.exists():
                    file_path.unlink()
                    return True
                return False
        except:
            return False

# Global components
delta_client: DeltaAPIClient = None
strategy_manager: StrategyManager = None
risk_manager: RiskManager = None
order_executor: OrderExecutor = None
db_manager: DatabaseManager = None
ws_manager: DeltaWebSocketManager = None
portfolio_manager = None
health_monitor = None
ACTIVE_STRATEGIES = {} 

# Configuration
TRACKED_SYMBOLS = ["BTCUSD", "ETHUSD", "XRPUSD", "SOLUSD"]
PRODUCTS_CACHE = {}
SYMBOL_TO_PID = {}

# Strategy registry for dynamic discovery
discovered_strategies = {}

# Global shutdown event
shutdown_event = asyncio.Event()

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    print(f"\n🛑 Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def discover_strategies_from_folder():
    """Automatically discover strategies from ./strategies folder"""
    global discovered_strategies
    
    # Clear existing discovered strategies
    discovered_strategies.clear()
    
    # Check strategies folder
    strategies_dir = Path(__file__).parent / "strategies"
    if not strategies_dir.exists():
        print(f"⚠️ Strategies directory not found: {strategies_dir}")
        print("📁 Creating strategies directory...")
        strategies_dir.mkdir(exist_ok=True)
        
        # Create __init__.py in strategies directory
        init_file = strategies_dir / "__init__.py"
        if not init_file.exists():
            init_file.write_text('"""Strategies package"""\n')
        
        return discovered_strategies

    # Add strategies directory to Python path
    if str(strategies_dir) not in sys.path:
        sys.path.insert(0, str(strategies_dir))
    
    print(f"🔍 Discovering strategies in: {strategies_dir}")
    
    # Discover strategy files
    strategy_files = list(strategies_dir.glob("*.py"))
    strategy_files = [f for f in strategy_files if f.name not in ["__init__.py", "base.py"]]
    
    print(f"📁 Found {len(strategy_files)} strategy files: {[f.stem for f in strategy_files]}")
    
    for strategy_file in strategy_files:
        module_name = strategy_file.stem
        try:
            # Import the module
            if module_name in sys.modules:
                # Reload if already imported
                module = importlib.reload(sys.modules[module_name])
            else:
                module = importlib.import_module(module_name)
            
            # Find strategy classes in the module
            for name, obj in inspect.getmembers(module, inspect.isclass):
                # Check if it's a strategy class (not the base class itself)
                if (hasattr(obj, '__module__') and
                    obj.__module__ == module_name and
                    name not in ['Strategy', 'BaseStrategy'] and
                    (BASE_STRATEGY_AVAILABLE and issubclass(obj, BaseStrategy) or
                     hasattr(obj, 'run') and hasattr(obj, 'start') and hasattr(obj, 'stop'))):
                    
                    discovered_strategies[name] = obj
                    print(f"✅ Discovered strategy: {name} from {module_name}.py")
                    
        except Exception as e:
            print(f"❌ Failed to import strategy from {strategy_file.name}: {e}")
    
    print(f"🎯 Successfully discovered {len(discovered_strategies)} strategies")
    return discovered_strategies

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management with comprehensive initialization"""
    global delta_client, strategy_manager, risk_manager, order_executor, db_manager, ws_manager, portfolio_manager, health_monitor
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("TradingBot")
    
    try:
        logger.info("🚀 Initializing VY Trading Bot...")
        
        # Initialize credentials
        api_key = os.getenv("DELTA_API_KEY")
        api_secret = os.getenv("DELTA_API_SECRET")
        
        if not api_key or not api_secret:
            raise ValueError("DELTA_API_KEY and DELTA_API_SECRET environment variables are required")
        
        credentials = DeltaCredentials(
            api_key=api_key,
            api_secret=api_secret
        )
        
        # Initialize core components
        logger.info("📡 Initializing Delta API client...")
        delta_client = DeltaAPIClient(credentials)
        
        logger.info("🗄️ Initializing database manager (POSTGRESQLLOCAL STORAGE)...")
        db_manager = DatabaseManager()  # This now uses local files
        await db_manager.init_database()
        
        logger.info("🛡️ Initializing risk manager...")
        risk_manager = RiskManager()
        
        logger.info("⚡ Initializing order executor...")
        order_executor = OrderExecutor(delta_client, db_manager)
        
        logger.info("🔌 Initializing WebSocket manager...")
        ws_manager = DeltaWebSocketManager(credentials.api_key, credentials.api_secret)
        
        # Initialize optional components
        if PORTFOLIO_MANAGER_AVAILABLE:
            logger.info("💼 Initializing portfolio manager...")
            portfolio_manager = PortfolioManager(max_portfolio_risk=0.05)
        
        # Initialize strategy manager
        logger.info("🎯 Initializing strategy manager...")
        strategy_manager = StrategyManager(delta_client, risk_manager, order_executor, db_manager)
        
        # Register built-in strategies
        strategy_manager.register_strategy("ema_strategy", EMAStrategy)
        logger.info("✅ Registered EMA strategy")
        
        # Register current directory strategies (legacy support)
        if CURRENT_STRATEGIES_AVAILABLE:
            if SMC_Strategy:
                strategy_manager.register_strategy("smc_strategy", SMC_Strategy)
                strategy_manager.register_strategy("SMC_Strategy", SMC_Strategy)
                logger.info("✅ Registered SMC_Strategy")
            if GridBot_Strategy:
                strategy_manager.register_strategy("gridbot_strategy", GridBot_Strategy)
                strategy_manager.register_strategy("GridBot_Strategy", GridBot_Strategy)
                logger.info("✅ Registered GridBot_Strategy")
        
        # Discover strategies from ./strategies folder
        logger.info("🔍 Auto-discovering strategies from ./strategies folder...")
        discovered = discover_strategies_from_folder()
        
        for strategy_name, strategy_class in discovered.items():
            try:
                if hasattr(strategy_manager, 'register_strategy'):
                    clean_name = strategy_name.lower().replace('_', '_')
                    if clean_name not in strategy_manager.strategy_registry:
                       strategy_manager.register_strategy(clean_name.lower(), strategy_class)
                       logger.info(f"✅ Registered discovered strategy: {strategy_name} as {clean_name}")
            except Exception as e:
                logger.error(f"❌ Failed to register strategy {strategy_name}: {e}")
        
        # Initialize health monitor after other components
        if HEALTH_MONITOR_AVAILABLE:
            logger.info("🏥 Initializing health monitor...")
            health_monitor = SystemHealthMonitor(delta_client, db_manager, strategy_manager)
        
        # Initialize Redis cache (optional)
        if REDIS_AVAILABLE:
            try:
                logger.info("🔄 Connecting to Redis cache...")
                redis_client = redis.from_url(
                    os.getenv("REDIS_URL", "redis://localhost:6379"),
                    encoding="utf8",
                    decode_responses=True
                )
                FastAPICache.init(RedisBackend(redis_client), prefix="vy-trading")
                logger.info("✅ Redis cache initialized")
            except Exception as e:
                logger.warning(f"⚠️ Redis connection failed: {e}")
        
        # Load products cache
        logger.info("📦 Loading products cache...")
        await load_products_cache()
        
        logger.info("🎉 All components initialized successfully!")
        logger.info(f"📊 Tracking symbols: {list(SYMBOL_TO_PID.keys())}")
        logger.info(f"🎯 Available strategies: {len(strategy_manager.strategy_registry)}")
        # After all registrations, log what's available
        logger.info("🎯 Strategy Registration Summary:")
        for name, strategy_class in strategy_manager.strategy_registry.items():
            logger.info(f"  ✅ {name} -> {strategy_class.__name__}")

        logger.info(f"📊 Total registered strategies: {len(strategy_manager.strategy_registry)}")
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize components: {e}")
        raise
    
    yield
    
    # Cleanup phase
    logger.info("🔄 Starting application cleanup...")
    try:
        # Stop all active strategies with timeout
        if strategy_manager and hasattr(strategy_manager, 'active_strategies'):
            stop_tasks = []
            for symbol in list(strategy_manager.active_strategies.keys()):
                task = asyncio.create_task(strategy_manager.stop_strategy(symbol))
                stop_tasks.append(task)
            
            if stop_tasks:
                try:
                    # Wait for all strategies to stop with timeout
                    await asyncio.wait_for(asyncio.gather(*stop_tasks, return_exceptions=True), timeout=15.0)
                    logger.info("✅ All strategies stopped")
                except asyncio.TimeoutError:
                    logger.warning("⚠️ Strategy shutdown timeout - some strategies may not have stopped cleanly")

        # Disconnect all WebSocket connections with timeout
        if ws_manager:
            disconnect_tasks = []
            for symbol in TRACKED_SYMBOLS:
                if hasattr(ws_manager, 'is_connected') and ws_manager.is_connected(symbol):
                    task = asyncio.create_task(ws_manager.disconnect_symbol(symbol))
                    disconnect_tasks.append(task)
            
            if disconnect_tasks:
                try:
                    await asyncio.wait_for(asyncio.gather(*disconnect_tasks, return_exceptions=True), timeout=10.0)
                    logger.info("✅ All WebSocket connections closed")
                except asyncio.TimeoutError:
                    logger.warning("⚠️ WebSocket disconnect timeout")

        # Close database connections (no-op for file storage)
        if db_manager and hasattr(db_manager, 'close'):
            try:
                await db_manager.close()
                logger.info("✅ Database manager closed")
            except Exception as e:
                logger.error(f"❌ Database close error: {e}")

        logger.info("✅ Cleanup completed successfully")

    except Exception as e:
        logger.error(f"❌ Cleanup error: {e}")

def safe_float_conversion(value, default=0.0):
    """Safely convert various formats to float"""
    if value is None:
        return default
    str_value = str(value).strip()
    if not str_value:
        return default
    try:
        # Handle percentage strings
        if str_value.endswith('%'):
            return float(str_value[:-1]) / 100.0
        # Remove common formatting
        cleaned = str_value.replace(',', '').replace('$', '').replace(' ', '')
        return float(cleaned)
    except (ValueError, TypeError):
        print(f"⚠️ Float conversion failed for '{value}', using default {default}")
        return default

async def load_products_cache():
    """Load products and create symbol mapping"""
    global PRODUCTS_CACHE, SYMBOL_TO_PID
    try:
        products = await delta_client.get_products()
        PRODUCTS_CACHE.clear()
        SYMBOL_TO_PID.clear()
        
        for product in products:
            PRODUCTS_CACHE[product["id"]] = product
            if product["symbol"] in TRACKED_SYMBOLS:
                SYMBOL_TO_PID[product["symbol"]] = product["id"]
        
        print(f"✅ Loaded {len(PRODUCTS_CACHE)} products")
        print(f"📊 Tracking symbols: {list(SYMBOL_TO_PID.keys())}")
    except Exception as e:
        print(f"❌ Failed to load products: {e}")

# Create FastAPI app AFTER all imports
app = FastAPI(
    title="VY Trading Bot - Professional Edition",
    description="Advanced modular cryptocurrency trading system with multi-strategy support",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configure rate limiting (optional)
if RATE_LIMITING_AVAILABLE:
    limiter = Limiter(key_func=get_remote_address)
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# ========================================
# API ENDPOINTS
# ========================================

@app.get("/api/strategies/debug")
async def debug_strategies():
    """Debug endpoint to check strategy registration"""
    return {
        "success": True,
        "registered_strategies": list(strategy_manager.strategy_registry.keys()),
        "registry_details": {name: str(cls) for name, cls in strategy_manager.strategy_registry.items()},
        "imports": {
            "SMC_Strategy": SMC_Strategy is not None,
            "GridBot_Strategy": GridBot_Strategy is not None,
            "CURRENT_STRATEGIES_AVAILABLE": CURRENT_STRATEGIES_AVAILABLE
        },
        "smc_class": str(strategy_manager.strategy_registry.get("SMC_Strategy", "Not found")),
        "smc_lowercase": str(strategy_manager.strategy_registry.get("smc_strategy", "Not found"))
    }

@app.get("/api/strategies/health")
async def get_strategies_health():
    """Health check for all running strategies with accurate status"""
    try:
        health_report = {
            "total_active": len(strategy_manager.active_strategies),
            "running_strategies": [],
            "stopped_strategies": [],
            "timestamp": time.time()
        }
        
        for symbol, strategy in strategy_manager.active_strategies.items():
            strategy_info = {
                "symbol": symbol,
                "strategy_type": strategy.__class__.__name__,
                "running": False,
                "state": "unknown"
            }
            
            # Check if strategy is actually running
            if hasattr(strategy, 'is_running'):
                strategy_info["running"] = strategy.is_running
            elif hasattr(strategy, 'state'):
                state_value = strategy.state.value if hasattr(strategy.state, 'value') else strategy.state
                strategy_info["running"] = state_value == "running"
                strategy_info["state"] = state_value
            
            if strategy_info["running"]:
                health_report["running_strategies"].append(strategy_info)
            else:
                health_report["stopped_strategies"].append(strategy_info)
        
        return health_report
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/candles/{symbol}")
async def get_candles(symbol: str, timeframe: str = "1m", limit: int = 50):
    """Get real candlestick data for symbol"""
    try:
        # Get product ID for the symbol
        product_id = SYMBOL_TO_PID.get(symbol)
        if not product_id:
            raise HTTPException(status_code=404, detail=f"Product ID not found for symbol: {symbol}")
        
        print(f"📊 Fetching candles for {symbol} (ID: {product_id})")
        
        # Fetch real candle data from Delta Exchange
        candles_data = await delta_client.get_candles(symbol=symbol, timeframe=timeframe, limit=limit)
        
        if not candles_data or not isinstance(candles_data, list):
            raise HTTPException(status_code=404, detail="No candle data available")

        # Process and format candle data
        processed_candles = []
        for candle in candles_data:
            try:
                processed_candle = {
                    "timestamp": int(candle.get("time", 0)) * 1000,  # Convert to milliseconds
                    "open": safe_float_conversion(candle.get("open", 0)),
                    "high": safe_float_conversion(candle.get("high", 0)),
                    "low": safe_float_conversion(candle.get("low", 0)),
                    "close": safe_float_conversion(candle.get("close", 0)),
                    "volume": safe_float_conversion(candle.get("volume", 0))
                }
                processed_candles.append(processed_candle)
            except Exception as e:
                print(f"⚠️ Skipping invalid candle data: {e}")
                continue

        print(f"✅ Processed {len(processed_candles)} candles for {symbol}")
        return {
            "success": True,
            "symbol": symbol,
            "timeframe": timeframe,
            "candles": processed_candles,
            "count": len(processed_candles)
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Candles fetch error for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def root():
    """Root endpoint with system information"""
    return {
        "message": "🚀 VY Trading Bot - Professional Edition",
        "version": "2.0.0",
        "status": "operational",
        "docs": "/docs",
        "redoc": "/redoc",
        "features": [
            "Modular Architecture",
            "Auto Strategy Discovery",
            "Professional Risk Management",
            "Delta Exchange Integration",
            "Real-time WebSocket Streaming",
            "Advanced Trading Strategies",
            "Local File Storage",
            "Health Monitoring" if HEALTH_MONITOR_AVAILABLE else None,
            "Portfolio Management" if PORTFOLIO_MANAGER_AVAILABLE else None,
            "Rate Limiting" if RATE_LIMITING_AVAILABLE else None,
            "Redis Caching" if REDIS_AVAILABLE else None
        ],
        "tracked_symbols": TRACKED_SYMBOLS,
        "strategies_discovered": len(discovered_strategies),
        "components": {
            "delta_client": "initialized" if delta_client else "not_initialized",
            "strategy_manager": "initialized" if strategy_manager else "not_initialized",
            "database": "local_storage" if db_manager else "not_initialized",
            "websocket": "initialized" if ws_manager else "not_initialized",
            "portfolio_manager": "available" if PORTFOLIO_MANAGER_AVAILABLE else "not_available",
            "health_monitor": "available" if HEALTH_MONITOR_AVAILABLE else "not_available"
        }
    }

@app.get("/api/health")
async def health_check():
    """Comprehensive system health check"""
    try:
        # Use the global db_manager instance instead of creating new one
        global db_manager
        
        # Basic health status
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "active_strategies": len(strategy_manager.active_strategies) if strategy_manager else 0,
            "tracked_symbols": TRACKED_SYMBOLS,  # Use the correct variable name
            "components": {
                "database": {
                    "status": "connected",
                    "type": "postgresql"  if db_manager.use_postgres else "local_storage"  # ✅ This will work now
                }
            },
            "enhanced_features": {
                "auto_mode": True,
                "manual_mode": True,
                "professional_risk_management": True,
                "ema_period_flexibility": True,
                "local_storage": True
            }
        }

        # Check Delta API
        try:
            if delta_client:
                balance = await delta_client.get_account_balance()
                health_status["components"]["delta_api"] = {
                    "status": "connected",
                    "account_balance": balance
                }
                health_status["account_balance"] = balance
            else:
                health_status["components"]["delta_api"] = {
                    "status": "not_initialized"
                }
        except Exception as e:
            health_status["components"]["delta_api"] = {
                "status": "error",
                "error": str(e)
            }
            health_status["status"] = "degraded"

        # Check database (local file storage)
        try:
            if db_manager:
                test_data = {"health_check": True, "timestamp": time.time()}
                await db_manager.save_strategy_state("_health_test", test_data)
                health_status["components"]["database"]["status"] = "connected"
                health_status["components"]["database"]["type"] = "local_storage"
            else:
                health_status["components"]["database"]["status"] = "not_initialized"
        except Exception as e:
            health_status["components"]["database"] = {
                "status": "error",
                "error": str(e)
            }
            health_status["status"] = "degraded"

        # Check strategy manager
        if strategy_manager:
            health_status["components"]["strategy_manager"] = {
                "status": "ready",
                "active_strategies": len(strategy_manager.active_strategies),
                "registered_strategies": len(strategy_manager.strategy_registry)
            }

        # Check WebSocket manager
        if ws_manager:
            health_status["components"]["websocket_manager"] = {"status": "ready"}

        # Optional component checks
        if PORTFOLIO_MANAGER_AVAILABLE:
            health_status["components"]["portfolio_manager"] = {"status": "available"}
        if HEALTH_MONITOR_AVAILABLE:
            health_status["components"]["health_monitor"] = {"status": "available"}

        return health_status

    except Exception as e:
        print(f"❌ Health check error: {e}")
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

@app.get("/api/strategies/available")
async def get_available_strategies():
    """Get list of all available strategies"""
    try:
        # Refresh strategy discovery
        discovered = discover_strategies_from_folder()
        # Get all registered strategies from strategy manager
        built_in_strategies = ["ema_strategy"]  # Core built-in strategies
        
        # Get current directory strategies that are registered
        current_dir_strategies = []
        if CURRENT_STRATEGIES_AVAILABLE:
            if SMC_Strategy and "SMC_Strategy" in strategy_manager.strategy_registry:
                current_dir_strategies.append("SMC_Strategy")
            if GridBot_Strategy and "GridBot_Strategy" in strategy_manager.strategy_registry:
                current_dir_strategies.append("GridBot_Strategy")
        
        # Get discovered strategies from folder
        discovered_strategies = list(discovered.keys())
        
        available_strategies = {
            "built_in": built_in_strategies,
            "discovered": discovered_strategies,
            "current_directory": current_dir_strategies
        }
       
        return {
            "success": True,
            "result": available_strategies
        }

    except Exception as e:
        print(f"Available strategies error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/strategies/start")
async def start_strategy_endpoint(request: Request):
    """Start a trading strategy"""
    try:
        data = await request.json()
        symbol = data.get("symbol")
        strategy_name = data.get("strategy_name", "ema_strategy")
        
        print(f"🎯 Starting strategy: {strategy_name} for {symbol}")

        if symbol not in TRACKED_SYMBOLS:
            raise HTTPException(status_code=400, detail=f"Invalid symbol. Must be one of: {TRACKED_SYMBOLS}")

        if not SYMBOL_TO_PID.get(symbol):
            raise HTTPException(status_code=400, detail=f"Product ID not found for symbol: {symbol}")

        # Check if strategy exists in registry
        if strategy_name not in strategy_manager.strategy_registry:
            available = list(strategy_manager.strategy_registry.keys())
            print(f"❌ Strategy '{strategy_name}' not found. Available: {available}")
            raise HTTPException(
                status_code=400, 
                detail=f"Unknown strategy: {strategy_name}. Available strategies: {available}"
            )

        # Get account balance
        account_balance = await delta_client.get_account_balance()

        # Handle strategy starting based on type
        if strategy_name == "ema_strategy":
            # Use strategy manager for EMA
            config = StrategyConfig(
                symbol=symbol,
                trading_mode=data.get("tradingMode", "Auto"),
                timeframe=data.get("timeframe", "15m"),
                risk_appetite=data.get("riskAppetite", "Moderate"),
                account_balance=account_balance,
                fast_period=safe_float_conversion(data.get("emaConfig", {}).get("fastPeriod", 21), 21),
                slow_period=safe_float_conversion(data.get("emaConfig", {}).get("slowPeriod", 51), 51),
                max_risk_percent=safe_float_conversion(data.get("riskAmount", "1"), 1.0),
                max_leverage=int(data.get("leverage", 5))
            )
            result = await strategy_manager.start_strategy(symbol, strategy_name, config)
            
        elif strategy_name in ["SMC_Strategy", "smc_strategy"] and SMC_Strategy:
            # Direct instantiation for SMC (keeping your existing approach)
            safe_config = {}
            for key, value in data.items():
                if key in ['riskAmount', 'atrThreshold', 'adxThreshold']:
                    safe_config[key] = safe_float_conversion(value, 1.0)
                else:
                    safe_config[key] = value

            smc_instance = SMC_Strategy(
                symbol=symbol,
                config=safe_config,
                delta_private=delta_client.request,
                products=PRODUCTS_CACHE,
                symbol_to_pid=SYMBOL_TO_PID,
                api_key=os.getenv("DELTA_API_KEY"),
                api_secret=os.getenv("DELTA_API_SECRET")
            )

            smc_instance.start()
            strategy_manager.active_strategies[symbol] = smc_instance
            ACTIVE_STRATEGIES[symbol] = smc_instance
            result = {"success": True, "message": f"SMC Strategy started for {symbol}"}
            
        elif strategy_name in ["GridBot_Strategy", "gridbot_strategy"] and GridBot_Strategy:
            # Direct instantiation for GridBot
            safe_config = {}
            for key, value in data.items():
                if key in ['riskAmount', 'gridSpacing', 'priceRange']:
                    safe_config[key] = safe_float_conversion(value, 1.0)
                else:
                    safe_config[key] = value

            grid_instance = GridBot_Strategy(
                symbol=symbol,
                config=safe_config,
                delta_private=delta_client.request,
                products=PRODUCTS_CACHE,
                symbol_to_pid=SYMBOL_TO_PID,
                api_key=os.getenv("DELTA_API_KEY"),
                api_secret=os.getenv("DELTA_API_SECRET")
            )

            grid_instance.start()
            strategy_manager.active_strategies[symbol] = grid_instance
            ACTIVE_STRATEGIES[symbol] = grid_instance
            result = {"success": True, "message": f"Grid Bot started for {symbol}"}
            
        else:
            # For any other registered strategies, use strategy manager
            config = StrategyConfig(symbol=symbol, account_balance=account_balance)
            result = await strategy_manager.start_strategy(symbol, strategy_name, config)

        print(f"Strategy start result: {result}")
        return result

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Strategy start error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/strategies/stop")
async def stop_strategy_endpoint(request: Request):
    """Stop strategy with proper cleanup"""
    try:
        data = await request.json()
        symbol = data.get("symbol")
        
        print(f"🛑 Stopping strategy for {symbol}...")
        
        # Check both active strategies collections
        strategy_found = False
        if symbol in strategy_manager.active_strategies:
            strategy = strategy_manager.active_strategies[symbol]
            await strategy.stop()
            del strategy_manager.active_strategies[symbol]
            strategy_found = True
        
        if symbol in ACTIVE_STRATEGIES:
            strategy = ACTIVE_STRATEGIES[symbol]
            await strategy.stop()
            del ACTIVE_STRATEGIES[symbol]
            strategy_found = True
        
        if strategy_found:
            print(f"✅ Strategy stopped for {symbol}")
            return {"success": True, "message": f"Strategy stopped for {symbol}"}
        else:
            return {"success": False, "message": "No strategy running for this symbol"}

    except Exception as e:
        print(f"❌ Stop error: {e}")
        # Force cleanup on error
        if 'symbol' in locals():
            strategy_manager.active_strategies.pop(symbol, None)
            ACTIVE_STRATEGIES.pop(symbol, None)
        return {"success": False, "error": str(e)}

@app.get("/api/strategies/status/{symbol}")
async def get_strategy_status(symbol: str):
    """Get detailed status for a specific symbol"""
    try:
        if symbol not in strategy_manager.active_strategies:
            return {"running": False, "error": "No strategy active for this symbol"}
        
        strategy = strategy_manager.active_strategies[symbol]
        
        # Get comprehensive status
        status = strategy.get_status() if hasattr(strategy, 'get_status') else {}
        
        # Verify strategy is actually running
        is_running = False
        if hasattr(strategy, 'is_running'):
            is_running = strategy.is_running
        elif hasattr(strategy, 'state'):
            state_value = strategy.state.value if hasattr(strategy.state, 'value') else strategy.state
            is_running = state_value == "running"
        else:
            is_running = status.get("running", False)
        
        detailed_status = {
            "running": is_running,
            "state": getattr(strategy, 'state', 'unknown'),
            "strategy_type": strategy.__class__.__name__,
            "info": status.get("info", f"Strategy details for {symbol}"),
            "details": status.get("details", {}),
            "metrics": status.get("metrics", {}),
            "last_update": time.time()
        }
        
        # Add WebSocket connection status
        if ws_manager and hasattr(ws_manager, 'is_connected'):
            detailed_status["websocket_connected"] = ws_manager.is_connected(symbol)
        
        return detailed_status
        
    except Exception as e:
        print(f"❌ Strategy status error for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/strategies/status")
async def get_all_strategies_status():
    """Get status of all active strategies with accurate running state"""
    try:
        all_status = {}
        
        # Check both strategy_manager and ACTIVE_STRATEGIES
        for symbol in TRACKED_SYMBOLS:
            strategy_status = {"running": False, "error": "No strategy active"}
            
            # Check if strategy is in active strategies
            if symbol in strategy_manager.active_strategies:
                strategy = strategy_manager.active_strategies[symbol]
                
                # Get strategy status and verify it's actually running
                status = strategy.get_status() if hasattr(strategy, 'get_status') else {}
                
                # For SMC/GridBot strategies, check is_running attribute
                if hasattr(strategy, 'is_running'):
                    is_actually_running = strategy.is_running
                elif hasattr(strategy, 'state'):
                    is_actually_running = strategy.state == "running" or strategy.state.value == "running"
                else:
                    is_actually_running = status.get("running", False)
                
                strategy_status = {
                    "running": is_actually_running,
                    "state": getattr(strategy, 'state', 'unknown'),
                    "info": status.get("info", f"Strategy active for {symbol}"),
                    "details": status.get("details", {}),
                    "strategy_type": strategy.__class__.__name__,
                    "last_update": time.time()
                }
                
                # Add WebSocket status if available
                if ws_manager and hasattr(ws_manager, 'is_connected'):
                    strategy_status["websocket_connected"] = ws_manager.is_connected(symbol)
            
            all_status[symbol] = strategy_status
        
        print(f"📊 Status check: {[(s, st['running']) for s, st in all_status.items() if st['running']]}")
        return all_status
        
    except Exception as e:
        print(f"❌ Status check error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/trades")
async def get_trades():
    """Get recent trade journal entries"""
    try:
        trades = await db_manager.get_recent_trades()
        
        # Convert datetime objects to strings for JSON serialization
        for trade in trades:
            if 'timestamp' in trade and hasattr(trade['timestamp'], 'isoformat'):
                trade['timestamp'] = trade['timestamp'].isoformat()
        
        return trades
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/trade_stats")
async def get_trade_stats():
    """Get comprehensive trade statistics"""
    try:
        return await db_manager.get_trade_statistics()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/wallet")
async def get_wallet():
    """Get wallet balances"""
    try:
        return await delta_client.get_wallet_balances()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/positions/{symbol}")
async def get_positions(symbol: str):
    """Get positions for a specific symbol"""
    try:
        product_id = SYMBOL_TO_PID.get(symbol)
        if not product_id:
            return {"error": f"Product ID not found for symbol: {symbol}"}

        positions = await delta_client.get_positions(str(product_id))
        
        # Return single position if list with one item, otherwise return as-is
        if isinstance(positions, list) and len(positions) == 1:
            return positions[0]
        elif isinstance(positions, list) and len(positions) == 0:
            return {}
        else:
            return positions

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Market data endpoint with optional rate limiting
if RATE_LIMITING_AVAILABLE:
    @app.get("/api/market/{symbol}")
    @limiter.limit("120/minute")
    async def get_market_data(request: Request, symbol: str):
        """Get real-time market data for symbol (rate limited)"""
        try:
            return await delta_client.get_ticker(symbol=symbol)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
else:
    @app.get("/api/market/{symbol}")
    async def get_market_data(symbol: str):
        """Get real-time market data for symbol"""
        try:
            return await delta_client.get_ticker(symbol=symbol)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/smc_analysis/{symbol}")
async def get_smc_analysis(symbol: str):
    """Get SMC analysis data for chart visualization"""
    try:
        # Check if SMC strategy is running for this symbol
        if symbol in strategy_manager.active_strategies:
            strategy = strategy_manager.active_strategies[symbol]
            
            # Check if it's an SMC strategy by looking for SMC-specific attributes
            if hasattr(strategy, 'market_structure'):
                current_time = int(time.time())
                hours_24_ago = current_time - (24 * 60 * 60)
                
                # Prepare swing points data
                swing_points = []
                if hasattr(strategy, 'last_higher_high') and strategy.last_higher_high:
                    swing_points.append({
                        'type': 'HH',
                        'price': strategy.last_higher_high,
                        'timestamp': current_time - 3600
                    })
                
                if hasattr(strategy, 'last_higher_low') and strategy.last_higher_low:
                    swing_points.append({
                        'type': 'HL',
                        'price': strategy.last_higher_low,
                        'timestamp': current_time - 3600
                    })
                
                if hasattr(strategy, 'last_lower_high') and strategy.last_lower_high:
                    swing_points.append({
                        'type': 'LH',
                        'price': strategy.last_lower_high,
                        'timestamp': current_time - 3600
                    })
                
                if hasattr(strategy, 'last_lower_low') and strategy.last_lower_low:
                    swing_points.append({
                        'type': 'LL',
                        'price': strategy.last_lower_low,
                        'timestamp': current_time - 3600
                    })

                # Process order blocks
                order_blocks = []
                for ob in getattr(strategy, 'order_blocks', []):
                    if ob.get('timestamp', 0) > hours_24_ago:
                        order_blocks.append({
                            'type': ob.get('type', 'BULLISH'),
                            'high': ob.get('high', 0),
                            'low': ob.get('low', 0),
                            'startTime': ob.get('timestamp', current_time),
                            'mitigated': ob.get('mitigated', False),
                            'strength': ob.get('strength', 0)
                        })

                # Process FVGs
                fvgs = []
                for fvg in getattr(strategy, 'fair_value_gaps', []):
                    if fvg.get('timestamp', 0) > hours_24_ago:
                        fvgs.append({
                            'type': fvg.get('type', 'BULLISH'),
                            'high': fvg.get('high', 0),
                            'low': fvg.get('low', 0),
                            'startTime': fvg.get('timestamp', current_time),
                            'filled': fvg.get('filled', False),
                            'size': fvg.get('size', 0)
                        })

                # Mock BOS/CHoCH data
                bos = []
                choch = []
                if getattr(strategy, 'bos_detected', False):
                    bos.append({
                        'timestamp': current_time - 1800,
                        'direction': 'BULLISH' if strategy.market_structure == 'BULLISH' else 'BEARISH',
                        'level': 0
                    })

                if getattr(strategy, 'choch_detected', False):
                    choch.append({
                        'timestamp': current_time - 3600,
                        'direction': strategy.market_structure,
                        'previousStructure': 'RANGING'
                    })

                return {
                    "success": True,
                    "result": {
                        "marketStructure": getattr(strategy, 'market_structure', 'RANGING'),
                        "orderBlocks": order_blocks,
                        "fvgs": fvgs,
                        "swingPoints": swing_points,
                        "bos": bos,
                        "choch": choch,
                        "liquiditySweeps": [],
                        "lastUpdated": current_time
                    }
                }

        # Return empty analysis if no SMC strategy running
        return {
            "success": True,
            "result": {
                "marketStructure": "RANGING",
                "orderBlocks": [],
                "fvgs": [],
                "swingPoints": [],
                "bos": [],
                "choch": [],
                "liquiditySweeps": [],
                "lastUpdated": int(time.time())
            }
        }

    except Exception as e:
        print(f"SMC analysis error for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/grid_performance/{symbol}")
async def get_grid_performance(symbol: str):
    """Get Grid Bot performance data"""
    try:
        if symbol in strategy_manager.active_strategies:
            strategy = strategy_manager.active_strategies[symbol]
            
            # Check if it's a Grid Bot strategy
            if hasattr(strategy, 'get_grid_performance'):
                performance = strategy.get_grid_performance()
                return {"success": True, "result": performance}

        # Return empty performance data if no Grid Bot running
        return {
            "success": True,
            "result": {
                "total_profits": 0,
                "total_trades": 0,
                "active_orders": 0,
                "position_value": 0,
                "avg_profit_per_trade": 0,
                "profit_percentage": 0
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# New endpoints for advanced features (if components available)
if HEALTH_MONITOR_AVAILABLE:
    @app.get("/api/system/health")
    async def get_system_health():
        """Get comprehensive system health status"""
        try:
            health_data = await health_monitor.check_system_health()
            summary = health_monitor.get_health_summary()
            
            return {
                "success": True,
                "result": {
                    "summary": summary,
                    "components": {name: {
                        "name": metric.name,
                        "status": metric.status.value,
                        "value": metric.value,
                        "threshold": metric.threshold,
                        "message": metric.message,
                        "timestamp": metric.timestamp,
                        "details": metric.details
                    } for name, metric in health_data.items()}
                }
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

if PSUTIL_AVAILABLE:
    @app.get("/api/system/metrics")
    async def get_system_metrics():
        """Get current system performance metrics"""
        try:
            return {
                "success": True,
                "result": {
                    "cpu_percent": psutil.cpu_percent(interval=1),
                    "memory_percent": psutil.virtual_memory().percent,
                    "disk_percent": (psutil.disk_usage('/').used / psutil.disk_usage('/').total) * 100,
                    "active_strategies": len(strategy_manager.active_strategies),
                    "network_io": dict(psutil.net_io_counters()._asdict()) if hasattr(psutil.net_io_counters(), '_asdict') else {},
                    "timestamp": time.time()
                }
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

if PORTFOLIO_MANAGER_AVAILABLE:
    @app.get("/api/portfolio/risk")
    async def get_portfolio_risk():
        """Get comprehensive portfolio risk analysis"""
        try:
            positions = []
            total_balance = await delta_client.get_account_balance()

            # Collect positions from all active strategies
            for symbol, strategy in strategy_manager.active_strategies.items():
                try:
                    position_data = await delta_client.get_positions(str(SYMBOL_TO_PID[symbol]))
                    if position_data and isinstance(position_data, dict):
                        position_size = abs(float(position_data.get("size", 0)))
                        if position_size > 0:
                            current_price_data = await delta_client.get_ticker(symbol=symbol)
                            mark_price = float(current_price_data.get("mark_price", 0))
                            
                            position = PositionRisk(
                                symbol=symbol,
                                risk_amount=position_size * mark_price * 0.02,  # Estimate 2% risk
                                position_size=position_size,
                                entry_price=float(position_data.get("entry_price", 0)),
                                current_price=mark_price,
                                unrealized_pnl=float(position_data.get("unrealized_pnl", 0)),
                                strategy=strategy.__class__.__name__
                            )
                            positions.append(position)
                except Exception as e:
                    print(f"Error processing position for {symbol}: {e}")

            # Calculate portfolio metrics
            portfolio_risk = await portfolio_manager.calculate_portfolio_risk(positions, total_balance)
            recommendations = portfolio_manager.get_portfolio_recommendations(positions, total_balance)

            return {
                "success": True,
                "result": {
                    "portfolio_risk": portfolio_risk,
                    "positions": [{
                        "symbol": pos.symbol,
                        "risk_amount": pos.risk_amount,
                        "position_size": pos.position_size,
                        "unrealized_pnl": pos.unrealized_pnl,
                        "strategy": pos.strategy,
                        "entry_price": pos.entry_price,
                        "current_price": pos.current_price
                    } for pos in positions],
                    "recommendations": recommendations,
                    "account_balance": total_balance,
                    "total_positions": len(positions)
                }
            }

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ========================================
# WEBSOCKET ENDPOINTS
# ========================================

@app.websocket("/ws/live/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):
    """WebSocket endpoint for real-time price updates"""
    await websocket.accept()
    try:
        # Store client WebSocket for this symbol
        if not hasattr(ws_manager, 'client_websockets'):
            ws_manager.client_websockets = {}
        ws_manager.client_websockets[symbol] = websocket
        
        print(f"✅ WebSocket connected for {symbol}")
        
        # Keep connection alive
        while True:
            try:
                # Send ping to keep connection alive
                await websocket.ping()
                await asyncio.sleep(30)  # Ping every 30 seconds
            except Exception:
                print(f"❌ WebSocket ping failed for {symbol}")
                break
                
    except Exception as e:
        print(f"❌ WebSocket error for {symbol}: {e}")
    finally:
        # Clean up connection
        if hasattr(ws_manager, 'client_websockets') and symbol in ws_manager.client_websockets:
            del ws_manager.client_websockets[symbol]
        print(f"🔌 WebSocket disconnected for {symbol}")

if HEALTH_MONITOR_AVAILABLE and PSUTIL_AVAILABLE:
    @app.websocket("/ws/metrics")
    async def websocket_metrics(websocket: WebSocket):
        """WebSocket endpoint for real-time system metrics"""
        await websocket.accept()
        try:
            print("✅ Metrics WebSocket connected")
            
            while True:
                # Collect current system metrics
                metrics_data = {
                    "cpu_percent": psutil.cpu_percent(),
                    "memory_percent": psutil.virtual_memory().percent,
                    "disk_percent": (psutil.disk_usage('/').used / psutil.disk_usage('/').total) * 100,
                    "active_strategies": len(strategy_manager.active_strategies),
                    "timestamp": time.time(),
                    "cpu_trend": 0,  # Would need historical data for real trends
                    "memory_trend": 0,
                    "api_latency_trend": 0
                }
                
                await websocket.send_json(metrics_data)
                await asyncio.sleep(5)  # Send updates every 5 seconds
                
        except Exception as e:
            print(f"❌ Metrics WebSocket error: {e}")
        finally:
            print("🔌 Metrics WebSocket disconnected")

# ========================================
# WEBSOCKET HANDLER
# ========================================

async def _websocket_handler(symbol: str, data: Dict):
    """Handle WebSocket messages from Delta Exchange"""
    try:
        # Forward to client WebSocket if connected
        if (hasattr(ws_manager, 'client_websockets') and
            symbol in ws_manager.client_websockets):
            client_ws = ws_manager.client_websockets[symbol]
            if hasattr(client_ws, 'client_state') and client_ws.client_state.name == "CONNECTED":
                await client_ws.send_json(data)

        # Update strategies that need real-time data
        if symbol in strategy_manager.active_strategies:
            strategy = strategy_manager.active_strategies[symbol]
            if hasattr(strategy, 'on_market_data'):
                await strategy.on_market_data(data)

    except Exception as e:
        print(f"❌ WebSocket handler error for {symbol}: {e}")

# ========================================
# APPLICATION STARTUP
# ========================================

# Ensure CORS middleware is properly configured
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://vy-delta-trader-2.onrender.com", "http://127.0.0.1:3000", "http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"]
)
@app.api_route("/health", methods=["GET", "HEAD"])
async def uptime_health(request: Request):
    return {"status": "ok"}


async def get_account_balance():
    """Get current account balance for risk calculations"""
    try:
        if delta_client:
            wallet_data = await delta_client.get_wallet_balances()
            if isinstance(wallet_data, list) and len(wallet_data) > 0:
                # Look for USDT or USD balance
                for balance in wallet_data:
                    asset_symbol = balance.get("asset", {}).get("symbol", "")
                    if asset_symbol in ["USDT", "USD"]:
                        return float(balance.get("balance", 0))
                # If no USDT/USD found, return first balance
                return float(wallet_data[0].get("balance", 0))
            return 0
        return 0
    except Exception as e:
        print(f"Error fetching account balance: {e}")
        return 0    

if __name__ == "__main__":
    import uvicorn
    
    # Get configuration from environment
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))
    workers = int(os.getenv("WORKERS", 1))
    reload = os.getenv("RELOAD", "true").lower() == "true"
    
    print(f"🚀 Starting VY Trading Bot on {host}:{port}")
    print(f"📁 Strategies will be auto-discovered from ./strategies folder")
    print(f"💾 Using local file storage (./.state/ directory)")
    print(f"🔄 Reload mode: {reload}")
    
    uvicorn.run(
        "main_app:app",
        host=host,
        port=port,
        workers=workers,
        reload=reload,
        log_level="info",
        access_log=True
    )
