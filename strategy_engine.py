# strategy_engine.py - Modular Strategy Management System

import asyncio
import time
import uuid
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import logging

from delta_client import DeltaAPIClient
from risk_manager import RiskManager
from order_executor import OrderExecutor
from database_manager import DatabaseManager

class StrategyState(Enum):
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"

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

@dataclass
class StrategyConfig:
    """Strategy configuration"""
    symbol: str
    trading_mode: str = "Auto"  # Auto or Manual
    timeframe: str = "15m"
    risk_appetite: str = "Moderate"  # Conservative, Moderate, Aggressive
    account_balance: float = 10000.0
    
    # EMA Configuration
    fast_period: int = 21
    slow_period: int = 51
    
    # Risk Management
    max_risk_percent: float = 1.0
    max_leverage: int = 5
    risk_reward_ratio: str = "1:3"
    
    # Protection Settings
    signal_cooldown: int = 60
    restart_protection: int = 120
    position_check_interval: int = 5

@dataclass
class StrategyMetrics:
    """Strategy performance metrics"""
    total_signals: int = 0
    successful_trades: int = 0
    failed_trades: int = 0
    total_pnl: float = 0.0
    win_rate: float = 0.0
    execution_latency_ms: float = 0.0
    last_trade_time: float = 0.0
    last_signal: Optional[str] = None

class BaseStrategy(ABC):
    """
    🎯 Modular Base Strategy Class
    Provides core functionality for all trading strategies
    """

    def __init__(self, config: StrategyConfig, delta_client: DeltaAPIClient,
                 risk_manager: RiskManager, order_executor: OrderExecutor,
                 db_manager: DatabaseManager):
        self.config = config
        self.delta_client = delta_client
        self.risk_manager = risk_manager
        self.order_executor = order_executor
        self.db_manager = db_manager
        
        # Strategy identity
        self.strategy_id = str(uuid.uuid4())
        self.instance_id = f"{config.symbol}_{int(time.time())}"
        
        # State management
        self.state = StrategyState.INITIALIZING
        self.is_running = False
        self.task: Optional[asyncio.Task] = None
        
        # Metrics
        self.metrics = StrategyMetrics()
        
        # Logger
        self.logger = self._setup_logger()
        
        # Status for API
        self.status = {
            "running": False,
            "state": self.state.value,
            "error": None,
            "info": "Strategy initialized",
            "details": {},
            "metrics": {}
        }

    def _setup_logger(self) -> logging.Logger:
        """Setup strategy logger"""
        logger = logging.getLogger(f"Strategy.{self.config.symbol}.{self.strategy_id[:8]}")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                f'%(asctime)s - {self.config.symbol} - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    @abstractmethod
    async def execute_strategy_logic(self) -> None:
        """Strategy-specific logic to be implemented by subclasses"""
        pass

    @abstractmethod
    def calculate_signals(self, market_data: dict) -> Dict[str, Any]:
        """Calculate trading signals"""
        pass

    async def start(self) -> bool:
        """Start the strategy"""
        if self.state == StrategyState.RUNNING:
            self.logger.warning("Strategy already running")
            return False

        try:
            self.state = StrategyState.RUNNING
            self.is_running = True
            
            # Load previous state
            await self._load_state()
            
            # Start strategy task
            self.task = asyncio.create_task(self._run_with_monitoring())
            
            self.status.update({
                "running": True,
                "state": self.state.value,
                "info": f"Strategy started for {self.config.symbol}"
            })
            
            self.logger.info(f"🚀 Strategy started: {self.config.symbol}")
            return True

        except Exception as e:
            self.state = StrategyState.ERROR
            self.status["error"] = str(e)
            self.logger.error(f"❌ Failed to start strategy: {e}")
            return False

    async def stop(self) -> None:
        """Stop the strategy with cooperative cancellation"""
        self.logger.info(f"🛑 Stopping strategy: {self.config.symbol}")
    
        # Step 1: Signal stop immediately
        self.state = StrategyState.STOPPING
        self.is_running = False
    
        # Step 2: Cancel task with short timeout
        if self.task and not self.task.done():
            self.logger.info("Cancelling strategy task...")
            self.task.cancel()
        
            try:
                # Wait for task completion with timeout
                await asyncio.wait_for(self.task, timeout=4.0)
                self.logger.info("Strategy task completed gracefully")
            except asyncio.CancelledError:
                self.logger.info("Strategy task cancelled successfully")
            except asyncio.TimeoutError:
                self.logger.warning("Strategy task cancellation timeout - forcing completion")
            except Exception as e:
                self.logger.error(f"Error during task cancellation: {e}")

        # Step 3: Save final state with timeout
        try:
            await asyncio.wait_for(self._save_state(), timeout=2.0)
            self.logger.info("Final state saved")
        except asyncio.TimeoutError:
            self.logger.warning("Save state timeout during shutdown - continuing")
        except Exception as e:
            self.logger.warning(f"Failed to save final state: {e}")

        # Step 4: Update status
        self.state = StrategyState.STOPPED
        self.status.update({
            "running": False,
            "state": self.state.value,
            "info": "Strategy stopped"
        })
    
        self.logger.info(f"✅ Strategy {self.config.symbol} stopped completely")


    async def _run_with_monitoring(self) -> None:
        """Run strategy with comprehensive monitoring"""
        try:
            await self.execute_strategy_logic()
        except asyncio.CancelledError:
            self.logger.info("Strategy cancelled")
            raise
        except Exception as e:
            self.logger.error(f"❌ Strategy execution error: {e}")
            self.state = StrategyState.ERROR
            self.status["error"] = str(e)

    async def _load_state(self) -> None:
        """Load strategy state from database"""
        try:
            state_data = await self.db_manager.load_strategy_state(self.config.symbol)
            if state_data:
                self.metrics.last_signal = state_data.get("last_signal")
                self.metrics.last_trade_time = state_data.get("last_trade_time", 0)
                self.metrics.total_signals = state_data.get("total_signals", 0)
                self.logger.info(f"✅ Loaded state: {len(state_data)} keys")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to load state: {e}")

    async def _save_state(self) -> None:
        """Save strategy state to database"""
        try:
            state_data = {
                "last_signal": self.metrics.last_signal,
                "last_trade_time": self.metrics.last_trade_time,
                "total_signals": self.metrics.total_signals,
                "strategy_id": self.strategy_id,
                "saved_at": time.time()
            }
            await self.db_manager.save_strategy_state(self.config.symbol, state_data)
        except Exception as e:
            self.logger.error(f"❌ Failed to save state: {e}")

    def get_status(self) -> dict:
        """Get current strategy status"""
        self.status.update({
            "metrics": {
                "total_signals": self.metrics.total_signals,
                "successful_trades": self.metrics.successful_trades,
                "win_rate": self.metrics.win_rate,
                "total_pnl": self.metrics.total_pnl,
                "execution_latency_ms": self.metrics.execution_latency_ms
            },
            "last_update": time.time()
        })
        return self.status.copy()

class StrategyManager:
    """
    🏗️ Central Strategy Management System
    Manages multiple strategy instances and their lifecycle
    """

    def __init__(self, delta_client: DeltaAPIClient, risk_manager: RiskManager,
                 order_executor: OrderExecutor, db_manager: DatabaseManager):
        self.delta_client = delta_client
        self.risk_manager = risk_manager
        self.order_executor = order_executor
        self.db_manager = db_manager
        
        self.active_strategies: Dict[str, BaseStrategy] = {}
        self.strategy_registry: Dict[str, type] = {}

    def register_strategy(self, name: str, strategy_class: type) -> None:
        """Register a new strategy class"""
        self.strategy_registry[name] = strategy_class

    async def start_strategy(self, symbol: str, strategy_name: str, config: StrategyConfig) -> dict:
        """Start a strategy for a symbol"""
        if symbol in self.active_strategies:
            await self.stop_strategy(symbol)

        if strategy_name not in self.strategy_registry:
            raise ValueError(f"Unknown strategy: {strategy_name}")

        strategy_class = self.strategy_registry[strategy_name]
        strategy = strategy_class(
            config, self.delta_client, self.risk_manager,
            self.order_executor, self.db_manager
        )

        success = await strategy.start()
        if success:
            self.active_strategies[symbol] = strategy
            return {"success": True, "message": f"Strategy {strategy_name} started for {symbol}"}
        else:
            return {"success": False, "message": "Failed to start strategy"}

    async def stop_strategy(self, symbol: str) -> dict:
        """Stop a strategy with timeout protection"""
        if symbol not in self.active_strategies:
            return {"success": False, "message": "No strategy running for symbol"}

        strategy = self.active_strategies.get(symbol)
        if not strategy:
            return {"success": False, "message": "Strategy not found"}

        try:
            # Stop with timeout
            await asyncio.wait_for(strategy.stop(), timeout=8.0)
            # Remove from active strategies
            self.active_strategies.pop(symbol, None)
            return {"success": True, "message": f"Strategy stopped for {symbol}"}
            
        except asyncio.TimeoutError:
            # Force removal on timeout
            self.active_strategies.pop(symbol, None)
            return {"success": True, "message": f"Strategy force-stopped for {symbol} (timeout)"}
        except Exception as e:
            # Force removal on error
            self.active_strategies.pop(symbol, None)
            return {"success": True, "message": f"Strategy stopped with errors for {symbol}: {str(e)}"}

    def get_strategy_status(self, symbol: str) -> dict:
        """Get status of a strategy"""
        if symbol not in self.active_strategies:
            return {"running": False, "error": "No strategy active"}
        return self.active_strategies[symbol].get_status()

    def get_all_strategies_status(self) -> Dict[str, dict]:
        """Get status of all active strategies"""
        return {symbol: strategy.get_status()
                for symbol, strategy in self.active_strategies.items()}

class MultiTimeframeStrategy(BaseStrategy):
    def __init__(self, config):
        super().__init__(config)
        self.timeframes = ['5m', '15m', '1H', '4H']
        self.candle_data = {}

    async def analyze_multiple_timeframes(self):
        for tf in self.timeframes:
            candles = await self.delta_client.get_candles(
                self.config.symbol, tf,
                start_time, end_time, 100
            )
            self.candle_data[tf] = self.calculate_indicators(candles)
        return self.generate_multi_tf_signal()
