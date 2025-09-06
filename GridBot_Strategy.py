# GridBot_Strategy.py - Professional Grid Trading Bot with Risk-Based Investment & Enhanced Safety

import time
import asyncio
import pandas as pd
import numpy as np
from strategy_engine import BaseStrategy, StrategyConfig
import requests
import websockets
import json
import hmac
import hashlib
import uuid
import sys
import os
from database import save_strategy_state, load_strategy_state

# TradeJournal import
try:
    from TradeJournal import add_trade
    TRADE_JOURNAL_AVAILABLE = True
except ImportError:
    TRADE_JOURNAL_AVAILABLE = False
    def add_trade(data): pass

BASE_URL = "https://api.india.delta.exchange"
WS_URL = "wss://socket.india.delta.exchange"

# Base Strategy Class
class Strategy:
    def __init__(self, symbol, config, delta_private, products, symbol_to_pid):
        self.symbol = symbol
        self.config = config
        self.delta_private = delta_private
        self.products = products
        self.symbol_to_pid = symbol_to_pid
        
        # Clean state management
        self.is_running = False
        self.task = None
        self.status = {
            "running": False,
            "error": None,
            "info": "Strategy initialized",
            "details": {}
        }

    def start(self):
        """Start the strategy"""
        if self.is_running:
            print(f"[{self.symbol}] Strategy already running")
            return
        
        print(f"[{self.symbol}] Starting strategy...")
        self.is_running = True
        self.status["running"] = True
        self.status["info"] = "Strategy started"
        self.task = asyncio.create_task(self.run())

    async def stop(self):
        """Stop the strategy"""
        if not self.is_running:
            print(f"[{self.symbol}] Strategy not running")
            return

        print(f"[{self.symbol}] Stopping strategy...")
        self.is_running = False
        
        if self.task and not self.task.done():
            print(f"[{self.symbol}] Cancelling strategy task...")
            self.task.cancel()
            try:
                await asyncio.wait_for(self.task, timeout=3.0)
                print(f"[{self.symbol}] Strategy task completed gracefully")
            except asyncio.CancelledError:
                print(f"[{self.symbol}] Strategy task cancelled successfully")
            except asyncio.TimeoutError:
                print(f"[{self.symbol}] Strategy task cancellation timed out - forcing")
        
        # Clean state update
        self.status["running"] = False
        self.status["info"] = "Strategy stopped"
        print(f"[{self.symbol}] Strategy stop completed")

    def get_status(self):
        """Get current strategy status"""
        return self.status.copy()

    async def run(self):
        """Override this in subclasses"""
        pass

class GridBot_Strategy(Strategy):
    # SAFETY CONSTANTS - CRITICAL RISK MANAGEMENT
    MAX_GRID_LEVELS = 50              # Maximum allowed grid levels
    MIN_GRID_LEVELS = 3               # Minimum grid levels
    MAX_INVESTMENT_AMOUNT = 50     # Maximum investment per grid (USD)
    MIN_INVESTMENT_AMOUNT = 5       # Minimum investment amount
    MAX_DAILY_DRAWDOWN = 0.08         # 8% max daily loss (higher for grid)
    SAFETY_MARGIN_RATIO = 0.70        # Use 70% of available balance
    MAX_POSITION_RATIO = 0.60         # Max 60% of investment as position
    MIN_GRID_PROFIT_PCT = 0.002       # Minimum 0.2% profit per grid
    MAX_GRID_PROFIT_PCT = 0.05        # Maximum 5% profit per grid
    MAX_ORDERS_PER_SIDE = 25          # Maximum orders per side
    MAX_RISK_PERCENT = 15.0           # Maximum 15% risk for grid trading
    MIN_RISK_PERCENT = 0.5            # Minimum 0.5% risk
    
    def __init__(self, symbol, config, delta_private, products, symbol_to_pid, api_key, api_secret):
        super().__init__(symbol, config, delta_private, products, symbol_to_pid)
        
        # Core Settings
        self.api_key = api_key
        self.api_secret = api_secret
        self.instance_id = str(uuid.uuid4())[:8]
        
        # SAFETY TRACKING - ENHANCED RISK MANAGEMENT
        self.daily_pnl = 0.0              # Track daily P&L
        self.daily_start_balance = 0.0     # Starting balance for day
        self.orders_placed_today = 0       # Count daily orders
        self.last_balance_check = 0        # Last balance check timestamp
        self.consecutive_errors = 0        # Track consecutive errors
        self.last_rebalance_time = 0       # Last rebalance timestamp
        
        # Grid Configuration
        self.trading_mode = config.get("tradingMode", "Auto")
        self.account_balance = config.get("account_balance", 10000)
        
        # Grid Parameters from config (will be validated and potentially overridden)
        self.grid_config = config.get("gridConfig", {})
        self.grid_type = self.grid_config.get("type", "Arithmetic")
        self.price_range = {
            "upper": float(self.grid_config.get("upperPrice", 0)),
            "lower": float(self.grid_config.get("lowerPrice", 0))
        }
        self.grid_levels = int(self.grid_config.get("gridLevels", 10))
        
        # RISK-BASED INVESTMENT CONFIGURATION
        self.risk_type = self.grid_config.get("riskType", "Percentage")
        self.risk_amount_config = self.grid_config.get("riskAmount", "3%")
        
        # These will be calculated dynamically based on available balance
        self.investment_amount = 0.0       # Will be calculated
        self.per_grid_amount = 0.0         # Will be calculated
        
        # Grid State
        self.grid_orders = {
            "buy_levels": [],    # List of buy price levels
            "sell_levels": [],   # List of sell price levels
            "active_orders": {}, # Dict of active order IDs
            "filled_orders": [], # History of filled orders
            "profits": []        # Profit records
        }
        
        # Performance Tracking
        self.total_profits = 0.0
        self.total_trades = 0
        self.grid_initialized = False
        self.last_price = 0.0
        
        # Enhanced Risk Management (calculated dynamically)
        self.max_position = 0.0  # Will be calculated based on investment
        self.current_position = 0.0
        self.stop_loss_percentage = float(self.grid_config.get("stopLoss", 12))  # 12% default
        self.profit_target_percentage = float(self.grid_config.get("profitTarget", 0))  # Optional
        
        print(f"[{self.symbol}] 🤖 Grid Bot Initialized - Instance: {self.instance_id}")
        print(f" Type: {self.grid_type} | Levels: {self.grid_levels}")
        print(f" Risk Type: {self.risk_type} | Risk Amount: {self.risk_amount_config}")
        print(f" 🛡️ Safety: Max Levels: {self.MAX_GRID_LEVELS}, Max Risk: {self.MAX_RISK_PERCENT}%")
        
        # VALIDATE CONFIGURATION - CRITICAL SAFETY CHECK
        self.validate_config()
        
        self.load_state()

    def validate_config(self):
        """Validate and sanitize Grid Bot configuration - CRITICAL SAFETY METHOD"""
        print(f"[{self.symbol}] 🛡️ Validating Grid Bot configuration...")
        
        # Validate grid levels
        if self.grid_levels < self.MIN_GRID_LEVELS or self.grid_levels > self.MAX_GRID_LEVELS:
            print(f"[{self.symbol}] ⚠️ Grid levels {self.grid_levels} invalid, using 10")
            self.grid_levels = 10
        
        # Validate stop loss
        if self.stop_loss_percentage <= 0 or self.stop_loss_percentage > 50:
            print(f"[{self.symbol}] ⚠️ Stop loss {self.stop_loss_percentage}% invalid, using 12%")
            self.stop_loss_percentage = 12
        
        # Validate grid type
        if self.grid_type not in ['Arithmetic', 'Geometric']:
            print(f"[{self.symbol}] ⚠️ Invalid grid type '{self.grid_type}', using 'Arithmetic'")
            self.grid_type = 'Arithmetic'
        
        # Validate risk type
        if self.risk_type not in ['Percentage', 'USD']:
            print(f"[{self.symbol}] ⚠️ Invalid risk type '{self.risk_type}', using 'Percentage'")
            self.risk_type = 'Percentage'
        
        # Validate risk amount
        if self.risk_type == 'Percentage':
            try:
                if isinstance(self.risk_amount_config, str) and '%' in self.risk_amount_config:
                    risk_pct = float(self.risk_amount_config.replace('%', ''))
                else:
                    risk_pct = float(self.risk_amount_config)
                
                # SAFETY: Enforce risk percentage bounds
                if risk_pct < self.MIN_RISK_PERCENT:
                    print(f"[{self.symbol}] ⚠️ Risk {risk_pct}% too low, using {self.MIN_RISK_PERCENT}%")
                    self.risk_amount_config = f"{self.MIN_RISK_PERCENT}%"
                elif risk_pct > self.MAX_RISK_PERCENT:
                    print(f"[{self.symbol}] ⚠️ Risk {risk_pct}% too high, capping at {self.MAX_RISK_PERCENT}%")
                    self.risk_amount_config = f"{self.MAX_RISK_PERCENT}%"
                    
            except ValueError:
                print(f"[{self.symbol}] ⚠️ Invalid risk format '{self.risk_amount_config}', using '3%'")
                self.risk_amount_config = "3%"
        
        # Validate price range (if set)
        if self.price_range["upper"] > 0 and self.price_range["lower"] > 0:
            if self.price_range["upper"] <= self.price_range["lower"]:
                print(f"[{self.symbol}] ⚠️ Invalid price range, will auto-calculate")
                self.price_range = {"upper": 0, "lower": 0}
            elif (self.price_range["upper"] / self.price_range["lower"]) > 2.0:
                print(f"[{self.symbol}] ⚠️ Price range too wide (>100%), limiting to 80%")
                mid_price = (self.price_range["upper"] + self.price_range["lower"]) / 2
                self.price_range["upper"] = mid_price * 1.4
                self.price_range["lower"] = mid_price * 0.6
        
        print(f"[{self.symbol}] ✅ Grid Bot configuration validated")

    async def get_available_balance(self):
        """Get available balance with enhanced error handling"""
        try:
            balance_response = await self.delta_private("GET", "/v2/wallet/balances")
            available_balance = 0
            
            for balance in balance_response.get("result", []):
                if balance.get("asset_symbol") in ["USDT", "USD"]:
                    available_balance = float(balance.get("available_balance", 0))
                    break
            
            self.last_balance_check = time.time()
            return available_balance
            
        except Exception as e:
            print(f"[{self.symbol}] ❌ Balance check error: {e}")
            return 0

    async def calculate_risk_based_investment(self):
        """Calculate investment amount based on available balance and risk settings - CORE METHOD"""
        try:
            # Get current available balance
            available_balance = await self.get_available_balance()
            
            if available_balance <= 0:
                raise ValueError("Insufficient available balance")
            
            # Calculate risk amount based on user settings
            if self.risk_type == "Percentage":
                if isinstance(self.risk_amount_config, str) and "%" in self.risk_amount_config:
                    percentage = float(self.risk_amount_config.replace("%", ""))
                else:
                    percentage = float(self.risk_amount_config)
                
                # SAFETY: Enforce bounds
                percentage = max(self.MIN_RISK_PERCENT, min(percentage, self.MAX_RISK_PERCENT))
                risk_amount = available_balance * (percentage / 100)
                
            else:  # USD
                risk_amount = float(self.risk_amount_config)
                # SAFETY: Cap at percentage of available balance
                max_risk_usd = available_balance * (self.MAX_RISK_PERCENT / 100)
                if risk_amount > max_risk_usd:
                    print(f"[{self.symbol}] ⚠️ USD risk amount ${risk_amount:.2f} exceeds {self.MAX_RISK_PERCENT}% of balance, capping at ${max_risk_usd:.2f}")
                    risk_amount = max_risk_usd
            
            # SAFETY: Apply absolute investment bounds
            risk_amount = max(self.MIN_INVESTMENT_AMOUNT, min(risk_amount, self.MAX_INVESTMENT_AMOUNT))
            
            # Additional safety: Don't use more than safety margin of available balance
            max_safe_investment = available_balance * self.SAFETY_MARGIN_RATIO
            if risk_amount > max_safe_investment:
                print(f"[{self.symbol}] ⚠️ Investment reduced from ${risk_amount:.2f} to ${max_safe_investment:.2f} for balance safety")
                risk_amount = max_safe_investment
            
            return risk_amount, available_balance, percentage if self.risk_type == "Percentage" else None
            
        except Exception as e:
            print(f"[{self.symbol}] ❌ Risk calculation error: {e}")
            return self.MIN_INVESTMENT_AMOUNT, 0, None

    def load_state(self):
        """Load Grid Bot state from database"""
        try:
            state = load_strategy_state(self.symbol)
            self.grid_orders = state.get("grid_orders", {
                "buy_levels": [],
                "sell_levels": [],
                "active_orders": {},
                "filled_orders": [],
                "profits": []
            })
            self.total_profits = state.get("total_profits", 0.0)
            self.total_trades = state.get("total_trades", 0)
            self.grid_initialized = state.get("grid_initialized", False)
            self.current_position = state.get("current_position", 0.0)
            self.daily_pnl = state.get("daily_pnl", 0.0)
            self.orders_placed_today = state.get("orders_placed_today", 0)
            self.investment_amount = state.get("investment_amount", 0.0)
            self.per_grid_amount = state.get("per_grid_amount", 0.0)
            self.max_position = state.get("max_position", 0.0)
            
            print(f"[{self.symbol}] ✅ Grid State loaded")
            print(f" Investment: ${self.investment_amount:.2f} | Profits: ${self.total_profits:.2f}")
            print(f" Daily PnL: ${self.daily_pnl:.2f} | Orders Today: {self.orders_placed_today}")
        except Exception as e:
            print(f"[{self.symbol}] ⚠️ Grid State load error: {e}")

    def save_state(self):
        """Save Grid Bot state to database"""
        try:
            state = {
                "grid_orders": self.grid_orders,
                "total_profits": self.total_profits,
                "total_trades": self.total_trades,
                "grid_initialized": self.grid_initialized,
                "current_position": self.current_position,
                "daily_pnl": self.daily_pnl,
                "orders_placed_today": self.orders_placed_today,
                "investment_amount": self.investment_amount,
                "per_grid_amount": self.per_grid_amount,
                "max_position": self.max_position,
                "instance_id": self.instance_id,
                "saved_at": time.time()
            }
            save_strategy_state(self.symbol, state)
        except Exception as e:
            print(f"[{self.symbol}] ❌ Grid State save error: {e}")

    async def safe_shutdown(self, reason="Emergency shutdown"):
        """CRITICAL SAFETY: Safely shutdown Grid Bot with comprehensive cleanup"""
        print(f"[{self.symbol}] 🚨 GRID BOT EMERGENCY SHUTDOWN: {reason}")
        
        try:
            # Mark as not running immediately
            self.is_running = False
            self.status["running"] = False
            self.status["error"] = reason
            self.status["info"] = f"Emergency shutdown: {reason}"
            
            # Cancel all active grid orders
            cancelled_orders = 0
            for order_id in list(self.grid_orders.get("active_orders", {}).keys()):
                try:
                    cancel_response = await asyncio.wait_for(
                        self.delta_private("DELETE", f"/v2/orders/{order_id}"),
                        timeout=5.0
                    )
                    if cancel_response.get('success'):
                        cancelled_orders += 1
                    else:
                        print(f"[{self.symbol}] ⚠️ Failed to cancel order {order_id}")
                except Exception as e:
                    print(f"[{self.symbol}] ❌ Cancel order error: {e}")
            
            print(f"[{self.symbol}] 📋 Cancelled {cancelled_orders} active orders")
            
            # Clear active orders from state
            self.grid_orders["active_orders"] = {}
            
            # Save current state before shutdown
            self.save_state()
            
            # Cancel any pending tasks
            if self.task and not self.task.done():
                self.task.cancel()
                try:
                    await asyncio.wait_for(self.task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            
            # Log shutdown details
            print(f"[{self.symbol}] 🛡️ Grid Bot safely shut down")
            print(f" Reason: {reason}")
            print(f" Total Profits: ${self.total_profits:.2f}")
            print(f" Daily PnL: ${self.daily_pnl:.2f}")
            print(f" Investment Amount: ${self.investment_amount:.2f}")
            print(f" Position Value: ${self.current_position:.2f}")
            
        except Exception as e:
            print(f"[{self.symbol}] ❌ Shutdown error: {e}")

    def calculate_grid_levels(self, current_price=None):
        """Calculate grid buy and sell levels with ENHANCED SAFETY"""
        try:
            if self.price_range["upper"] == 0 or self.price_range["lower"] == 0:
                if current_price:
                    # Auto-calculate range based on current price (±15% with safety limits)
                    range_pct = min(0.15, 0.05 + (self.grid_levels * 0.005))  # Scale with grid levels
                    self.price_range["upper"] = current_price * (1 + range_pct)
                    self.price_range["lower"] = current_price * (1 - range_pct)
                    print(f"[{self.symbol}] 📊 Auto-calculated range: ${self.price_range['lower']:.2f} - ${self.price_range['upper']:.2f}")
                else:
                    raise ValueError("Price range not set and no current price provided")
            
            buy_levels = []
            sell_levels = []
            
            if self.grid_type == "Arithmetic":
                # Arithmetic progression - equal price differences
                price_step = (self.price_range["upper"] - self.price_range["lower"]) / self.grid_levels
                
                for i in range(self.grid_levels):
                    level_price = self.price_range["lower"] + (i * price_step)
                    buy_levels.append(round(level_price, 4))
                    sell_levels.append(round(level_price + price_step, 4))
                    
            else:  # Geometric
                # Geometric progression - equal percentage differences
                ratio = (self.price_range["upper"] / self.price_range["lower"]) ** (1 / self.grid_levels)
                
                for i in range(self.grid_levels):
                    level_price = self.price_range["lower"] * (ratio ** i)
                    buy_levels.append(round(level_price, 4))
                    sell_levels.append(round(level_price * ratio, 4))
            
            # SAFETY: Limit number of levels per side
            max_levels_per_side = min(self.MAX_ORDERS_PER_SIDE, len(buy_levels))
            buy_levels = buy_levels[:max_levels_per_side]
            sell_levels = sell_levels[:max_levels_per_side]
            
            self.grid_orders["buy_levels"] = buy_levels
            self.grid_orders["sell_levels"] = sell_levels
            
            print(f"[{self.symbol}] 📊 Grid levels calculated:")
            print(f" Buy levels: {len(buy_levels)} from ${min(buy_levels):.4f} to ${max(buy_levels):.4f}")
            print(f" Sell levels: {len(sell_levels)} from ${min(sell_levels):.4f} to ${max(sell_levels):.4f}")
            
            return buy_levels, sell_levels
            
        except Exception as e:
            print(f"[{self.symbol}] ❌ Grid levels calculation error: {e}")
            return [], []

    async def initialize_grid_orders(self, current_price):
        """Initialize grid with RISK-BASED INVESTMENT calculation - CORE METHOD"""
        try:
            print(f"[{self.symbol}] 🚀 Initializing Risk-Based Grid at price ${current_price:.4f}")
            
            # ENHANCED: Calculate investment based on available balance and risk
            calculated_investment, available_balance, risk_percentage = await self.calculate_risk_based_investment()
            
            if calculated_investment <= 0:
                await self.safe_shutdown("Failed to calculate investment amount")
                return False
            
            # Update investment parameters
            self.investment_amount = calculated_investment
            self.per_grid_amount = calculated_investment / self.grid_levels
            self.max_position = calculated_investment * self.MAX_POSITION_RATIO
            
            print(f"[{self.symbol}] 💰 Risk-Based Investment Calculation:")
            print(f" Available Balance: ${available_balance:.2f}")
            print(f" Risk Setting: {self.risk_amount_config} ({risk_percentage}% of balance)" if risk_percentage else f" Risk Setting: ${self.risk_amount_config}")
            print(f" Calculated Investment: ${self.investment_amount:.2f}")
            print(f" Per Grid Amount: ${self.per_grid_amount:.2f}")
            print(f" Max Position: ${self.max_position:.2f}")
            print(f" Safety Margin Used: {(self.investment_amount/available_balance)*100:.1f}% of balance")
            
            # Calculate grid levels
            buy_levels, sell_levels = self.calculate_grid_levels(current_price)
            if not buy_levels or not sell_levels:
                await self.safe_shutdown("Failed to calculate grid levels")
                return False
            
            # Place initial orders within calculated investment limit
            orders_placed = 0
            total_investment_used = 0
            order_failures = 0
            
            # ENHANCED: Place buy orders below current price with safety checks
            for buy_price in buy_levels:
                if buy_price < current_price and total_investment_used < self.investment_amount:
                    if self.per_grid_amount <= (self.investment_amount - total_investment_used):
                        success = await self.place_grid_order("buy", buy_price, self.per_grid_amount)
                        if success:
                            orders_placed += 1
                            total_investment_used += self.per_grid_amount
                            self.orders_placed_today += 1
                        else:
                            order_failures += 1
                            if order_failures > 3:  # Stop if too many failures
                                print(f"[{self.symbol}] ⚠️ Too many order failures, stopping initialization")
                                break
            
            # ENHANCED: Place initial sell orders above current price with position safety
            sell_orders_placed = 0
            max_sell_orders = min(3, len([p for p in sell_levels if p > current_price]))
            
            for sell_price in sell_levels:
                if sell_price > current_price and sell_orders_placed < max_sell_orders:
                    # Place small sell orders to start (reduced size for safety)
                    small_sell_amount = self.per_grid_amount * 0.3
                    success = await self.place_grid_order("sell", sell_price, small_sell_amount)
                    if success:
                        sell_orders_placed += 1
                        self.orders_placed_today += 1
            
            if orders_placed == 0:
                await self.safe_shutdown("Failed to place any grid orders")
                return False
            
            self.grid_initialized = True
            self.save_state()
            
            print(f"[{self.symbol}] ✅ Risk-Based Grid initialized successfully")
            print(f" Orders placed: {orders_placed} buy + {sell_orders_placed} sell")
            print(f" Investment used: ${total_investment_used:.2f} / ${self.investment_amount:.2f}")
            print(f" Investment utilization: {(total_investment_used/self.investment_amount)*100:.1f}%")
            print(f" Order failures: {order_failures}")
            
            return True
            
        except Exception as e:
            print(f"[{self.symbol}] ❌ Risk-based grid initialization error: {e}")
            await self.safe_shutdown(f"Grid initialization failed: {e}")
            return False

    async def rebalance_investment_if_needed(self):
        """Periodically rebalance investment based on balance changes"""
        try:
            current_time = time.time()
            # Only rebalance every 30 minutes
            if current_time - self.last_rebalance_time < 1800:
                return False
            
            current_investment, available_balance, risk_percentage = await self.calculate_risk_based_investment()
            
            # Only rebalance if investment changes significantly (>25%)
            if self.investment_amount > 0:
                change_pct = abs(current_investment - self.investment_amount) / self.investment_amount
                
                if change_pct > 0.25:  # 25% change threshold
                    print(f"[{self.symbol}] 📊 Balance changed significantly, rebalancing...")
                    print(f" Old investment: ${self.investment_amount:.2f}")
                    print(f" New investment: ${current_investment:.2f}")
                    print(f" Available balance: ${available_balance:.2f}")
                    print(f" Change: {change_pct*100:.1f}%")
                    
                    # Update investment parameters
                    self.investment_amount = current_investment
                    self.per_grid_amount = current_investment / self.grid_levels
                    self.max_position = current_investment * self.MAX_POSITION_RATIO
                    
                    # Save updated state
                    self.save_state()
                    
                    print(f"[{self.symbol}] ✅ Investment rebalanced to ${current_investment:.2f}")
                    self.last_rebalance_time = current_time
                    return True
            
            self.last_rebalance_time = current_time
            return False
            
        except Exception as e:
            print(f"[{self.symbol}] ❌ Rebalance error: {e}")
            return False

    async def place_grid_order(self, side, price, quantity):
        """Place a single grid order with ENHANCED ERROR HANDLING"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Convert quantity to integer (required by exchange)
                size = max(1, int(round(quantity / price)))  # Convert USD to contracts
                
                # SAFETY: Validate order parameters
                if size <= 0:
                    print(f"[{self.symbol}] ❌ Invalid order size: {size}")
                    return False
                
                if price <= 0:
                    print(f"[{self.symbol}] ❌ Invalid order price: {price}")
                    return False
                
                order_payload = {
                    "product_id": self.symbol_to_pid[self.symbol],
                    "order_type": "limit_order",
                    "side": side,
                    "size": size,
                    "limit_price": round(price, 4),  # More precision
                    "reduce_only": False,
                    "client_order_id": f"grid_{side}_{int(price*10000)}_{self.instance_id}_{int(time.time())}",
                    "time_in_force": "gtc"  # Good till cancelled
                }
                
                response = await asyncio.wait_for(
                    self.delta_private("POST", "/v2/orders", body=order_payload),
                    timeout=15.0
                )
                
                if response.get('success'):
                    order_id = response.get('result', {}).get('id')
                    
                    if not order_id:
                        print(f"[{self.symbol}] ⚠️ Order placed but no ID returned")
                        return False
                    
                    # Store order in active orders
                    self.grid_orders["active_orders"][order_id] = {
                        "side": side,
                        "price": price,
                        "size": size,
                        "quantity": quantity,
                        "timestamp": time.time(),
                        "client_order_id": order_payload["client_order_id"],
                        "attempt": attempt + 1
                    }
                    
                    print(f"[{self.symbol}] ✅ Grid {side.upper()} order placed: {size} @ ${price:.4f} (attempt {attempt + 1})")
                    return True
                else:
                    error = response.get('error', {})
                    error_msg = error.get('message', 'Unknown error')
                    print(f"[{self.symbol}] ❌ Grid order failed (attempt {attempt + 1}): {error_msg}")
                    
                    if attempt == max_retries - 1:
                        # Final attempt failed
                        return False
                    
                    # Wait before retry with exponential backoff
                    await asyncio.sleep(2 ** attempt)
                    
            except asyncio.TimeoutError:
                print(f"[{self.symbol}] ⚠️ Grid order timeout (attempt {attempt + 1})")
                if attempt == max_retries - 1:
                    return False
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                print(f"[{self.symbol}] ❌ Grid order exception (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    return False
                await asyncio.sleep(2 ** attempt)
        
        return False

    async def check_filled_orders(self):
        """Check for filled grid orders and manage the grid - ENHANCED SAFETY"""
        try:
            filled_orders = []
            order_check_failures = 0
            
            for order_id, order_info in list(self.grid_orders["active_orders"].items()):
                try:
                    # Check order status with timeout
                    order_response = await asyncio.wait_for(
                        self.delta_private("GET", f"/v2/orders/{order_id}"),
                        timeout=10.0
                    )
                    
                    if order_response.get('success'):
                        order_data = order_response.get('result', {})
                        state = order_data.get('state', '')
                        
                        if state == 'filled':
                            filled_orders.append((order_id, order_info, order_data))
                            # Remove from active orders
                            del self.grid_orders["active_orders"][order_id]
                        elif state in ['cancelled', 'rejected', 'expired']:
                            print(f"[{self.symbol}] ⚠️ Grid order {order_id} {state}")
                            del self.grid_orders["active_orders"][order_id]
                    else:
                        order_check_failures += 1
                        if order_check_failures > 5:  # Too many failures
                            print(f"[{self.symbol}] ⚠️ Too many order check failures, stopping checks")
                            break
                            
                except asyncio.TimeoutError:
                    print(f"[{self.symbol}] ⚠️ Order status check timeout for {order_id}")
                    order_check_failures += 1
                except Exception as e:
                    print(f"[{self.symbol}] ❌ Order check error for {order_id}: {e}")
                    order_check_failures += 1
            
            # Process filled orders
            for order_id, order_info, order_data in filled_orders:
                try:
                    await self.process_filled_order(order_id, order_info, order_data)
                except Exception as e:
                    print(f"[{self.symbol}] ❌ Process filled order error: {e}")
            
            return len(filled_orders)
            
        except Exception as e:
            print(f"[{self.symbol}] ❌ Check filled orders error: {e}")
            return 0

    async def process_filled_order(self, order_id, order_info, order_data):
        """Process a filled grid order and place corresponding counter-order - ENHANCED"""
        try:
            side = order_info["side"]
            fill_price = float(order_data.get('avg_fill_price', order_info["price"]))
            fill_size = float(order_data.get('size', order_info["size"]))
            
            print(f"[{self.symbol}] 🎯 Grid order filled: {side.upper()} {fill_size} @ ${fill_price:.4f}")
            
            profit = 0.0
            
            # ENHANCED: Process buy fill
            if side == "buy":
                self.current_position += (fill_size * fill_price)
                
                # SAFETY: Check position limits
                if abs(self.current_position) > self.max_position:
                    print(f"[{self.symbol}] ⚠️ Position limit reached: ${abs(self.current_position):.2f} > ${self.max_position:.2f}")
                    # Still place counter-order but with reduced size
                
                # Place corresponding sell order with enhanced profit calculation
                profit_pct = self.get_grid_profit_percentage()
                sell_price = fill_price * (1 + profit_pct)
                sell_quantity = fill_size * fill_price * 0.995  # Account for fees (0.5% safety)
                
                success = await self.place_grid_order("sell", sell_price, sell_quantity)
                if success:
                    print(f"[{self.symbol}] 📈 Counter-sell placed @ ${sell_price:.4f} (+{profit_pct*100:.2f}%)")
                
            else:  # sell fill
                self.current_position -= (fill_size * fill_price)
                
                # Calculate profit (enhanced calculation)
                profit = self.calculate_trade_profit(fill_price, fill_size, "sell")
                self.total_profits += profit
                self.total_trades += 1
                self.daily_pnl += profit  # Update daily P&L
                
                # Place corresponding buy order
                profit_pct = self.get_grid_profit_percentage()
                buy_price = fill_price * (1 - profit_pct)
                buy_quantity = fill_size * fill_price
                
                success = await self.place_grid_order("buy", buy_price, buy_quantity)
                if success:
                    print(f"[{self.symbol}] 📉 Counter-buy placed @ ${buy_price:.4f} (-{profit_pct*100:.2f}%)")
                
                print(f"[{self.symbol}] 💰 Grid profit: ${profit:.4f} (Total: ${self.total_profits:.2f})")
            
            # Record filled order with enhanced data
            self.grid_orders["filled_orders"].append({
                "order_id": order_id,
                "side": side,
                "price": fill_price,
                "size": fill_size,
                "timestamp": time.time(),
                "profit": profit if side == "sell" else 0,
                "position_after": self.current_position
            })
            
            # Keep only recent filled orders (memory management)
            if len(self.grid_orders["filled_orders"]) > 1000:
                self.grid_orders["filled_orders"] = self.grid_orders["filled_orders"][-500:]
            
            # ENHANCED: Log to journal with better data
            if TRADE_JOURNAL_AVAILABLE and side == "sell" and profit > 0:
                add_trade({
                    'symbol': self.symbol,
                    'side': 'grid_cycle',
                    'size': fill_size,
                    'entry_price': fill_price * (1 - self.get_grid_profit_percentage()),
                    'exit_price': fill_price,
                    'leverage': 1,
                    'profit_loss': profit,
                    'strategy': 'Grid Bot',
                    'status': 'Closed',
                    'timestamp': time.time(),
                    'instance_id': self.instance_id
                })
            
            self.save_state()
            
        except Exception as e:
            print(f"[{self.symbol}] ❌ Process filled order error: {e}")

    def get_grid_profit_percentage(self):
        """Calculate profit percentage per grid level with SAFETY BOUNDS"""
        try:
            if self.price_range["upper"] <= 0 or self.price_range["lower"] <= 0:
                return max(self.MIN_GRID_PROFIT_PCT, 0.01)  # Default 1%
            
            price_range = self.price_range["upper"] - self.price_range["lower"]
            grid_step = price_range / max(1, self.grid_levels)
            avg_price = (self.price_range["upper"] + self.price_range["lower"]) / 2
            
            if avg_price <= 0:
                return self.MIN_GRID_PROFIT_PCT
            
            # Calculate profit as a percentage of grid step
            profit_pct = (grid_step / avg_price) * 0.6  # 60% of grid step as profit
            
            # SAFETY: Enforce bounds
            profit_pct = max(self.MIN_GRID_PROFIT_PCT, min(self.MAX_GRID_PROFIT_PCT, profit_pct))
            
            return profit_pct
            
        except Exception as e:
            print(f"[{self.symbol}] ❌ Grid profit calculation error: {e}")
            return self.MIN_GRID_PROFIT_PCT

    def calculate_trade_profit(self, sell_price, size, side):
        """Calculate profit from a completed grid cycle - ENHANCED"""
        if side == "sell":
            try:
                # Enhanced profit calculation
                profit_per_unit = sell_price * self.get_grid_profit_percentage()
                gross_profit = profit_per_unit * size
                
                # Account for trading fees (estimated)
                estimated_fees = (sell_price * size) * 0.001  # 0.1% estimated fees
                net_profit = gross_profit - estimated_fees
                
                return max(0, net_profit)  # Ensure non-negative
            except Exception as e:
                print(f"[{self.symbol}] ❌ Profit calculation error: {e}")
                return 0
        return 0

    async def check_stop_loss(self, current_price):
        """Check if stop-loss should be triggered - ENHANCED SAFETY"""
        try:
            # DAILY LOSS CHECK - CRITICAL
            if self.daily_pnl < -(self.investment_amount * self.MAX_DAILY_DRAWDOWN):
                print(f"[{self.symbol}] 🚨 DAILY LOSS LIMIT TRIGGERED: ${self.daily_pnl:.2f}")
                await self.safe_shutdown(f"Daily loss limit exceeded: ${self.daily_pnl:.2f}")
                return True
            
            # POSITION-BASED STOP LOSS
            if not self.grid_initialized or abs(self.current_position) < 10:  # Minimum position threshold
                return False
            
            position_value = abs(self.current_position)
            if position_value == 0:
                return False
            
            # Calculate unrealized P&L
            avg_position_price = position_value / abs(self.current_position / current_price)
            
            if self.current_position > 0:  # Long position
                loss_pct = (avg_position_price - current_price) / avg_position_price
            else:  # Short position  
                loss_pct = (current_price - avg_position_price) / avg_position_price
            
            if loss_pct > (self.stop_loss_percentage / 100):
                print(f"[{self.symbol}] 🚨 POSITION STOP LOSS TRIGGERED: {loss_pct*100:.2f}%")
                await self.safe_shutdown(f"Stop loss triggered: {loss_pct*100:.2f}%")
                return True
            
            return False
            
        except Exception as e:
            print(f"[{self.symbol}] ❌ Stop loss check error: {e}")
            return False

    async def run(self):
        """Main Grid Bot execution loop with ENHANCED SAFETY and RISK-BASED INVESTMENT"""
        print(f"[{self.symbol}] 🤖 Starting Risk-Based Grid Bot - Instance: {self.instance_id}")
        
        loop_count = 0
        consecutive_errors = 0
        last_price_update = 0
        
        self.status["info"] = f"Risk-Based Grid Bot Active - {self.grid_levels} levels"
        self.status["details"] = {
            "loop": loop_count,
            "grid_initialized": self.grid_initialized,
            "active_orders": len(self.grid_orders.get("active_orders", {})),
            "total_profits": self.total_profits,
            "total_trades": self.total_trades,
            "current_position": self.current_position,
            "daily_pnl": self.daily_pnl,
            "investment_amount": self.investment_amount
        }
        
        try:
            while self.is_running:
                print(f"[{self.symbol}] 🔄 Grid Loop {loop_count + 1}")
                loop_count += 1
                
                # SAFETY: Daily loss protection - CRITICAL CHECK
                if self.daily_pnl < -(self.investment_amount * self.MAX_DAILY_DRAWDOWN):
                    await self.safe_shutdown(f"Daily loss limit exceeded: ${self.daily_pnl:.2f}")
                    break
                
                # SAFETY: Order limit per day
                if self.orders_placed_today > 100:  # Max 100 orders per day
                    print(f"[{self.symbol}] 🚫 Daily order limit reached: {self.orders_placed_today}")
                    await asyncio.sleep(300)  # Wait 5 minutes
                    continue
                
                try:
                    # Get current market price with timeout
                    market_data = await asyncio.wait_for(
                        self.get_current_market_price(), 
                        timeout=15.0
                    )
                    current_price = float(market_data.get("mark_price", 0))
                    
                    if current_price <= 0:
                        consecutive_errors += 1
                        print(f"[{self.symbol}] ⚠️ Invalid price data (errors: {consecutive_errors})")
                        if consecutive_errors > 5:
                            await self.safe_shutdown("Too many price data failures")
                            break
                        await asyncio.sleep(30)
                        continue
                    
                    consecutive_errors = 0  # Reset error counter
                    self.last_price = current_price
                    last_price_update = time.time()
                    
                except asyncio.TimeoutError:
                    consecutive_errors += 1
                    print(f"[{self.symbol}] ⚠️ Price fetch timeout (errors: {consecutive_errors})")
                    if consecutive_errors > 3:
                        await self.safe_shutdown("Price fetch timeouts")
                        break
                    await asyncio.sleep(30)
                    continue
                except Exception as e:
                    consecutive_errors += 1
                    print(f"[{self.symbol}] ❌ Price fetch error: {e} (errors: {consecutive_errors})")
                    if consecutive_errors > 5:
                        await self.safe_shutdown(f"Price fetch errors: {e}")
                        break
                    await asyncio.sleep(30)
                    continue
                
                # Initialize grid if not done or investment is 0 (first time)
                if not self.grid_initialized or self.investment_amount == 0:
                    success = await self.initialize_grid_orders(current_price)
                    if not success:
                        break  # Safe shutdown was called
                
                # Periodically rebalance investment based on balance changes
                if loop_count % 20 == 0:  # Every 20 loops (~15-20 minutes)
                    try:
                        rebalanced = await self.rebalance_investment_if_needed()
                        if rebalanced:
                            # If investment changed significantly, consider reinitializing grid
                            active_orders = len(self.grid_orders.get("active_orders", {}))
                            if active_orders < 3:  # Only if very few orders remain
                                print(f"[{self.symbol}] 🔄 Considering grid reinitialization due to investment change")
                                # Grid will be reinitialized in next iteration if needed
                    except Exception as e:
                        print(f"[{self.symbol}] ⚠️ Rebalance check error: {e}")
                
                # Check for filled orders
                try:
                    filled_count = await self.check_filled_orders()
                except Exception as e:
                    print(f"[{self.symbol}] ❌ Check filled orders error: {e}")
                    filled_count = 0
                
                # Check stop loss
                try:
                    stop_loss_triggered = await self.check_stop_loss(current_price)
                    if stop_loss_triggered:
                        break  # Safe shutdown was called
                except Exception as e:
                    print(f"[{self.symbol}] ❌ Stop loss check error: {e}")
                
                # SAFETY: Check if we have too few active orders (might indicate issues)
                active_order_count = len(self.grid_orders.get("active_orders", {}))
                if self.grid_initialized and active_order_count < 2:
                    print(f"[{self.symbol}] ⚠️ Very few active orders ({active_order_count}), monitoring closely")
                
                # Update status with comprehensive information
                self.status["details"].update({
                    "loop": loop_count,
                    "current_price": current_price,
                    "active_orders": active_order_count,
                    "filled_orders": filled_count,
                    "total_profits": self.total_profits,
                    "daily_pnl": self.daily_pnl,
                    "position_value": self.current_position,
                    "investment_amount": self.investment_amount,
                    "orders_today": self.orders_placed_today,
                    "consecutive_errors": consecutive_errors
                })
                
                if filled_count > 0:
                    self.status["info"] = f"Grid Bot Active - {filled_count} orders filled | Profit: ${self.total_profits:.2f} | Investment: ${self.investment_amount:.2f}"
                else:
                    self.status["info"] = f"Grid Bot monitoring - {active_order_count} active orders | Daily PnL: ${self.daily_pnl:.2f} | Investment: ${self.investment_amount:.2f}"
                
                # Save state periodically
                if loop_count % 10 == 0:  # Every 10 loops
                    self.save_state()
                
                # ENHANCED: Variable sleep based on market activity
                if filled_count > 0:
                    await asyncio.sleep(15)  # More frequent checks after fills
                else:
                    await asyncio.sleep(45)  # Longer intervals when quiet
                
        except asyncio.CancelledError:
            print(f"[{self.symbol}] Grid Bot cancelled")
            raise
        except Exception as e:
            print(f"[{self.symbol}] ❌ Grid Bot critical error: {e}")
            await self.safe_shutdown(f"Critical error: {e}")
        finally:
            self.save_state()
            print(f"[{self.symbol}] Grid Bot stopped")

    async def get_current_market_price(self):
        """Get current market price with enhanced error handling"""
        try:
            pid = self.symbol_to_pid.get(self.symbol)
            if not pid:
                raise ValueError("Product ID not found")
            
            response = await self.delta_private("GET", f"/v2/tickers", params={"product_id": pid})
            
            if response.get('success'):
                result = response.get('result')
                if isinstance(result, list) and result:
                    return result[0]
                elif isinstance(result, dict):
                    return result
            
            raise ValueError("Invalid ticker response")
            
        except Exception as e:
            print(f"[{self.symbol}] ❌ Market price error: {e}")
            raise

    def get_grid_performance(self):
        """Get grid performance metrics with enhanced data"""
        try:
            filled_orders = self.grid_orders.get("filled_orders", [])
            if not filled_orders:
                return {
                    "total_profits": 0,
                    "total_trades": 0,
                    "avg_profit_per_trade": 0,
                    "profit_percentage": 0,
                    "active_orders": len(self.grid_orders.get("active_orders", {})),
                    "position_value": self.current_position,
                    "daily_pnl": self.daily_pnl,
                    "investment_amount": self.investment_amount,
                    "win_rate": 0
                }
            
            sell_orders = [o for o in filled_orders if o["side"] == "sell"]
            profitable_trades = [o for o in sell_orders if o.get("profit", 0) > 0]
            
            total_profit = sum(order.get("profit", 0) for order in sell_orders)
            trade_count = len(sell_orders)
            
            return {
                "total_profits": total_profit,
                "total_trades": trade_count,
                "avg_profit_per_trade": total_profit / max(1, trade_count),
                "profit_percentage": (total_profit / max(self.investment_amount, 1)) * 100,
                "active_orders": len(self.grid_orders.get("active_orders", {})),
                "position_value": self.current_position,
                "daily_pnl": self.daily_pnl,
                "investment_amount": self.investment_amount,
                "win_rate": (len(profitable_trades) / max(1, trade_count)) * 100,
                "orders_placed_today": self.orders_placed_today,
                "grid_efficiency": min(100, (trade_count / max(1, self.grid_levels)) * 100)
            }
        except Exception as e:
            print(f"[{self.symbol}] ❌ Performance calculation error: {e}")
            return {"error": str(e)}

    def start(self):
        """Start Grid Bot with safety validation"""
        if self.is_running:
            print(f"[{self.symbol}] Grid Bot already running")
            return
        
        # Final safety check before starting
        if not hasattr(self, 'symbol_to_pid') or self.symbol not in self.symbol_to_pid:
            print(f"[{self.symbol}] ❌ Cannot start: Product ID not found")
            return
        
        self.is_running = True
        self.status["running"] = True
        self.status["error"] = None
        self.task = asyncio.create_task(self.run())
        print(f"[{self.symbol}] 🤖 Risk-Based Grid Bot started safely - Instance: {self.instance_id}")

    async def stop(self):
        """Stop Grid Bot with enhanced cleanup"""
        print(f"[{self.symbol}] 🛑 Stopping Risk-Based Grid Bot")
        self.is_running = False
        self.status["running"] = False
        
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await asyncio.wait_for(self.task, timeout=10.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                print(f"[{self.symbol}] ⚠️ Task cancellation timeout")
        
        # Save final state
        self.save_state()
        
        # Log final statistics
        active_orders = len(self.grid_orders.get("active_orders", {}))
        print(f"[{self.symbol}] ✅ Risk-Based Grid Bot stopped safely")
        print(f" Final Stats:")
        print(f"  Investment Amount: ${self.investment_amount:.2f}")
        print(f"  Total Profits: ${self.total_profits:.2f}")
        print(f"  Daily PnL: ${self.daily_pnl:.2f}")
        print(f"  Active Orders: {active_orders}")
        print(f"  Orders Today: {self.orders_placed_today}")
        
        if active_orders > 0:
            print(f" ⚠️ {active_orders} active orders remain - use emergency_close_all_orders() if needed")

    def get_status(self):
        """Get current strategy status with enhanced information"""
        status = self.status.copy()
        status["running"] = self.is_running
        status["grid_initialized"] = self.grid_initialized
        status["total_profits"] = self.total_profits
        status["daily_pnl"] = self.daily_pnl
        status["current_position"] = self.current_position
        status["investment_amount"] = self.investment_amount
        status["active_orders"] = len(self.grid_orders.get("active_orders", {}))
        status["orders_today"] = self.orders_placed_today
        status["risk_settings"] = {
            "risk_type": self.risk_type,
            "risk_amount": self.risk_amount_config
        }
        status["safety_limits"] = {
            "max_investment": self.MAX_INVESTMENT_AMOUNT,
            "max_daily_drawdown": self.MAX_DAILY_DRAWDOWN,
            "max_grid_levels": self.MAX_GRID_LEVELS,
            "max_risk_percent": self.MAX_RISK_PERCENT,
            "max_position_ratio": self.MAX_POSITION_RATIO
        }
        return status

    async def emergency_close_all_orders(self):
        """Enhanced emergency closure with comprehensive cleanup"""
        try:
            print(f"[{self.symbol}] 🚨 EMERGENCY: Closing all grid positions and orders")
            
            cancelled_orders = 0
            failed_cancellations = 0
            
            # Cancel all active orders with individual error handling
            for order_id in list(self.grid_orders.get("active_orders", {}).keys()):
                try:
                    cancel_response = await asyncio.wait_for(
                        self.delta_private("DELETE", f"/v2/orders/{order_id}"),
                        timeout=10.0
                    )
                    if cancel_response.get('success'):
                        cancelled_orders += 1
                    else:
                        failed_cancellations += 1
                        print(f"[{self.symbol}] ⚠️ Failed to cancel order {order_id}")
                except Exception as e:
                    failed_cancellations += 1
                    print(f"[{self.symbol}] ❌ Cancel order error: {e}")
            
            print(f"[{self.symbol}] 📋 Emergency cleanup: {cancelled_orders} cancelled, {failed_cancellations} failed")
            
            # Close position at market if needed (enhanced)
            if abs(self.current_position) > 10:  # Only if significant position
                try:
                    side = "sell" if self.current_position > 0 else "buy"
                    size = max(1, abs(int(self.current_position / self.last_price)))
                    
                    market_order = {
                        "product_id": self.symbol_to_pid[self.symbol],
                        "order_type": "market_order",
                        "side": side,
                        "size": size,
                        "reduce_only": True,
                        "client_order_id": f"emergency_close_{self.instance_id}_{int(time.time())}"
                    }
                    
                    close_response = await self.delta_private("POST", "/v2/orders", body=market_order)
                    
                    if close_response.get('success'):
                        print(f"[{self.symbol}] ✅ Emergency position closed: {side} {size}")
                    else:
                        print(f"[{self.symbol}] ❌ Failed to close position")
                        
                except Exception as e:
                    print(f"[{self.symbol}] ❌ Emergency close position error: {e}")
            
            # Reset grid state but preserve history
            self.grid_orders = {
                "buy_levels": [],
                "sell_levels": [],
                "active_orders": {},
                "filled_orders": self.grid_orders.get("filled_orders", []),  # Keep history
                "profits": self.grid_orders.get("profits", [])  # Keep history
            }
            self.grid_initialized = False
            self.current_position = 0
            self.save_state()
            
            print(f"[{self.symbol}] ⚠️ Grid Bot emergency shutdown completed")
            print(f" Investment preserved: ${self.investment_amount:.2f}")
            print(f" Total profits preserved: ${self.total_profits:.2f}")
            
        except Exception as e:
            print(f"[{self.symbol}] ❌ Emergency close error: {e}")
