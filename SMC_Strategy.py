# SMC_Strategy.py - Professional Smart Money Concepts Strategy with Integer Validation & Enhanced Safety

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
        """Start the strategy - simple and clean"""
        if self.is_running:
            print(f"[{self.symbol}] Strategy already running")
            return
        
        print(f"[{self.symbol}] Starting strategy...")
        self.is_running = True
        self.status["running"] = True
        self.status["info"] = "Strategy started"
        self.task = asyncio.create_task(self.run())

    async def stop(self):
        """Stop the strategy - EXACTLY like your old working code"""
        if not self.is_running:
            print(f"[{self.symbol}] Strategy not running")
            return
        
        print(f"[{self.symbol}] Stopping strategy...")
        self.is_running = False
        
        if self.task and not self.task.done():
            print(f"[{self.symbol}] Cancelling strategy task...")
            self.task.cancel()
            try:
                # Short timeout like your old code
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

class SMC_Strategy(Strategy):
    # SAFETY CONSTANTS - CRITICAL ADDITIONS WITH INTEGER ENFORCEMENT
    MAX_POSITION_SIZE = 100        # Hard cap on position size (INTEGER)
    MIN_SL_DISTANCE = 0.20         # Minimum stop loss distance ($)
    MAX_DAILY_DRAWDOWN = 0.05      # 5% max daily loss
    SAFETY_MARGIN_RATIO = 0.60     # Use only 60% of available margin
    MAX_RISK_PERCENT = 10.0        # Cap risk at 10%
    MIN_POSITION_SIZE = 1          # Minimum position size (INTEGER)
    MAX_LEVERAGE = 50              # Maximum leverage cap (INTEGER)
    MIN_LEVERAGE = 1               # Minimum leverage (INTEGER)
    
    def __init__(self, symbol, config, delta_private, products, symbol_to_pid, api_key, api_secret):
        super().__init__(symbol, config, delta_private, products, symbol_to_pid)
        
        # SMC Core Settings
        self.api_key = api_key
        self.api_secret = api_secret
        self.instance_id = str(uuid.uuid4())[:8]
        
        # Enhanced state management
        self.last_signal = None
        self.last_trade_time = 0
        self.signal_cooldown = 120  # 2 min cooldown for SMC
        self.bot_start_time = time.time()
        self.restart_protection_time = 180  # 3 min restart protection
        
        # SAFETY TRACKING - NEW ADDITIONS
        self.daily_pnl = 0.0           # Track daily P&L
        self.daily_start_balance = 0.0  # Starting balance for day
        self.trade_count_today = 0     # Count daily trades
        
        # SMC Configuration
        self.trading_mode = config.get("tradingMode", "Auto")
        self.account_balance = config.get("account_balance", 10000)
        self.timeframe = config.get("timeframe", "15m")
        
        # SMC Parameters
        self.smc_config = config.get("smcConfig", {})
        self.lookback_periods = int(self.smc_config.get("lookbackPeriods", 50))  # ENSURE INTEGER
        self.ob_strength = self.smc_config.get("orderBlockStrength", "Medium")  # Weak, Medium, Strong
        self.fvg_min_size = float(self.smc_config.get("fvgMinSize", 0.1))  # Minimum FVG size %
        self.structure_confirmation = int(self.smc_config.get("structureConfirmation", 3))  # ENSURE INTEGER
        
        # Risk Management
        if self.trading_mode == "Auto":
            self.risk_appetite = config.get("riskAppetite", "Moderate")
            self.auto_calculated = config.get("autoCalculated", {})
        else:
            self.risk_management = config.get("riskManagement", {})
        
        # SMC State Variables
        self.market_structure = "RANGING"  # BULLISH, BEARISH, RANGING
        self.last_higher_high = None
        self.last_higher_low = None
        self.last_lower_high = None
        self.last_lower_low = None
        self.order_blocks = []  # List of detected order blocks
        self.fair_value_gaps = []  # List of detected FVGs
        self.liquidity_zones = []  # Liquidity sweep zones
        self.bos_detected = False  # Break of Structure
        self.choch_detected = False  # Change of Character
        
        # Candle buffer for analysis
        self.candle_buffer = []
        
        print(f"[{self.symbol}] 🎯 SMC Strategy Initialized - Instance: {self.instance_id}")
        print(f" Mode: {self.trading_mode} | Structure Lookback: {self.lookback_periods}")
        print(f" Order Block Strength: {self.ob_strength} | FVG Min Size: {self.fvg_min_size}%")
        print(f" 🛡️ Safety: Max Position: {self.MAX_POSITION_SIZE}, Max Risk: {self.MAX_RISK_PERCENT}%")
        print(f" 🔢 Integer Safety: Position & Leverage guaranteed as integers")
        
        # VALIDATE CONFIGURATION - CRITICAL SAFETY CHECK
        self.validate_config()
        
        self.load_state()

    def validate_config(self):
        """Validate and sanitize user configuration - CRITICAL SAFETY METHOD WITH INTEGER ENFORCEMENT"""
        print(f"[{self.symbol}] 🛡️ Validating SMC configuration with integer enforcement...")
        
        # Validate FVG min size
        if self.fvg_min_size <= 0 or self.fvg_min_size > 5.0:
            print(f"[{self.symbol}] ⚠️ FVG min size {self.fvg_min_size}% invalid, using 0.5%")
            self.fvg_min_size = 0.5
        
        # Validate order block strength
        if self.ob_strength not in ['Weak', 'Medium', 'Strong']:
            print(f"[{self.symbol}] ⚠️ Invalid OB strength '{self.ob_strength}', using 'Medium'")
            self.ob_strength = 'Medium'
        
        # Validate lookback periods - ENSURE INTEGER
        try:
            self.lookback_periods = int(self.lookback_periods)
        except (ValueError, TypeError):
            print(f"[{self.symbol}] ⚠️ Invalid lookback periods, using 50")
            self.lookback_periods = 50
        
        if self.lookback_periods < 10 or self.lookback_periods > 200:
            print(f"[{self.symbol}] ⚠️ Lookback periods {self.lookback_periods} invalid, using 50")
            self.lookback_periods = 50
        
        # Validate structure confirmation - ENSURE INTEGER
        try:
            self.structure_confirmation = int(self.structure_confirmation)
        except (ValueError, TypeError):
            print(f"[{self.symbol}] ⚠️ Invalid structure confirmation, using 3")
            self.structure_confirmation = 3
        
        if self.structure_confirmation < 1 or self.structure_confirmation > 10:
            print(f"[{self.symbol}] ⚠️ Structure confirmation {self.structure_confirmation} invalid, using 3")
            self.structure_confirmation = 3
        
        # Validate risk management
        if self.trading_mode != "Auto" and hasattr(self, 'risk_management'):
            risk_str = self.risk_management.get("riskAmount", "1%")
            if isinstance(risk_str, str) and "%" in risk_str:
                try:
                    risk_pct = float(risk_str.replace("%", ""))
                    if risk_pct > self.MAX_RISK_PERCENT:
                        print(f"[{self.symbol}] ⚠️ Risk {risk_pct}% too high, capping at {self.MAX_RISK_PERCENT}%")
                        self.risk_management["riskAmount"] = f"{self.MAX_RISK_PERCENT}%"
                    elif risk_pct <= 0:
                        print(f"[{self.symbol}] ⚠️ Risk {risk_pct}% too low, setting to 1%")
                        self.risk_management["riskAmount"] = "1%"
                except ValueError:
                    print(f"[{self.symbol}] ⚠️ Invalid risk format '{risk_str}', using 1%")
                    self.risk_management["riskAmount"] = "1%"
        
        # Validate timeframe
        valid_timeframes = ['1m', '5m', '15m', '30m', '1H', '4H', '1D']
        if self.timeframe not in valid_timeframes:
            print(f"[{self.symbol}] ⚠️ Invalid timeframe '{self.timeframe}', using '15m'")
            self.timeframe = '15m'
        
        print(f"[{self.symbol}] ✅ SMC Configuration validated and sanitized")
        print(f" Final Integer Parameters: Lookback: {self.lookback_periods}, Structure: {self.structure_confirmation}")

    def load_state(self):
        """Load SMC strategy state"""
        try:
            state = load_strategy_state(self.symbol)
            self.last_signal = state.get("last_signal", None)
            self.last_trade_time = state.get("last_trade_time", 0)
            self.market_structure = state.get("market_structure", "RANGING")
            self.order_blocks = state.get("order_blocks", [])
            self.fair_value_gaps = state.get("fair_value_gaps", [])
            self.daily_pnl = state.get("daily_pnl", 0.0)
            self.trade_count_today = state.get("trade_count_today", 0)
            
            # Restart protection
            time_since_last_trade = time.time() - self.last_trade_time
            if time_since_last_trade < self.restart_protection_time:
                print(f"[{self.symbol}] 🛡️ SMC RESTART PROTECTION: {self.restart_protection_time - time_since_last_trade:.0f}s remaining")
            
            print(f"[{self.symbol}] ✅ SMC State loaded - Structure: {self.market_structure}")
            print(f" Daily PnL: ${self.daily_pnl:.2f} | Trades Today: {self.trade_count_today}")
        except Exception as e:
            print(f"[{self.symbol}] ⚠️ SMC State load error: {e}")

    def save_state(self):
        """Save SMC strategy state"""
        try:
            state = {
                "last_signal": self.last_signal,
                "last_trade_time": self.last_trade_time,
                "market_structure": self.market_structure,
                "order_blocks": self.order_blocks[-10:],  # Keep last 10 OBs
                "fair_value_gaps": self.fair_value_gaps[-10:],  # Keep last 10 FVGs
                "daily_pnl": self.daily_pnl,
                "trade_count_today": self.trade_count_today,
                "instance_id": self.instance_id,
                "saved_at": time.time()
            }
            save_strategy_state(self.symbol, state)
        except Exception as e:
            print(f"[{self.symbol}] ❌ SMC State save error: {e}")

    def identify_swing_points(self, highs, lows, strength=3):
        """Identify swing highs and lows for market structure with INTEGER STRENGTH"""
        # ENSURE INTEGER STRENGTH
        strength = int(strength) if strength >= 1 else 3
        
        swing_highs = []
        swing_lows = []
        
        for i in range(strength, len(highs) - strength):
            # Swing High: current high is higher than 'strength' bars on each side
            is_swing_high = all(highs[i] > highs[j] for j in range(i-strength, i)) and \
                          all(highs[i] > highs[j] for j in range(i+1, i+strength+1))
            
            # Swing Low: current low is lower than 'strength' bars on each side
            is_swing_low = all(lows[i] < lows[j] for j in range(i-strength, i)) and \
                         all(lows[i] < lows[j] for j in range(i+1, i+strength+1))
            
            if is_swing_high:
                swing_highs.append({'index': i, 'price': highs[i]})
            
            if is_swing_low:
                swing_lows.append({'index': i, 'price': lows[i]})
        
        return swing_highs, swing_lows

    def analyze_market_structure(self, df):
        """Analyze market structure for BOS/CHoCH detection"""
        if len(df) < 20:
            return "RANGING"
        
        highs = df['high'].values
        lows = df['low'].values
        
        # Get swing points with integer strength parameter
        swing_highs, swing_lows = self.identify_swing_points(highs, lows, self.structure_confirmation)
        
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return "RANGING"
        
        # Check for Higher Highs and Higher Lows (Bullish Structure)
        recent_highs = swing_highs[-3:] if len(swing_highs) >= 3 else swing_highs
        recent_lows = swing_lows[-3:] if len(swing_lows) >= 3 else swing_lows
        
        # Bullish Structure Check
        if len(recent_highs) >= 2 and len(recent_lows) >= 2:
            hh_condition = recent_highs[-1]['price'] > recent_highs[-2]['price']
            hl_condition = recent_lows[-1]['price'] > recent_lows[-2]['price']
            
            if hh_condition and hl_condition:
                self.last_higher_high = recent_highs[-1]['price']
                self.last_higher_low = recent_lows[-1]['price']
                return "BULLISH"
        
        # Bearish Structure Check
        if len(recent_highs) >= 2 and len(recent_lows) >= 2:
            lh_condition = recent_highs[-1]['price'] < recent_highs[-2]['price']
            ll_condition = recent_lows[-1]['price'] < recent_lows[-2]['price']
            
            if lh_condition and ll_condition:
                self.last_lower_high = recent_highs[-1]['price']
                self.last_lower_low = recent_lows[-1]['price']
                return "BEARISH"
        
        return "RANGING"

    def detect_order_blocks(self, df):
        """Detect institutional order blocks"""
        new_order_blocks = []
        
        if len(df) < 10:
            return new_order_blocks
        
        for i in range(5, len(df) - 2):
            current_candle = df.iloc[i]
            next_candle = df.iloc[i + 1]
            
            # Bullish Order Block: Big red candle followed by gap up and strong bullish move
            if (current_candle['close'] < current_candle['open'] and  # Red candle
                next_candle['open'] > current_candle['close'] and  # Gap up
                next_candle['close'] > next_candle['open']):  # Green candle
                
                # Check for strong bullish move (price moves significantly up)
                move_size = (next_candle['close'] - current_candle['low']) / current_candle['low']
                
                strength_threshold = {
                    'Weak': 0.003,  # 0.3%
                    'Medium': 0.007,  # 0.7%
                    'Strong': 0.015  # 1.5%
                }.get(self.ob_strength, 0.007)
                
                if move_size > strength_threshold:
                    ob = {
                        'type': 'BULLISH',
                        'high': float(current_candle['high']),
                        'low': float(current_candle['low']),
                        'timestamp': int(current_candle['timestamp']),
                        'strength': float(move_size),
                        'mitigated': False
                    }
                    new_order_blocks.append(ob)
            
            # Bearish Order Block: Big green candle followed by gap down and strong bearish move
            elif (current_candle['close'] > current_candle['open'] and  # Green candle
                  next_candle['open'] < current_candle['close'] and  # Gap down
                  next_candle['close'] < next_candle['open']):  # Red candle
                
                move_size = (current_candle['high'] - next_candle['close']) / current_candle['high']
                
                strength_threshold = {
                    'Weak': 0.003,
                    'Medium': 0.007,
                    'Strong': 0.015
                }.get(self.ob_strength, 0.007)
                
                if move_size > strength_threshold:
                    ob = {
                        'type': 'BEARISH',
                        'high': float(current_candle['high']),
                        'low': float(current_candle['low']),
                        'timestamp': int(current_candle['timestamp']),
                        'strength': float(move_size),
                        'mitigated': False
                    }
                    new_order_blocks.append(ob)
        
        return new_order_blocks

    def detect_fair_value_gaps(self, df):
        """Detect Fair Value Gaps (imbalances)"""
        new_fvgs = []
        
        if len(df) < 3:
            return new_fvgs
        
        for i in range(1, len(df) - 1):
            prev_candle = df.iloc[i - 1]
            current_candle = df.iloc[i]
            next_candle = df.iloc[i + 1]
            
            # Bullish FVG: Gap between previous high and next low
            if prev_candle['high'] < next_candle['low']:
                gap_size = (next_candle['low'] - prev_candle['high']) / prev_candle['high']
                
                if gap_size >= (self.fvg_min_size / 100):  # Convert % to decimal
                    fvg = {
                        'type': 'BULLISH',
                        'high': float(next_candle['low']),
                        'low': float(prev_candle['high']),
                        'timestamp': int(current_candle['timestamp']),
                        'size': float(gap_size),
                        'filled': False
                    }
                    new_fvgs.append(fvg)
            
            # Bearish FVG: Gap between previous low and next high
            elif prev_candle['low'] > next_candle['high']:
                gap_size = (prev_candle['low'] - next_candle['high']) / next_candle['high']
                
                if gap_size >= (self.fvg_min_size / 100):
                    fvg = {
                        'type': 'BEARISH',
                        'high': float(prev_candle['low']),
                        'low': float(next_candle['high']),
                        'timestamp': int(current_candle['timestamp']),
                        'size': float(gap_size),
                        'filled': False
                    }
                    new_fvgs.append(fvg)
        
        return new_fvgs

    def detect_liquidity_sweep(self, df):
        """Detect liquidity sweeps (stop hunts)"""
        if len(df) < 10:
            return False, None
        
        recent_candles = df.tail(5)
        current_price = df.iloc[-1]['close']
        
        # Look for recent highs/lows that could be liquidity zones
        recent_high = recent_candles['high'].max()
        recent_low = recent_candles['low'].min()
        
        # Check if current price swept above recent high (bullish sweep)
        if current_price > recent_high * 1.001:  # 0.1% above high
            return True, {'type': 'BULLISH_SWEEP', 'level': recent_high}
        
        # Check if current price swept below recent low (bearish sweep)
        if current_price < recent_low * 0.999:  # 0.1% below low
            return True, {'type': 'BEARISH_SWEEP', 'level': recent_low}
        
        return False, None

    def check_smc_entry_signal(self, df):
        """Check for SMC-based entry signals"""
        if len(df) < 20:
            return None
        
        current_price = df.iloc[-1]['close']
        
        # Check for order block mitigation + structure confirmation
        for ob in self.order_blocks:
            if ob['mitigated']:
                continue
            
            # Bullish OB mitigation: price returns to OB zone from above
            if (ob['type'] == 'BULLISH' and
                self.market_structure in ['BULLISH', 'RANGING'] and
                ob['low'] <= current_price <= ob['high']):
                
                # Check if we have bullish structure or BOS
                if self.market_structure == 'BULLISH' or self.bos_detected:
                    return {
                        'type': 'BUY',
                        'reason': 'BULLISH_OB_MITIGATION',
                        'entry_price': current_price,
                        'sl_price': ob['low'] * 0.999,
                        'ob_data': ob
                    }
            
            # Bearish OB mitigation: price returns to OB zone from below
            elif (ob['type'] == 'BEARISH' and
                  self.market_structure in ['BEARISH', 'RANGING'] and
                  ob['low'] <= current_price <= ob['high']):
                
                if self.market_structure == 'BEARISH' or self.bos_detected:
                    return {
                        'type': 'SELL',
                        'reason': 'BEARISH_OB_MITIGATION',
                        'entry_price': current_price,
                        'sl_price': ob['high'] * 1.001,
                        'ob_data': ob
                    }
        
        # Check for FVG fill opportunities
        for fvg in self.fair_value_gaps:
            if fvg['filled']:
                continue
            
            # Bullish FVG: price drops into gap for buying opportunity
            if (fvg['type'] == 'BULLISH' and
                self.market_structure == 'BULLISH' and
                fvg['low'] <= current_price <= fvg['high']):
                
                return {
                    'type': 'BUY',
                    'reason': 'BULLISH_FVG_FILL',
                    'entry_price': current_price,
                    'sl_price': fvg['low'] * 0.998,
                    'fvg_data': fvg
                }
            
            # Bearish FVG: price rises into gap for selling opportunity
            elif (fvg['type'] == 'BEARISH' and
                  self.market_structure == 'BEARISH' and
                  fvg['low'] <= current_price <= fvg['high']):
                
                return {
                    'type': 'SELL',
                    'reason': 'BEARISH_FVG_FILL',
                    'entry_price': current_price,
                    'sl_price': fvg['high'] * 1.002,
                    'fvg_data': fvg
                }
        
        return None

    async def calculate_optimal_position_and_leverage(self, entry_price, sl_price, user_max_leverage):
        """Calculate optimal position size and leverage with GUARANTEED INTEGER OUTPUTS"""
        try:
            # 1. Get current available balance/margin
            balance_response = await self.delta_private("GET", "/v2/wallet/balances")
            available_balance = 0
            
            for balance in balance_response.get("result", []):
                if balance.get("asset_symbol") in ["USDT", "USD"]:
                    available_balance = float(balance.get("available_balance", 0))
                    break
            
            if available_balance <= 0:
                return None, None, "Insufficient available balance"

            # 2. Calculate user risk amount with SAFETY CAPS
            if self.trading_mode == "Auto":
                risk_percent = 1.0  # 1% for SMC auto mode
                risk_amount = available_balance * (risk_percent / 100)
            else:
                risk_type = self.risk_management.get("riskType", "Percentage")
                risk_amount_str = self.risk_management.get("riskAmount", "1%")
                
                if risk_type == "Percentage":
                    if isinstance(risk_amount_str, str) and "%" in risk_amount_str:
                        percentage = float(risk_amount_str.replace("%", ""))
                    else:
                        percentage = float(risk_amount_str)
                    percentage = min(percentage, self.MAX_RISK_PERCENT)
                    risk_amount = available_balance * (percentage / 100)
                else:
                    risk_amount = float(risk_amount_str)
                    max_risk_usd = available_balance * (self.MAX_RISK_PERCENT / 100)
                    risk_amount = min(risk_amount, max_risk_usd)

            # 3. Calculate SL distance with SAFETY VALIDATION
            sl_distance = abs(entry_price - sl_price)
            if sl_distance <= 0:
                return None, None, "Invalid stop loss distance"
            
            if sl_distance < self.MIN_SL_DISTANCE:
                return None, None, f"Stop loss distance {sl_distance:.4f} too tight (min: {self.MIN_SL_DISTANCE})"

            # 4. Calculate initial position size - ENSURE INTEGER
            initial_position_size = risk_amount / sl_distance
            position_size = max(self.MIN_POSITION_SIZE, int(initial_position_size))  # EXPLICIT INTEGER CONVERSION
            
            # SAFETY CAP: Maximum position size - ENSURE INTEGER
            position_size = min(position_size, int(self.MAX_POSITION_SIZE))  # EXPLICIT INTEGER CONVERSION
            
            # 5. Find optimal leverage - ENSURE INTEGER
            user_max_leverage = int(user_max_leverage)  # ENSURE INPUT IS INTEGER
            max_leverage = min(user_max_leverage, int(self.MAX_LEVERAGE))  # ENSURE INTEGER BOUNDS
            
            # Try different leverage levels starting from highest
            for leverage in range(int(max_leverage), 0, -1):  # ENSURE INTEGER RANGE
                # Calculate margin requirement
                notional_value = float(position_size) * entry_price  # position_size is int
                margin_required = notional_value / leverage
                
                if margin_required <= (available_balance * self.SAFETY_MARGIN_RATIO):
                    # FINAL VALIDATION - GUARANTEE INTEGERS
                    final_position_size = int(position_size)
                    final_leverage = int(leverage)
                    
                    print(f"[{self.symbol}] 🔢 SMC Position Calculation (INTEGERS GUARANTEED):")
                    print(f" Initial Position Size: {initial_position_size:.2f}")
                    print(f" Final Position Size: {final_position_size} (type: {type(final_position_size).__name__})")
                    print(f" Final Leverage: {final_leverage} (type: {type(final_leverage).__name__})")
                    print(f" Risk Amount: ${risk_amount:.2f}")
                    print(f" Available Balance: ${available_balance:.2f}")
                    
                    return final_position_size, final_leverage, {
                        "risk_amount": risk_amount,
                        "available_balance": available_balance,
                        "margin_required": margin_required,
                        "notional_value": notional_value,
                        "margin_utilization": (margin_required / available_balance) * 100,
                        "position_capped": initial_position_size > self.MAX_POSITION_SIZE
                    }
            
            # If no leverage works, reduce position size - ENSURE INTEGERS
            max_affordable_notional = available_balance * self.SAFETY_MARGIN_RATIO * max_leverage
            max_position_size = max(1, int(max_affordable_notional / entry_price))  # EXPLICIT INTEGER
            
            adjusted_position_size = min(int(position_size), max_position_size)  # EXPLICIT INTEGER
            
            for leverage in range(int(max_leverage), 0, -1):  # ENSURE INTEGER RANGE
                notional_value = float(adjusted_position_size) * entry_price
                margin_required = notional_value / leverage
                
                if margin_required <= (available_balance * self.SAFETY_MARGIN_RATIO):
                    # FINAL VALIDATION - GUARANTEE INTEGERS
                    final_position_size = int(adjusted_position_size)
                    final_leverage = int(leverage)
                    
                    print(f"[{self.symbol}] 🔢 SMC Adjusted Position Calculation (INTEGERS GUARANTEED):")
                    print(f" Adjusted Position Size: {final_position_size} (type: {type(final_position_size).__name__})")
                    print(f" Final Leverage: {final_leverage} (type: {type(final_leverage).__name__})")
                    
                    return final_position_size, final_leverage, {
                        "risk_amount": risk_amount,
                        "available_balance": available_balance,
                        "margin_required": margin_required,
                        "notional_value": notional_value,
                        "margin_utilization": (margin_required / available_balance) * 100,
                        "position_adjusted": True,
                        "position_capped": initial_position_size > self.MAX_POSITION_SIZE
                    }
            
            return None, None, "Cannot find suitable position size and leverage combination"
            
        except Exception as e:
            print(f"[{self.symbol}] ❌ SMC Position calculation error: {e}")
            return None, None, str(e)

    async def safe_shutdown(self, reason="Emergency shutdown"):
        """CRITICAL SAFETY: Safely shutdown strategy with comprehensive logging"""
        print(f"[{self.symbol}] 🚨 SMC EMERGENCY SHUTDOWN: {reason}")
        
        try:
            # Mark as not running immediately
            self.is_running = False
            self.status["running"] = False
            self.status["error"] = reason
            self.status["info"] = f"Emergency shutdown: {reason}"
            
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
            print(f"[{self.symbol}] 🛡️ SMC Strategy safely shut down")
            print(f" Reason: {reason}")
            print(f" Daily PnL: ${self.daily_pnl:.2f}")
            print(f" Trades Today: {self.trade_count_today}")
            
        except Exception as e:
            print(f"[{self.symbol}] ❌ SMC Shutdown error: {e}")

    async def run(self):
        """Main SMC strategy execution loop with ENHANCED SAFETY and INTEGER VALIDATION"""
        print(f"[{self.symbol}] 🎯 Starting SMC Strategy with Integer Safety - Instance: {self.instance_id}")
        
        timeframe = self.timeframe
        user_max_leverage = self.get_user_max_leverage()
        
        print(f"[{self.symbol}] 📊 SMC Configuration:")
        print(f" Timeframe: {timeframe} | Max Leverage: {user_max_leverage}x (integer)")
        print(f" OB Strength: {self.ob_strength} | FVG Min Size: {self.fvg_min_size}%")
        print(f" 🛡️ Safety Limits: Max Position: {self.MAX_POSITION_SIZE}, Max Risk: {self.MAX_RISK_PERCENT}%")
        print(f" 🔢 Integer Safety: Position & Leverage guaranteed as integers")
        
        # Calculate interval for timeframe
        if timeframe.endswith('m'):
            interval_sec = int(timeframe[:-1]) * 60  # ENSURE INTEGER
        elif timeframe.endswith('H'):
            interval_sec = int(timeframe[:-1]) * 3600  # ENSURE INTEGER
        else:
            interval_sec = 60
        
        buffer_max_size = 200
        loop_count = 0
        
        self.status["info"] = f"SMC Strategy Active - {self.market_structure} Structure"
        self.status["details"] = {
            "loop": loop_count,
            "structure": self.market_structure,
            "order_blocks": len(self.order_blocks),
            "fvgs": len(self.fair_value_gaps),
            "instance": self.instance_id,
            "daily_pnl": self.daily_pnl,
            "integer_safety": True
        }
        
        try:
            while self.is_running:
                print(f"[{self.symbol}] 🔄 SMC Loop {loop_count + 1}")
                loop_count += 1
                
                # SAFETY CHECK: Daily loss protection - CRITICAL
                if self.daily_pnl < -(self.account_balance * self.MAX_DAILY_DRAWDOWN):
                    await self.safe_shutdown(f"Daily loss limit exceeded: ${self.daily_pnl:.2f}")
                    break
                
                # Fetch candles with timeout
                end_ts = int(time.time())
                last_ts = max((c.get('timestamp', 0) for c in self.candle_buffer), default=0)
                start_ts = last_ts + 1 if self.candle_buffer else end_ts - (buffer_max_size * interval_sec)
                limit = 20 if self.candle_buffer else buffer_max_size
                
                try:
                    new_candles = await asyncio.wait_for(
                        self.fetch_candles(self.symbol, timeframe, start_ts, end_ts, limit),
                        timeout=30
                    )
                except asyncio.TimeoutError:
                    print(f"[{self.symbol}] SMC candle fetch timeout")
                    await asyncio.sleep(10)
                    continue
                except Exception as e:
                    print(f"[{self.symbol}] ❌ SMC Candle fetch error: {e}")
                    await asyncio.sleep(10)
                    continue
                
                if new_candles:
                    # Process and clean candles
                    for c in new_candles:
                        if 'time' in c and 'timestamp' not in c:
                            c['timestamp'] = c.pop('time')
                    
                    self.candle_buffer.extend(new_candles)
                    
                    # Clean and deduplicate
                    self.candle_buffer = [c for c in self.candle_buffer
                                        if all(k in c for k in ['timestamp', 'open', 'high', 'low', 'close'])
                                        and all(c[k] is not None for k in ['timestamp', 'open', 'high', 'low', 'close'])]
                    
                    # Convert to proper types with error handling
                    valid_candles = []
                    for c in self.candle_buffer:
                        try:
                            c['open'] = float(c['open'])
                            c['high'] = float(c['high'])
                            c['low'] = float(c['low'])
                            c['close'] = float(c['close'])
                            c['timestamp'] = int(c['timestamp'])  # ENSURE INTEGER TIMESTAMP
                            valid_candles.append(c)
                        except (ValueError, TypeError) as e:
                            print(f"[{self.symbol}] ⚠️ Invalid candle data: {e}")
                            continue
                    
                    self.candle_buffer = valid_candles
                    
                    # Remove duplicates and sort
                    deduplicated = {c['timestamp']: c for c in self.candle_buffer}
                    self.candle_buffer = sorted(list(deduplicated.values()),
                                              key=lambda x: x['timestamp'])[-buffer_max_size:]
                
                if len(self.candle_buffer) < 50:
                    self.status["info"] = f"SMC waiting for data ({len(self.candle_buffer)}/50 candles)"
                    await asyncio.sleep(10)
                    continue
                
                # Convert to DataFrame for analysis
                try:
                    df = pd.DataFrame(self.candle_buffer)
                    df = df.sort_values('timestamp').reset_index(drop=True)
                except Exception as e:
                    print(f"[{self.symbol}] ❌ SMC DataFrame creation error: {e}")
                    await asyncio.sleep(10)
                    continue
                
                print(f"[{self.symbol}] 📊 SMC Analysis on {len(df)} candles")
                
                # 1. Analyze Market Structure
                prev_structure = self.market_structure
                try:
                    self.market_structure = self.analyze_market_structure(df)
                except Exception as e:
                    print(f"[{self.symbol}] ⚠️ SMC Structure analysis error: {e}")
                    self.market_structure = "RANGING"
                
                if prev_structure != self.market_structure:
                    print(f"[{self.symbol}] 🔄 SMC Structure Change: {prev_structure} → {self.market_structure}")
                    # Structure change might indicate BOS or CHoCH
                    if prev_structure != "RANGING":
                        self.choch_detected = True
                        print(f"[{self.symbol}] 🎯 SMC CHoCH Detected!")
                
                # 2. Detect Order Blocks
                try:
                    new_obs = self.detect_order_blocks(df)
                    if new_obs:
                        self.order_blocks.extend(new_obs)
                        self.order_blocks = self.order_blocks[-20:]  # Keep last 20
                        print(f"[{self.symbol}] 📦 New Order Blocks: {len(new_obs)} (Total: {len(self.order_blocks)})")
                except Exception as e:
                    print(f"[{self.symbol}] ⚠️ SMC Order block detection error: {e}")
                
                # 3. Detect Fair Value Gaps
                try:
                    new_fvgs = self.detect_fair_value_gaps(df)
                    if new_fvgs:
                        self.fair_value_gaps.extend(new_fvgs)
                        self.fair_value_gaps = self.fair_value_gaps[-20:]  # Keep last 20
                        print(f"[{self.symbol}] 🕳️ New FVGs: {len(new_fvgs)} (Total: {len(self.fair_value_gaps)})")
                except Exception as e:
                    print(f"[{self.symbol}] ⚠️ SMC FVG detection error: {e}")
                
                # 4. Check for liquidity sweeps
                try:
                    sweep_detected, sweep_data = self.detect_liquidity_sweep(df)
                    if sweep_detected:
                        print(f"[{self.symbol}] 💥 SMC Liquidity Sweep: {sweep_data['type']} at {sweep_data['level']}")
                        self.bos_detected = True
                except Exception as e:
                    print(f"[{self.symbol}] ⚠️ SMC Liquidity sweep detection error: {e}")
                
                # 5. Update status
                self.status["details"].update({
                    "loop": loop_count,
                    "structure": self.market_structure,
                    "order_blocks": len(self.order_blocks),
                    "fvgs": len(self.fair_value_gaps),
                    "bos": self.bos_detected,
                    "choch": self.choch_detected,
                    "daily_pnl": self.daily_pnl,
                    "trades_today": self.trade_count_today,
                    "integer_safety": True
                })
                
                # 6. Check existing position
                already_open = await self.check_existing_position()
                if already_open:
                    self.status["info"] = f"SMC position open - {self.market_structure} structure"
                    await asyncio.sleep(15)
                    continue
                
                # 7. Check for entry signals with enhanced safety checks
                current_time = time.time()
                time_since_last_trade = current_time - self.last_trade_time
                time_since_bot_start = current_time - self.bot_start_time
                
                # SAFETY: Restart protection
                if time_since_bot_start < self.restart_protection_time:
                    remaining = self.restart_protection_time - time_since_bot_start
                    self.status["info"] = f"🛡️ SMC restart protection ({remaining:.0f}s)"
                    await asyncio.sleep(10)
                    continue
                
                # SAFETY: Signal cooldown
                if time_since_last_trade < self.signal_cooldown:
                    remaining = self.signal_cooldown - time_since_last_trade
                    self.status["info"] = f"⏳ SMC cooldown active ({remaining:.0f}s)"
                    await asyncio.sleep(10)
                    continue
                
                # SAFETY: Max trades per day limit
                if self.trade_count_today >= 10:  # Max 10 trades per day
                    self.status["info"] = f"🚫 SMC Daily trade limit reached ({self.trade_count_today}/10)"
                    await asyncio.sleep(30)
                    continue
                
                # Check for SMC signals
                try:
                    signal = self.check_smc_entry_signal(df)
                except Exception as e:
                    print(f"[{self.symbol}] ⚠️ SMC Signal detection error: {e}")
                    signal = None
                
                if signal and signal['type'] != self.last_signal:
                    print(f"[{self.symbol}] 🎯 SMC SIGNAL: {signal['type']} - {signal['reason']}")
                    
                    # Lock state immediately
                    self.last_signal = signal['type'].lower()
                    self.last_trade_time = current_time
                    self.save_state()
                    
                    # Execute trade with enhanced safety and integer validation
                    try:
                        trade_result = await self.execute_smc_trade(signal, user_max_leverage)
                        
                        if trade_result and trade_result.get("success"):
                            self.status["info"] = f"✅ SMC {signal['type']} executed - {signal['reason']}"
                            print(f"[{self.symbol}] ✅ SMC Trade Success: {signal['reason']}")
                            self.trade_count_today += 1
                        else:
                            self.status["info"] = f"❌ SMC {signal['type']} failed - {trade_result.get('message', 'Unknown error')}"
                            print(f"[{self.symbol}] ❌ SMC Trade Failed: {trade_result.get('message')}")
                            self.last_signal = None  # Reset on failure
                            self.save_state()
                    
                    except Exception as e:
                        print(f"[{self.symbol}] ❌ SMC Trade execution exception: {e}")
                        self.last_signal = None
                        self.save_state()
                
                else:
                    self.status["info"] = f"SMC monitoring - {self.market_structure} | OBs:{len(self.order_blocks)} FVGs:{len(self.fair_value_gaps)} | PnL:${self.daily_pnl:.2f}"
                
                # Save state periodically
                if loop_count % 5 == 0:
                    self.save_state()
                
                await asyncio.sleep(15)  # SMC doesn't need ultra-fast updates
        
        except asyncio.CancelledError:
            print(f"[{self.symbol}] SMC Strategy cancelled")
            raise
        except Exception as e:
            print(f"[{self.symbol}] ❌ SMC Strategy critical error: {e}")
            await self.safe_shutdown(f"Critical error: {e}")
        finally:
            self.save_state()
            print(f"[{self.symbol}] SMC Strategy stopped")

    async def execute_smc_trade(self, signal, user_max_leverage):
        """Execute SMC-based trade with INTEGER VALIDATION and comprehensive risk management"""
        try:
            side = signal['type'].lower()
            entry_price = signal['entry_price']
            sl_price = signal['sl_price']
            
            print(f"[{self.symbol}] 🎯 SMC Trade Setup with Integer Safety:")
            print(f" Side: {side.upper()} | Entry: ${entry_price:.4f} | SL: ${sl_price:.4f}")
            
            # Calculate optimal position size and leverage with INTEGER GUARANTEES
            position_size, leverage, details = await self.calculate_optimal_position_and_leverage(
                entry_price, sl_price, user_max_leverage
            )
            
            if position_size is None:
                print(f"[{self.symbol}] ❌ SMC Position calculation failed: {details}")
                return {"success": False, "message": details}
            
            # CRITICAL: Final integer validation before proceeding
            if not isinstance(position_size, int) or not isinstance(leverage, int):
                print(f"[{self.symbol}] ❌ CRITICAL: Position or leverage not integer - position: {type(position_size)}, leverage: {type(leverage)}")
                return {"success": False, "message": "Integer validation failed"}
            
            # Calculate TP with 2.5:1 RR
            risk_distance = abs(entry_price - sl_price)
            tp_distance = risk_distance * 2.5
            
            if side == 'buy':
                tp_price = entry_price + tp_distance
            else:
                tp_price = entry_price - tp_distance
            
            # Log detailed trade information with integer confirmation
            print(f"[{self.symbol}] 📋 SMC Trade Execution Plan (INTEGER VALIDATED):")
            print(f" Position Size: {position_size} contracts (type: {type(position_size).__name__})")
            print(f" Leverage: {leverage}x (type: {type(leverage).__name__})")
            print(f" Entry: ${entry_price:.4f} | SL: ${sl_price:.4f} | TP: ${tp_price:.4f}")
            print(f" Risk Amount: ${details['risk_amount']:.2f}")
            print(f" Margin Required: ${details['margin_required']:.2f}")
            print(f" Margin Utilization: {details['margin_utilization']:.1f}%")
            print(f" Notional Value: ${details['notional_value']:.2f}")
            
            # Safety warnings
            if details.get('position_capped'):
                print(f" ⚠️ Position size capped at maximum: {self.MAX_POSITION_SIZE}")
            if details.get('position_adjusted'):
                print(f" ⚠️ Position size adjusted to fit available margin")
            if leverage < user_max_leverage:
                print(f" ⚠️ Leverage reduced from {user_max_leverage}x to {leverage}x for margin safety")
            
            # Execute the trade with integer validation
            success = await self.execute_order(
                self.symbol_to_pid[self.symbol],
                side,
                entry_price,
                position_size,  # GUARANTEED INTEGER
                leverage,       # GUARANTEED INTEGER
                sl_price,
                tp_price
            )
            
            if success:
                print(f"[{self.symbol}] ✅ SMC Trade executed successfully with integers")
                return {
                    "success": True, 
                    "message": f"SMC {side.upper()} order placed", 
                    "details": details,
                    "position_size": position_size,
                    "leverage": leverage
                }
            else:
                print(f"[{self.symbol}] ❌ SMC Trade execution failed")
                return {"success": False, "message": "Order execution failed"}
                
        except Exception as e:
            print(f"[{self.symbol}] ❌ SMC Trade execution error: {e}")
            return {"success": False, "message": str(e)}

    async def execute_order(self, pid, side, entry_price, position_size, leverage, sl_price, tp_price):
        """Execute order with MANDATORY INTEGER VALIDATION"""
        
        # CRITICAL: ENSURE INTEGERS BEFORE API CALLS
        if not isinstance(position_size, int):
            print(f"[{self.symbol}] 🔧 Converting position_size to integer: {position_size} -> {int(position_size)}")
            position_size = max(1, int(position_size))
        
        if not isinstance(leverage, int):
            print(f"[{self.symbol}] 🔧 Converting leverage to integer: {leverage} -> {int(leverage)}")
            leverage = max(1, int(leverage))
        
        # Final validation
        if position_size < 1 or leverage < 1:
            print(f"[{self.symbol}] ❌ Invalid SMC order parameters: size={position_size}, leverage={leverage}")
            return False
        
        print(f"[{self.symbol}] 📋 SMC Order Parameters Validated:")
        print(f" Position Size: {position_size} (type: {type(position_size).__name__})")
        print(f" Leverage: {leverage} (type: {type(leverage).__name__})")
        
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                print(f"[{self.symbol}] 📤 SMC Order execution attempt {attempt + 1}/{max_retries}")
                
                # Set leverage with integer validation
                leverage_payload = {"leverage": leverage}  # GUARANTEED INTEGER
                
                leverage_response = await asyncio.wait_for(
                    self.delta_private("POST", f"/v2/products/{pid}/orders/leverage", body=leverage_payload),
                    timeout=10.0
                )
                
                if not leverage_response.get('success'):
                    error_msg = leverage_response.get('error', {}).get('message', 'Unknown error')
                    print(f"[{self.symbol}] ❌ SMC Leverage setting failed (attempt {attempt + 1}): {error_msg}")
                    if attempt == max_retries - 1:
                        await self.safe_shutdown(f"Leverage setting failed: {error_msg}")
                        return False
                    await asyncio.sleep(2 ** attempt)
                    continue
                
                print(f"[{self.symbol}] ✅ SMC Leverage set to {leverage}x successfully")
                
                # Main order with integer validation
                main_order = {
                    "product_id": pid,
                    "order_type": "market_order",
                    "side": side,
                    "size": position_size,  # GUARANTEED INTEGER
                    "reduce_only": False,
                    "client_order_id": f"smc_{side}_{self.instance_id}_{int(time.time())}"
                }
                
                print(f"[{self.symbol}] 📋 SMC Order Payload: size={position_size} (int), leverage={leverage}x (int)")
                
                main_response = await asyncio.wait_for(
                    self.delta_private("POST", "/v2/orders", body=main_order),
                    timeout=15.0
                )
                
                if main_response.get('success'):
                    print(f"[{self.symbol}] ✅ SMC Main order executed: {position_size} contracts @ {leverage}x")
                    
                    # Enhanced Trade Journal logging with integer confirmation
                    if TRADE_JOURNAL_AVAILABLE:
                        add_trade({
                            'symbol': self.symbol,
                            'side': side,
                            'size': position_size,  # Integer
                            'entry_price': float(entry_price),
                            'sl_price': float(sl_price),
                            'tp_price': float(tp_price),
                            'leverage': leverage,  # Integer
                            'strategy': 'SMC Strategy',
                            'status': 'Open',
                            'timestamp': time.time(),
                            'instance_id': self.instance_id
                        })
                    
                    # Wait then place bracket orders
                    await asyncio.sleep(2)
                    
                    bracket_payload = {
                        "product_id": pid,
                        "stop_loss_order": {
                            "order_type": "market_order",
                            "stop_price": str(round(sl_price, 4))
                        },
                        "take_profit_order": {
                            "order_type": "limit_order",
                            "limit_price": str(round(tp_price, 4))
                        },
                        "bracket_stop_trigger_method": "mark_price"
                    }
                    
                    bracket_response = await asyncio.wait_for(
                        self.delta_private("POST", "/v2/orders/bracket", body=bracket_payload),
                        timeout=15.0
                    )
                    
                    if bracket_response.get('success'):
                        print(f"[{self.symbol}] ✅ SMC Bracket orders placed successfully")
                    else:
                        print(f"[{self.symbol}] ⚠️ SMC Bracket order failed: {bracket_response}")
                    
                    return True
                else:
                    error_msg = main_response.get('error', {}).get('message', 'Unknown error')
                    print(f"[{self.symbol}] ❌ SMC Main order failed (attempt {attempt + 1}): {error_msg}")
                    
                    if attempt == max_retries - 1:
                        await self.safe_shutdown(f"Order execution failed: {error_msg}")
                        return False
                    
                    await asyncio.sleep(2 ** attempt)
        
            except asyncio.TimeoutError:
                print(f"[{self.symbol}] ⚠️ SMC Order execution timeout (attempt {attempt + 1})")
                if attempt == max_retries - 1:
                    await self.safe_shutdown("Order execution timeout")
                    return False
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                print(f"[{self.symbol}] ❌ SMC Order execution error (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    await self.safe_shutdown(f"Critical order error: {e}")
                    return False
                await asyncio.sleep(2 ** attempt)
        
        return False

    def get_user_max_leverage(self):
        """Get user's maximum allowed leverage with GUARANTEED INTEGER OUTPUT"""
        if self.trading_mode == "Auto":
            user_max = int(float(self.auto_calculated.get("leverage", 3)))  # Double conversion for safety
        else:
            user_max = int(float(self.risk_management.get("leverage", 3)))  # Double conversion for safety
        
        # Respect exchange limits
        if self.symbol in ['BTCUSD', 'ETHUSD']:
            exchange_max = 200
        else:
            exchange_max = 100
        
        # ENHANCED SAFETY: Cap SMC at 50x and ensure integer
        final_leverage = min(user_max, exchange_max, int(self.MAX_LEVERAGE))
        final_leverage = max(int(self.MIN_LEVERAGE), final_leverage)  # Ensure minimum
        
        print(f"[{self.symbol}] 🔢 SMC Leverage Calculation (INTEGER GUARANTEED):")
        print(f" User Max: {user_max} | Exchange Max: {exchange_max} | Strategy Max: {self.MAX_LEVERAGE}")
        print(f" Final Leverage: {final_leverage} (type: {type(final_leverage).__name__})")
        
        return int(final_leverage)  # GUARANTEE INTEGER OUTPUT

    async def check_existing_position(self):
        """Check for existing positions with enhanced error handling"""
        try:
            position_resp = await asyncio.wait_for(
                self.delta_private("GET", f"/v2/positions/margined",
                                 params={"product_ids": self.symbol_to_pid[self.symbol]}),
                timeout=10.0
            )
            
            result = position_resp.get("result", [])
            
            if isinstance(result, list):
                total_size = sum(abs(pos.get("size", 0)) for pos in result
                               if pos.get("product_id") == self.symbol_to_pid[self.symbol])
            else:
                total_size = abs(result.get("size", 0)) if result.get("product_id") == self.symbol_to_pid[self.symbol] else 0
            
            return total_size > 0
        
        except Exception as e:
            print(f"[{self.symbol}] ❌ SMC position check error: {e}")
            return True  # Assume position exists on error (safe default)

    async def fetch_candles(self, symbol, resolution, start_ts, end_ts, limit):
        """Fetch candles for analysis with enhanced error handling"""
        try:
            response = await asyncio.wait_for(
                self.delta_private("GET", "/v2/history/candles", params={
                    "symbol": symbol,
                    "resolution": resolution,
                    "start": start_ts,
                    "end": end_ts,
                    "limit": limit
                }),
                timeout=15.0
            )
            
            candles = response.get("result", [])
            return sorted(candles, key=lambda x: x.get('timestamp', 0))
        
        except Exception as e:
            print(f"[{self.symbol}] ❌ SMC Candle fetch error: {e}")
            return []

    def start(self):
        """Start SMC strategy with comprehensive validation"""
        if self.is_running:
            print(f"[{self.symbol}] SMC Strategy already running")
            return
        
        # Final safety check before starting
        if not hasattr(self, 'symbol_to_pid') or self.symbol not in self.symbol_to_pid:
            print(f"[{self.symbol}] ❌ Cannot start: Product ID not found")
            return
        
        # Validate integer parameters
        if not isinstance(self.lookback_periods, int) or not isinstance(self.structure_confirmation, int):
            print(f"[{self.symbol}] ❌ Cannot start: Integer parameters validation failed")
            return
        
        self.is_running = True
        self.status["running"] = True
        self.status["error"] = None
        self.task = asyncio.create_task(self.run())
        print(f"[{self.symbol}] 🎯 SMC Strategy started safely with integer validation - Instance: {self.instance_id}")
        print(f" Integer Parameters: Lookback: {self.lookback_periods}, Structure: {self.structure_confirmation}")

    async def stop(self):
        """Stop SMC strategy with enhanced cleanup"""
        print(f"[{self.symbol}] 🛑 Stopping SMC Strategy")
        self.is_running = False
        self.status["running"] = False
        
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await asyncio.wait_for(self.task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                print(f"[{self.symbol}] ⚠️ SMC Task cancellation timeout")
        
        # Save final state
        self.save_state()
        print(f"[{self.symbol}] ✅ SMC Strategy stopped safely")
        print(f" Final Statistics:")
        print(f"  Daily PnL: ${self.daily_pnl:.2f}")
        print(f"  Trades Today: {self.trade_count_today}")
        print(f"  Market Structure: {self.market_structure}")

    def get_status(self):
        """Get current strategy status with enhanced information"""
        status = self.status.copy()
        status["running"] = self.is_running
        status["daily_pnl"] = self.daily_pnl
        status["trades_today"] = self.trade_count_today
        status["market_structure"] = self.market_structure
        status["order_blocks"] = len(self.order_blocks)
        status["fair_value_gaps"] = len(self.fair_value_gaps)
        status["safety_limits"] = {
            "max_position_size": self.MAX_POSITION_SIZE,
            "max_daily_drawdown": self.MAX_DAILY_DRAWDOWN,
            "max_risk_percent": self.MAX_RISK_PERCENT,
            "max_leverage": self.MAX_LEVERAGE
        }
        status["integer_validation"] = {
            "position_size_type": "integer",
            "leverage_type": "integer",
            "lookback_periods_type": "integer",
            "structure_confirmation_type": "integer"
        }
        return status
