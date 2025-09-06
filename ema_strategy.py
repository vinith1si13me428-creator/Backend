# enhanced_ema_strategy.py - Professional EMA with Dynamic Risk Management
import asyncio
import time
import pandas as pd
import talib
import math
from typing import Dict, Any, List, Optional, Tuple

from strategy_engine import BaseStrategy, StrategyConfig
from delta_client import DeltaAPIClient
from risk_manager import RiskManager
from order_executor import OrderExecutor
from database_manager import DatabaseManager

class EMAStrategy(BaseStrategy):
    """
    🎯 Enhanced EMA Strategy with Dynamic Position Sizing & ATR-Based Risk Management
    
    Key Features:
    ✅ Dynamic position sizing based on available balance
    ✅ ATR-based stop loss distance calculation
    ✅ Optimal leverage selection within margin requirements
    ✅ Risk percentage/amount compliance
    ✅ Market condition awareness
    ✅ Anti-whipsaw protection
    """
    
    # Enhanced Safety Constants
    MAX_RISK_PERCENT = 5.0          # Maximum 5% risk per trade
    MIN_RISK_PERCENT = 0.5          # Minimum 0.5% risk per trade
    MAX_LEVERAGE = 50               # Maximum allowed leverage
    MIN_LEVERAGE = 1                # Minimum leverage
    MARGIN_SAFETY_FACTOR = 0.8      # Use 80% of available margin for safety
    ATR_MULTIPLIER_RANGE = (1.0, 3.0)  # ATR multiplier for SL distance
    MIN_RR_RATIO = 1.5              # Minimum risk:reward ratio
    MAX_RR_RATIO = 5.0              # Maximum risk:reward ratio
    
    def __init__(self, config: StrategyConfig, delta_client: DeltaAPIClient,
                 risk_manager: RiskManager, order_executor: OrderExecutor,
                 db_manager: DatabaseManager):
        
        super().__init__(config, delta_client, risk_manager, order_executor, db_manager)
        
        # Enhanced EMA settings
        self.candle_buffer: List[dict] = []
        self.buffer_max_size = 200
        self.min_candles_for_ema = max(config.fast_period, config.slow_period) + 50
        
        # Market condition detection
        self.market_condition = "UNKNOWN"  # TRENDING_UP, TRENDING_DOWN, SIDEWAYS, VOLATILE
        self.volatility_threshold = 0.02   # 2% daily volatility threshold
        self.trend_strength = 0.0          # ADX-based trend strength
        
        # Anti-whipsaw protection
        self.consecutive_losses = 0
        self.max_consecutive_losses = 3
        self.whipsaw_protection_time = 300  # 5 minutes after consecutive losses
        self.last_loss_time = 0
        
        # Enhanced state tracking
        self.available_balance = 0.0
        self.margin_used = 0.0
        self.daily_trades = 0
        self.max_daily_trades = 10
        
        self.logger.info(f"✅ Enhanced EMA Strategy initialized with dynamic risk management")
        self.logger.info(f"📊 EMA: {config.fast_period}/{config.slow_period} | Max Risk: {self.MAX_RISK_PERCENT}%")

    async def execute_strategy_logic(self) -> None:
        """Enhanced EMA strategy with comprehensive risk management"""
        self.logger.info(f"🚀 Starting Enhanced EMA Strategy with Dynamic Position Sizing")
        
        loop_count = 0
        
        while self.is_running:
            loop_count += 1
            self.logger.debug(f"🔄 Enhanced EMA Loop {loop_count}")
            
            try:
                # 1. Update available balance and margin info
                await self._update_account_info()
                
                # 2. Update candle buffer and market conditions
                await self._update_candle_buffer_and_market_conditions()
                
                # 3. Check if we have sufficient data
                if len(self.candle_buffer) < self.min_candles_for_ema:
                    self.status["info"] = f"Waiting for data ({len(self.candle_buffer)}/{self.min_candles_for_ema})"
                    await asyncio.sleep(10)
                    continue
                
                # 4. Check daily trade limits and account safety
                if not await self._check_safety_conditions():
                    await asyncio.sleep(30)
                    continue
                
                # 5. Calculate enhanced signals with market condition awareness
                signals = await self.calculate_enhanced_signals_with_conditions()
                
                # 6. Check existing position
                has_position = await self._check_existing_position()
                
                # 7. Process signals with enhanced risk management
                if signals.get("signal") and not has_position:
                    await self._process_enhanced_signals_with_dynamic_sizing(signals)
                
                # 8. Update status and save state
                await self._update_status_and_save_state(loop_count)
                
                # 9. Dynamic sleep based on market conditions
                sleep_time = self._calculate_dynamic_sleep_time()
                await asyncio.sleep(sleep_time)
                
            except asyncio.CancelledError:
                self.logger.info("Enhanced EMA strategy cancelled")
                break
            except Exception as e:
                self.logger.error(f"❌ Enhanced EMA error: {e}")
                await asyncio.sleep(20)

    async def _update_account_info(self) -> None:
        """Update available balance and margin information"""
        try:
            # Get wallet balances
            balances = await self.delta_client.get_wallet_balances()
            
            self.available_balance = 0.0
            for balance in balances:
                if balance.get("asset", {}).get("symbol") in ["USDT", "USD"]:
                    self.available_balance = float(balance.get("available_balance", 0))
                    break
            
            # Get margin information
            positions = await self.delta_client.get_positions()
            self.margin_used = 0.0
            
            if isinstance(positions, list):
                for position in positions:
                    self.margin_used += float(position.get("margin", 0))
            
            self.logger.debug(f"💰 Balance: ${self.available_balance:.2f} | Margin Used: ${self.margin_used:.2f}")
            
        except Exception as e:
            self.logger.error(f"❌ Account info update error: {e}")

    async def _update_candle_buffer_and_market_conditions(self) -> None:
        """Update candles and detect market conditions"""
        try:
            # Update candle buffer (existing logic)
            end_ts = int(time.time())
            interval_sec = self._get_interval_seconds()
            start_ts = end_ts - (self.buffer_max_size * interval_sec)
            
            new_candles = await self.delta_client.get_candles(
                self.config.symbol, self.config.timeframe, start_ts, end_ts, self.buffer_max_size
            )
            
            if new_candles:
                self._process_candles(new_candles)
                
                # Detect market conditions
                self._detect_market_conditions()
            
        except Exception as e:
            self.logger.error(f"❌ Candle/condition update error: {e}")

    def _detect_market_conditions(self) -> None:
        """Detect current market conditions for better signal filtering"""
        try:
            if len(self.candle_buffer) < 50:
                return
            
            df = pd.DataFrame(self.candle_buffer[-50:])
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df = df.dropna()
            
            # 1. Calculate trend strength using ADX
            adx = talib.ADX(df['high'], df['low'], df['close'], timeperiod=14)
            self.trend_strength = adx.iloc[-1] if len(adx.dropna()) > 0 else 0
            
            # 2. Calculate volatility (ATR as % of price)
            atr = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)
            current_atr = atr.iloc[-1] if len(atr.dropna()) > 0 else 0
            volatility_pct = (current_atr / df['close'].iloc[-1]) * 100
            
            # 3. Calculate directional movement
            ema_20 = talib.EMA(df['close'], timeperiod=20)
            ema_50 = talib.EMA(df['close'], timeperiod=50)
            
            if len(ema_20.dropna()) > 0 and len(ema_50.dropna()) > 0:
                trend_direction = "UP" if ema_20.iloc[-1] > ema_50.iloc[-1] else "DOWN"
            else:
                trend_direction = "UNKNOWN"
            
            # 4. Determine market condition
            if self.trend_strength > 25 and volatility_pct > self.volatility_threshold:
                self.market_condition = f"TRENDING_{trend_direction}"
            elif volatility_pct > self.volatility_threshold * 2:
                self.market_condition = "VOLATILE"
            else:
                self.market_condition = "SIDEWAYS"
            
            self.logger.debug(f"📊 Market: {self.market_condition} | ADX: {self.trend_strength:.1f} | Vol: {volatility_pct:.2f}%")
            
        except Exception as e:
            self.logger.error(f"❌ Market condition detection error: {e}")

    async def calculate_enhanced_signals_with_conditions(self) -> Dict[str, Any]:
        """Calculate EMA signals with market condition awareness"""
        try:
            # Basic EMA crossover calculation
            base_signals = await self._calculate_base_ema_signals()
            
            if not base_signals.get("signal"):
                return base_signals
            
            # Enhanced filtering based on market conditions
            signal_quality = self._assess_signal_quality(base_signals)
            
            # ATR-based stop loss calculation
            atr_data = self._calculate_atr_based_stops(base_signals)
            
            # Combine all data
            enhanced_signals = {
                **base_signals,
                **signal_quality,
                **atr_data,
                "market_condition": self.market_condition,
                "trend_strength": self.trend_strength
            }
            
            return enhanced_signals
            
        except Exception as e:
            self.logger.error(f"❌ Enhanced signal calculation error: {e}")
            return {"signal": None, "reason": f"Error: {e}"}

    async def _calculate_base_ema_signals(self) -> Dict[str, Any]:
        """Calculate basic EMA crossover signals"""
        df = pd.DataFrame(self.candle_buffer)
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df = df.dropna().sort_values('timestamp')
        
        if len(df) < self.min_candles_for_ema:
            return {"signal": None, "reason": "Insufficient data"}
        
        # Calculate EMAs using TEMA for reduced lag
        fast_ema = talib.TEMA(df['close'].astype('float64'), timeperiod=self.config.fast_period)
        slow_ema = talib.TEMA(df['close'].astype('float64'), timeperiod=self.config.slow_period)
        
        if fast_ema.dropna().size < 2 or slow_ema.dropna().size < 2:
            return {"signal": None, "reason": "Insufficient EMA data"}
        
        # Current and previous values
        curr_fast, curr_slow = fast_ema.iloc[-1], slow_ema.iloc[-1]
        prev_fast, prev_slow = fast_ema.iloc[-2], slow_ema.iloc[-2]
        
        # Get current price
        current_price = df['close'].iloc[-1]
        
        # Crossover detection
        tolerance = 1e-8
        bullish_cross = (prev_fast <= prev_slow) and (curr_fast > curr_slow + tolerance)
        bearish_cross = (prev_fast >= prev_slow) and (curr_fast < curr_slow - tolerance)
        
        signal_data = {
            "signal": None,
            "entry_price": current_price,
            "fast_ema": curr_fast,
            "slow_ema": curr_slow,
            "crossover_strength": abs(curr_fast - curr_slow) / current_price,
            "timestamp": int(time.time())
        }
        
        if bullish_cross:
            signal_data["signal"] = "buy"
            signal_data["reason"] = "Bullish TEMA crossover"
        elif bearish_cross:
            signal_data["signal"] = "sell"
            signal_data["reason"] = "Bearish TEMA crossover"
        else:
            signal_data["reason"] = "No crossover detected"
        
        return signal_data

    def _assess_signal_quality(self, signals: Dict[str, Any]) -> Dict[str, Any]:
        """Assess signal quality based on market conditions"""
        signal_type = signals.get("signal")
        quality_score = 0.0
        quality_factors = []
        
        # 1. Market condition alignment
        if self.market_condition.startswith("TRENDING"):
            quality_score += 0.3
            quality_factors.append("trending_market")
            
            # Check if signal aligns with trend
            if ("UP" in self.market_condition and signal_type == "buy") or \
               ("DOWN" in self.market_condition and signal_type == "sell"):
                quality_score += 0.2
                quality_factors.append("trend_aligned")
        
        # 2. Trend strength (ADX)
        if self.trend_strength > 25:
            quality_score += 0.2
            quality_factors.append("strong_trend")
        elif self.trend_strength > 20:
            quality_score += 0.1
            quality_factors.append("moderate_trend")
        
        # 3. Crossover strength
        crossover_strength = signals.get("crossover_strength", 0)
        if crossover_strength > 0.005:  # 0.5%
            quality_score += 0.2
            quality_factors.append("strong_crossover")
        elif crossover_strength > 0.002:  # 0.2%
            quality_score += 0.1
            quality_factors.append("moderate_crossover")
        
        # 4. Avoid sideways markets for EMA
        if self.market_condition == "SIDEWAYS":
            quality_score -= 0.3
            quality_factors.append("sideways_penalty")
        
        # 5. Anti-whipsaw check
        if self.consecutive_losses >= 2:
            quality_score -= 0.2
            quality_factors.append("whipsaw_risk")
        
        return {
            "signal_quality": max(0.0, min(1.0, quality_score)),
            "quality_factors": quality_factors,
            "market_suitable": quality_score > 0.3
        }

    def _calculate_atr_based_stops(self, signals: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate ATR-based stop loss and take profit levels"""
        try:
            if len(self.candle_buffer) < 20:
                return {"atr_available": False}
            
            df = pd.DataFrame(self.candle_buffer[-50:])
            atr = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)
            
            if len(atr.dropna()) < 1:
                return {"atr_available": False}
            
            current_atr = atr.iloc[-1]
            entry_price = signals.get("entry_price", 0)
            signal_type = signals.get("signal")
            
            # Dynamic ATR multiplier based on market conditions
            if self.market_condition.startswith("TRENDING"):
                atr_multiplier = 1.5  # Tighter stops in trending markets
            elif self.market_condition == "VOLATILE":
                atr_multiplier = 2.5  # Wider stops in volatile markets
            else:
                atr_multiplier = 2.0  # Standard stops in sideways markets
            
            # Adjust based on signal quality
            quality = signals.get("signal_quality", 0.5)
            atr_multiplier *= (0.8 + quality * 0.4)  # 0.8x to 1.2x based on quality
            
            # Calculate stop loss distance
            sl_distance = current_atr * atr_multiplier
            sl_distance_pct = (sl_distance / entry_price) * 100
            
            # Calculate stop loss and take profit prices
            if signal_type == "buy":
                sl_price = entry_price - sl_distance
                # Dynamic R:R based on market conditions
                rr_ratio = 3.0 if self.market_condition.startswith("TRENDING") else 2.0
                tp_price = entry_price + (sl_distance * rr_ratio)
            elif signal_type == "sell":
                sl_price = entry_price + sl_distance
                rr_ratio = 3.0 if self.market_condition.startswith("TRENDING") else 2.0
                tp_price = entry_price - (sl_distance * rr_ratio)
            else:
                return {"atr_available": False}
            
            return {
                "atr_available": True,
                "atr_value": current_atr,
                "atr_multiplier": atr_multiplier,
                "sl_distance": sl_distance,
                "sl_distance_pct": sl_distance_pct,
                "sl_price": sl_price,
                "tp_price": tp_price,
                "risk_reward_ratio": rr_ratio
            }
            
        except Exception as e:
            self.logger.error(f"❌ ATR calculation error: {e}")
            return {"atr_available": False}

    async def _process_enhanced_signals_with_dynamic_sizing(self, signals: Dict[str, Any]) -> None:
        """Process signals with dynamic position sizing and leverage optimization"""
        try:
            # Check signal quality threshold
            if not signals.get("market_suitable", False):
                self.status["info"] = f"Signal filtered: Poor market conditions"
                return
            
            if signals.get("signal_quality", 0) < 0.4:
                self.status["info"] = f"Signal filtered: Low quality ({signals.get('signal_quality', 0)*100:.1f}%)"
                return
            
            # Check ATR availability
            if not signals.get("atr_available", False):
                self.status["info"] = "Signal filtered: ATR data unavailable"
                return
            
            # Anti-whipsaw protection
            if await self._check_whipsaw_protection():
                return
            
            # Calculate optimal position size and leverage
            position_data = await self._calculate_optimal_position_and_leverage(signals)
            
            if not position_data:
                self.status["info"] = "Signal filtered: No valid position size found"
                return
            
            # Execute trade with dynamic parameters
            success = await self._execute_dynamic_trade(signals, position_data)
            
            if success:
                self.daily_trades += 1
                self.consecutive_losses = 0  # Reset on successful trade
                self.status["info"] = f"✅ Enhanced trade executed: {position_data['position_size']} @ {position_data['leverage']}x"
            else:
                self.consecutive_losses += 1
                self.last_loss_time = time.time()
                self.status["info"] = "❌ Trade execution failed"
            
        except Exception as e:
            self.logger.error(f"❌ Enhanced signal processing error: {e}")

    async def _calculate_optimal_position_and_leverage(self, signals: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Calculate optimal position size and leverage based on:
        1. Available balance
        2. Risk percentage/amount
        3. ATR-based SL distance
        4. Margin requirements
        5. Exchange limits
        """
        try:
            entry_price = signals["entry_price"]
            sl_distance = signals["sl_distance"]
            sl_distance_pct = signals["sl_distance_pct"]
            
            # Get user risk settings
            if self.config.trading_mode == "Auto":
                risk_pct = min(2.0, self.MAX_RISK_PERCENT)  # Conservative auto mode
            else:
                risk_pct = float(self.config.max_risk_percent or 2.0)
                risk_pct = max(self.MIN_RISK_PERCENT, min(risk_pct, self.MAX_RISK_PERCENT))
            
            # Adjust risk based on signal quality
            quality = signals.get("signal_quality", 0.5)
            adjusted_risk_pct = risk_pct * (0.5 + quality * 0.5)  # 50% to 100% of max risk
            
            # Calculate risk amount in USD
            risk_amount_usd = self.available_balance * (adjusted_risk_pct / 100)
            
            # Calculate base position size (in USD notional)
            # Risk Amount = Position Size * SL Distance %
            # Position Size = Risk Amount / SL Distance %
            max_position_usd = risk_amount_usd / (sl_distance_pct / 100)
            
            # Safety cap based on available balance
            max_safe_position_usd = self.available_balance * self.MARGIN_SAFETY_FACTOR
            max_position_usd = min(max_position_usd, max_safe_position_usd)
            
            # Find optimal leverage within exchange limits
            optimal_combinations = []
            
            for leverage in range(self.MIN_LEVERAGE, min(self.MAX_LEVERAGE + 1, 51)):  # Test up to 50x
                # Calculate required margin for this leverage
                required_margin = max_position_usd / leverage
                
                # Check if we have enough available balance
                if required_margin <= self.available_balance * self.MARGIN_SAFETY_FACTOR:
                    # Calculate position size in contracts
                    position_size_contracts = int(max_position_usd / entry_price)
                    
                    if position_size_contracts >= 1:  # Must be at least 1 contract
                        # Calculate actual risk with this position size
                        actual_notional = position_size_contracts * entry_price
                        actual_risk_usd = actual_notional * (sl_distance_pct / 100)
                        actual_risk_pct = (actual_risk_usd / self.available_balance) * 100
                        
                        # Verify risk is within limits
                        if actual_risk_pct <= self.MAX_RISK_PERCENT:
                            optimal_combinations.append({
                                "leverage": leverage,
                                "position_size": position_size_contracts,
                                "required_margin": required_margin,
                                "notional_value": actual_notional,
                                "risk_usd": actual_risk_usd,
                                "risk_pct": actual_risk_pct,
                                "margin_utilization": (required_margin / self.available_balance) * 100
                            })
            
            if not optimal_combinations:
                self.logger.warning("No valid position/leverage combinations found")
                return None
            
            # Select optimal combination (prefer higher leverage for capital efficiency)
            # But ensure margin utilization is reasonable
            best_combo = None
            for combo in optimal_combinations:
                if combo["margin_utilization"] <= 50:  # Prefer <50% margin utilization
                    if not best_combo or combo["leverage"] > best_combo["leverage"]:
                        best_combo = combo
            
            # Fallback: pick combination with lowest margin utilization
            if not best_combo:
                best_combo = min(optimal_combinations, key=lambda x: x["margin_utilization"])
            
            # Add additional metadata
            best_combo.update({
                "signal_quality": quality,
                "adjusted_risk_pct": adjusted_risk_pct,
                "atr_multiplier": signals.get("atr_multiplier", 2.0),
                "market_condition": self.market_condition
            })
            
            self.logger.info(f"📊 Optimal Position Calculated:")
            self.logger.info(f"   Position: {best_combo['position_size']} contracts")
            self.logger.info(f"   Leverage: {best_combo['leverage']}x")
            self.logger.info(f"   Required Margin: ${best_combo['required_margin']:.2f}")
            self.logger.info(f"   Risk: ${best_combo['risk_usd']:.2f} ({best_combo['risk_pct']:.2f}%)")
            self.logger.info(f"   Margin Utilization: {best_combo['margin_utilization']:.1f}%")
            
            return best_combo
            
        except Exception as e:
            self.logger.error(f"❌ Position/leverage calculation error: {e}")
            return None

    async def _execute_dynamic_trade(self, signals: Dict[str, Any], position_data: Dict[str, Any]) -> bool:
        """Execute trade with calculated position size and leverage"""
        try:
            signal_type = signals["signal"]
            entry_price = signals["entry_price"]
            sl_price = signals["sl_price"]
            tp_price = signals["tp_price"]
            
            position_size = position_data["position_size"]
            leverage = position_data["leverage"]
            
            self.logger.info(f"🎯 Executing Enhanced {signal_type.upper()} Trade:")
            self.logger.info(f"   Entry: ${entry_price:.4f}")
            self.logger.info(f"   Stop Loss: ${sl_price:.4f}")
            self.logger.info(f"   Take Profit: ${tp_price:.4f}")
            self.logger.info(f"   Size: {position_size} contracts @ {leverage}x leverage")
            self.logger.info(f"   Quality: {signals.get('signal_quality', 0)*100:.1f}%")
            self.logger.info(f"   Market: {self.market_condition}")
            
            # Execute trade using existing order executor
            success = await self.order_executor.execute_ema_trade(
                symbol=self.config.symbol,
                signal=signal_type,
                entry_price=entry_price,
                sl_price=sl_price,
                tp_price=tp_price,
                position_size=position_size,
                leverage=leverage
            )
            
            # Update metrics
            if success:
                self.metrics.successful_trades += 1
                self.metrics.last_trade_time = time.time()
                
                # Log to database with enhanced data
                await self._log_enhanced_trade(signals, position_data, success)
            else:
                self.metrics.failed_trades += 1
            
            return success
            
        except Exception as e:
            self.logger.error(f"❌ Dynamic trade execution error: {e}")
            return False

    async def _check_safety_conditions(self) -> bool:
        """Check various safety conditions before trading"""
        # Daily trade limit
        if self.daily_trades >= self.max_daily_trades:
            self.status["info"] = f"Daily trade limit reached ({self.daily_trades}/{self.max_daily_trades})"
            return False
        
        # Minimum balance check
        if self.available_balance < 100:  # Minimum $100
            self.status["info"] = f"Insufficient balance: ${self.available_balance:.2f}"
            return False
        
        # Maximum daily loss check
        max_daily_loss = self.available_balance * 0.10  # 10% max daily loss
        if abs(self.daily_pnl) > max_daily_loss and self.daily_pnl < 0:
            self.status["info"] = f"Daily loss limit exceeded: ${self.daily_pnl:.2f}"
            return False
        
        return True

    async def _check_whipsaw_protection(self) -> bool:
        """Check anti-whipsaw protection"""
        if self.consecutive_losses >= self.max_consecutive_losses:
            time_since_loss = time.time() - self.last_loss_time
            if time_since_loss < self.whipsaw_protection_time:
                remaining = self.whipsaw_protection_time - time_since_loss
                self.status["info"] = f"🛡️ Whipsaw protection active ({remaining:.0f}s remaining)"
                return True
        return False

    def _calculate_dynamic_sleep_time(self) -> int:
        """Calculate sleep time based on market conditions"""
        if self.market_condition == "VOLATILE":
            return 10  # More frequent checks in volatile markets
        elif self.market_condition.startswith("TRENDING"):
            return 15  # Standard frequency for trending markets
        else:
            return 30  # Less frequent in sideways markets

    def _get_interval_seconds(self) -> int:
        """Get interval in seconds for timeframe"""
        if self.config.timeframe.endswith('m'):
            return int(self.config.timeframe[:-1]) * 60
        elif self.config.timeframe.endswith('H'):
            return int(self.config.timeframe[:-1]) * 3600
        else:
            return 60

    def _process_candles(self, new_candles: List[dict]) -> None:
        """Process and clean new candles"""
        for candle in new_candles:
            if 'time' in candle and 'timestamp' not in candle:
                candle['timestamp'] = candle.pop('time')
        
        self.candle_buffer.extend(new_candles)
        
        # Clean and validate
        self.candle_buffer = [c for c in self.candle_buffer 
                             if all(k in c and c[k] is not None 
                                   for k in ['timestamp', 'close', 'high', 'low'])]
        
        # Convert types and deduplicate
        for candle in self.candle_buffer:
            try:
                candle['close'] = float(candle['close'])
                candle['high'] = float(candle['high'])
                candle['low'] = float(candle['low'])
                candle['timestamp'] = int(candle['timestamp'])
            except (ValueError, TypeError):
                continue
        
        # Deduplicate and sort
        deduplicated = {c['timestamp']: c for c in self.candle_buffer}
        self.candle_buffer = sorted(list(deduplicated.values()), 
                                   key=lambda x: x['timestamp'])[-self.buffer_max_size:]

    async def _log_enhanced_trade(self, signals: Dict[str, Any], position_data: Dict[str, Any], success: bool) -> None:
        """Log enhanced trade data for analysis"""
        try:
            trade_data = {
                'symbol': self.config.symbol,
                'signal': signals['signal'],
                'entry_price': signals['entry_price'],
                'sl_price': signals['sl_price'],
                'tp_price': signals['tp_price'],
                'position_size': position_data['position_size'],
                'leverage': position_data['leverage'],
                'risk_usd': position_data['risk_usd'],
                'risk_pct': position_data['risk_pct'],
                'margin_required': position_data['required_margin'],
                'signal_quality': signals.get('signal_quality', 0),
                'market_condition': self.market_condition,
                'trend_strength': self.trend_strength,
                'atr_multiplier': signals.get('atr_multiplier', 2.0),
                'success': success,
                'timestamp': time.time(),
                'strategy': 'Enhanced_EMA_v2'
            }
            
            # Save to database
            await self.db_manager.log_trade(trade_data)
            
        except Exception as e:
            self.logger.error(f"❌ Trade logging error: {e}")

    async def _update_status_and_save_state(self, loop_count: int) -> None:
        """Update status and save state periodically"""
        try:
            # Update status
            self.status["details"] = {
                "loop": loop_count,
                "available_balance": self.available_balance,
                "margin_used": self.margin_used,
                "market_condition": self.market_condition,
                "trend_strength": self.trend_strength,
                "daily_trades": self.daily_trades,
                "consecutive_losses": self.consecutive_losses,
                "candle_buffer_size": len(self.candle_buffer)
            }
            
            # Save state periodically
            if loop_count % 10 == 0:
                await self.db_manager.save_strategy_state(self.config.symbol, {
                    "enhanced_ema_state": {
                        "market_condition": self.market_condition,
                        "trend_strength": self.trend_strength,
                        "consecutive_losses": self.consecutive_losses,
                        "daily_trades": self.daily_trades,
                        "available_balance": self.available_balance,
                        "last_update": time.time()
                    }
                })
                
        except Exception as e:
            self.logger.error(f"❌ Status/state update error: {e}")

    # Keep existing methods from your working strategy
    async def _check_existing_position(self) -> bool:
        """Use your existing working position check method"""
        try:
            products = await self.delta_client.get_products()
            product_id = None
            for product in products:
                if product.get("symbol") == self.config.symbol:
                    product_id = product.get("id")
                    break
            
            if not product_id:
                return False
            
            positions = await self.delta_client.get_positions(str(product_id))
            
            if isinstance(positions, list):
                total_size = sum(abs(pos.get("size", 0)) for pos in positions)
            else:
                total_size = abs(positions.get("size", 0))
            
            return total_size > 0
            
        except Exception as e:
            self.logger.error(f"❌ Position check error: {e}")
            return True
