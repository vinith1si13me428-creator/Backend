# risk_manager.py - Professional Risk Management System
from typing import Dict, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

class RiskLevel(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class RiskProfile:
    """Risk profile configuration"""
    name: str
    risk_percentage: float
    leverage_max: int
    volatility_buffer: float
    risk_reward_min: float
    description: str

class RiskManager:
    """
    🛡️ Professional Risk Management System
    Handles position sizing, leverage, and risk assessment
    """
    
    RISK_PROFILES = {
        "Conservative": RiskProfile("Conservative", 0.5, 3, 1.5, 3.0, "Low risk, steady growth"),
        "Moderate": RiskProfile("Moderate", 1.0, 5, 1.0, 2.5, "Balanced risk/reward"),
        "Aggressive": RiskProfile("Aggressive", 2.0, 10, 0.8, 2.0, "High risk, high reward")
    }
    
    def __init__(self):
        self.circuit_breakers = {
            "max_daily_loss": 1000,
            "max_position_size": 100,
            "max_leverage_btc_eth": 200,
            "max_leverage_others": 100
        }
    
    def get_user_risk_limit(self, trading_mode: str, risk_percent: float, account_balance: float) -> float:
        """Calculate user's risk limit in USD"""
        if trading_mode == "Auto":
            # Use predefined profile
            profile = self.RISK_PROFILES.get("Moderate")  # Default
            return account_balance * (profile.risk_percentage / 100)
        else:
            # Use manual configuration
            return account_balance * (risk_percent / 100)
    
    def get_max_leverage(self, symbol: str, user_max: int) -> int:
        """Get maximum allowed leverage for symbol"""
        if symbol in ['BTCUSD', 'ETHUSD']:
            exchange_max = self.circuit_breakers["max_leverage_btc_eth"]
        else:
            exchange_max = self.circuit_breakers["max_leverage_others"]
        
        return min(user_max, exchange_max)
    
    def calculate_optimal_position_size(self, entry_price: float, sl_distance: float, 
        user_risk_limit: float, user_max_leverage: int,
        account_balance: float) -> Optional[Dict]:
        """
        Calculate optimal position size and leverage combination
        Returns the best combination that maximizes position size while staying within limits
        """
        
        # Account for trading fees and buffer
        fee_buffer = 0.002  # 0.2%
        available_margin = account_balance * (0.6 - fee_buffer)  # 60% available for trading
        
        best_combination = None
        best_position_size = 0
        
        # Try all leverage levels from max down to 1
        for leverage in range(user_max_leverage, 0, -1):
            # Risk constraint: position_size <= (user_risk_limit * leverage) / (sl_distance * entry_price)
            max_position_size_risk = (user_risk_limit * leverage) / (sl_distance * entry_price)
            position_size_risk = max(1, int(max_position_size_risk))
            
            # Margin constraint: position_size <= (available_margin * leverage) / entry_price
            max_position_size_margin = (available_margin * leverage) / entry_price
            position_size_margin = max(1, int(max_position_size_margin))
            
            # Take minimum of both constraints
            position_size = min(position_size_risk, position_size_margin)
            
            if position_size < 1:
                continue
            
            # Calculate actual risk and margin
            actual_risk = (sl_distance * position_size * entry_price) / leverage
            actual_margin = (position_size * entry_price) / leverage
            
            # Check constraints
            if (actual_risk <= user_risk_limit and 
                actual_margin <= available_margin and 
                position_size > best_position_size):
                
                best_combination = {
                    "position_size": position_size,
                    "leverage": leverage,
                    "risk": actual_risk,
                    "margin_required": actual_margin
                }
                best_position_size = position_size
        
        return best_combination
    
    def assess_risk_level(self, total_pnl: float, account_balance: float,
        execution_latency: float, api_success_rate: float) -> RiskLevel:
        """Assess current risk level"""
        risk_score = 0
        
        # P&L risk
        pnl_percent = (total_pnl / account_balance) * 100 if account_balance > 0 else 0
        if pnl_percent < -5:
            risk_score += 2
        elif pnl_percent < -2:
            risk_score += 1
        
        # Execution risk
        if execution_latency > 500:
            risk_score += 2
        elif execution_latency > 200:
            risk_score += 1
        
        # API risk
        if api_success_rate < 90:
            risk_score += 2
        elif api_success_rate < 95:
            risk_score += 1
        
        # Determine risk level
        if risk_score >= 4:
            return RiskLevel.CRITICAL
        elif risk_score >= 2:
            return RiskLevel.HIGH
        elif risk_score >= 1:
            return RiskLevel.MEDIUM
        else:
            return RiskLevel.LOW
    
    def check_circuit_breakers(self, total_pnl: float, position_size: int,
        execution_latency: float) -> Tuple[bool, str]:
        """Check if circuit breakers should trigger"""
        
        if total_pnl < -self.circuit_breakers["max_daily_loss"]:
            return True, "Daily loss limit exceeded"
        
        if position_size > self.circuit_breakers["max_position_size"]:
            return True, "Position size limit exceeded"
        
        if execution_latency > 1000:
            return True, "Execution latency too high"
        
        return False, "All circuit breakers OK"
    
# Enhance risk_manager.py
class AdvancedRiskManager(RiskManager):
    def calculate_kelly_criterion_size(self, win_rate, avg_win, avg_loss):
        if avg_loss == 0 or win_rate <= 0:
            return 0
        
        win_prob = win_rate / 100
        lose_prob = 1 - win_prob
        win_loss_ratio = avg_win / avg_loss
        
        kelly_percent = win_prob - (lose_prob / win_loss_ratio)
        return max(0, min(0.25, kelly_percent))  # Cap at 25%
    
    def adjust_position_for_volatility(self, base_size, current_atr, avg_atr):
        volatility_multiplier = avg_atr / current_atr if current_atr > 0 else 1
        return int(base_size * volatility_multiplier)
