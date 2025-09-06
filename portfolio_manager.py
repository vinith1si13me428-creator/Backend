# portfolio_manager.py - Advanced Portfolio Risk Management

import asyncio
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging

@dataclass
class PositionRisk:
    symbol: str
    risk_amount: float
    position_size: float
    entry_price: float
    current_price: float
    unrealized_pnl: float
    strategy: str

class PortfolioManager:
    """
    🏦 Advanced Portfolio Risk Management System
    Manages cross-strategy risk, correlation, and portfolio-level controls
    """
    
    def __init__(self, max_portfolio_risk: float = 0.05):
        self.max_portfolio_risk = max_portfolio_risk  # 5% max total risk
        self.max_correlation_exposure = 0.3  # 30% max to correlated assets
        self.correlation_matrix = {
            ('BTCUSD', 'ETHUSD'): 0.85,  # High correlation
            ('BTCUSD', 'SOLUSD'): 0.75,
            ('ETHUSD', 'SOLUSD'): 0.70,
            ('XRPUSD', 'BTCUSD'): 0.65,
        }
        self.logger = logging.getLogger("PortfolioManager")
        
    def get_correlation(self, symbol1: str, symbol2: str) -> float:
        """Get correlation coefficient between two symbols"""
        key = (symbol1, symbol2) if symbol1 < symbol2 else (symbol2, symbol1)
        return self.correlation_matrix.get(key, 0.0)
    
    async def calculate_portfolio_risk(self, positions: List[PositionRisk], 
                                     account_balance: float) -> Dict[str, float]:
        """Calculate comprehensive portfolio risk metrics"""
        if not positions:
            return {
                "total_risk": 0.0,
                "risk_percentage": 0.0,
                "correlation_risk": 0.0,
                "diversification_ratio": 1.0,
                "max_drawdown_risk": 0.0
            }
        
        # Calculate individual risks
        total_individual_risk = sum(pos.risk_amount for pos in positions)
        
        # Calculate correlation-adjusted risk
        correlation_risk = self._calculate_correlation_risk(positions)
        
        # Diversification ratio
        diversification_ratio = total_individual_risk / max(correlation_risk, 0.01)
        
        # Portfolio risk as percentage of account
        risk_percentage = correlation_risk / account_balance * 100
        
        return {
            "total_risk": correlation_risk,
            "risk_percentage": risk_percentage,
            "correlation_risk": correlation_risk - total_individual_risk,
            "diversification_ratio": diversification_ratio,
            "max_drawdown_risk": self._estimate_max_drawdown(positions)
        }
    
    def _calculate_correlation_risk(self, positions: List[PositionRisk]) -> float:
        """Calculate correlation-adjusted portfolio risk"""
        n = len(positions)
        if n <= 1:
            return sum(pos.risk_amount for pos in positions)
        
        # Create risk matrix
        risks = np.array([pos.risk_amount for pos in positions])
        symbols = [pos.symbol for pos in positions]
        
        # Create correlation matrix
        correlation_matrix = np.eye(n)
        for i in range(n):
            for j in range(i+1, n):
                corr = self.get_correlation(symbols[i], symbols[j])
                correlation_matrix[i,j] = correlation_matrix[j,i] = corr
        
        # Calculate portfolio variance: w'Σw where Σ is covariance matrix
        # Simplified: assume equal volatility, so covariance = correlation
        portfolio_variance = np.dot(risks, np.dot(correlation_matrix, risks))
        
        return np.sqrt(portfolio_variance)
    
    def _estimate_max_drawdown(self, positions: List[PositionRisk]) -> float:
        """Estimate maximum potential drawdown"""
        # Simplified drawdown estimation based on historical volatility
        total_exposure = sum(abs(pos.position_size * pos.current_price) for pos in positions)
        
        # Assume worst-case scenario: 20% market drop with correlations
        worst_case_drop = 0.20
        correlation_factor = 0.8  # Markets tend to correlate in crashes
        
        return total_exposure * worst_case_drop * correlation_factor
    
    async def should_allow_new_trade(self, new_position: PositionRisk, 
                                   current_positions: List[PositionRisk],
                                   account_balance: float) -> Tuple[bool, str]:
        """Determine if a new trade should be allowed based on portfolio risk"""
        
        # Test with new position added
        test_positions = current_positions + [new_position]
        portfolio_risk = await self.calculate_portfolio_risk(test_positions, account_balance)
        
        # Check total portfolio risk
        if portfolio_risk["risk_percentage"] > self.max_portfolio_risk * 100:
            return False, f"Portfolio risk {portfolio_risk['risk_percentage']:.2f}% exceeds limit {self.max_portfolio_risk*100:.1f}%"
        
        # Check correlation concentration
        symbol_exposure = self._calculate_symbol_concentration(test_positions, new_position.symbol)
        if symbol_exposure > self.max_correlation_exposure:
            return False, f"Correlation exposure {symbol_exposure:.1%} exceeds limit {self.max_correlation_exposure:.1%}"
        
        # Check maximum drawdown
        if portfolio_risk["max_drawdown_risk"] > account_balance * 0.15:  # 15% max drawdown
            return False, f"Potential drawdown too high: {portfolio_risk['max_drawdown_risk']:,.0f}"
        
        return True, "Trade approved by portfolio risk management"
    
    def _calculate_symbol_concentration(self, positions: List[PositionRisk], target_symbol: str) -> float:
        """Calculate exposure concentration for correlated symbols"""
        total_risk = sum(pos.risk_amount for pos in positions)
        if total_risk == 0:
            return 0
        
        # Calculate correlated exposure
        correlated_risk = 0
        for pos in positions:
            if pos.symbol == target_symbol:
                correlated_risk += pos.risk_amount
            else:
                correlation = self.get_correlation(pos.symbol, target_symbol)
                correlated_risk += pos.risk_amount * correlation
        
        return correlated_risk / total_risk
    
    def get_portfolio_recommendations(self, positions: List[PositionRisk], 
                                    account_balance: float) -> List[str]:
        """Generate portfolio optimization recommendations"""
        recommendations = []
        
        if not positions:
            return ["Portfolio is empty - consider starting with low-risk positions"]
        
        # Check for over-concentration
        symbol_risks = {}
        for pos in positions:
            symbol_risks[pos.symbol] = symbol_risks.get(pos.symbol, 0) + pos.risk_amount
        
        total_risk = sum(symbol_risks.values())
        for symbol, risk in symbol_risks.items():
            concentration = risk / total_risk if total_risk > 0 else 0
            if concentration > 0.4:  # 40% concentration warning
                recommendations.append(f"⚠️ High concentration in {symbol}: {concentration:.1%}")
        
        # Check for correlation risk
        btc_exposure = symbol_risks.get('BTCUSD', 0)
        eth_exposure = symbol_risks.get('ETHUSD', 0)
        combined_btc_eth = (btc_exposure + eth_exposure * 0.85) / total_risk if total_risk > 0 else 0
        
        if combined_btc_eth > 0.6:
            recommendations.append(f"⚠️ High BTC/ETH correlation exposure: {combined_btc_eth:.1%}")
            recommendations.append("💡 Consider adding non-correlated assets like XRPUSD")
        
        # Overall risk level recommendations
        risk_pct = total_risk / account_balance * 100
        if risk_pct < 1:
            recommendations.append("💡 Conservative risk level - consider increasing position sizes")
        elif risk_pct > 3:
            recommendations.append("⚠️ Aggressive risk level - consider reducing position sizes")
        
        return recommendations if recommendations else ["✅ Portfolio risk is well-balanced"]
