# order_executor.py - Professional Order Execution System
import asyncio
import uuid
import time
from typing import Dict, Optional
import logging

from delta_client import DeltaAPIClient
from database_manager import DatabaseManager

class OrderExecutor:
    """
    ⚡ Professional Order Execution System
    Handles all order placement and execution logic
    """
    
    def __init__(self, delta_client: DeltaAPIClient, db_manager: DatabaseManager):
        self.delta_client = delta_client
        self.db_manager = db_manager
        self.logger = logging.getLogger("OrderExecutor")
        
        # Track execution metrics
        self.execution_count = 0
        self.success_count = 0
        self.total_latency = 0.0
    
    async def execute_ema_trade(self, symbol: str, signal: str, entry_price: float,
        sl_price: float, tp_price: float, position_size: int,
        leverage: int) -> bool:
        """Execute EMA strategy trade with proper error handling"""
        
        start_time = time.perf_counter()
        self.execution_count += 1
        
        try:
            # Get product ID for symbol
            product_id = await self._get_product_id(symbol)
            if not product_id:
                self.logger.error(f"Product ID not found for {symbol}")
                return False
            
            # Validate parameters
            position_size = max(1, int(round(float(position_size))))
            leverage = max(1, min(int(leverage), 200))
            
            self.logger.info(f"📋 Executing {signal.upper()} order: {symbol}")
            self.logger.info(f"   Size: {position_size}, Entry: ${entry_price:.4f}, Leverage: {leverage}x")
            
            # Step 1: Set leverage
            try:
                await self.delta_client.set_leverage(product_id, leverage)
                self.logger.info(f"✅ Leverage set to {leverage}x")
            except Exception as e:
                self.logger.warning(f"⚠️ Leverage setting failed: {e}")
            
            # Step 2: Place main order
            main_order = {
                "product_id": product_id,
                "order_type": "market_order",
                "side": signal,
                "size": position_size,
                "reduce_only": False,
                "client_order_id": str(uuid.uuid4())
            }
            
            main_response = await self.delta_client.place_order(main_order)
            
            if not main_response or not main_response.get('id'):
                self.logger.error("❌ Main order failed")
                return False
            
            order_id = main_response.get('id')
            self.logger.info(f"✅ Main order placed: {order_id}")
            
            # Step 3: Log trade to database
            await self._log_trade_to_database(
                symbol, signal, position_size, entry_price, 
                sl_price, tp_price, leverage
            )
            
            # Step 4: Place bracket orders (TP/SL) after brief delay
            await asyncio.sleep(2)
            
            bracket_success = await self._place_bracket_orders(
                product_id, symbol, sl_price, tp_price
            )
            
            if bracket_success:
                self.logger.info("✅ Bracket orders placed successfully")
            else:
                self.logger.warning("⚠️ Bracket orders failed, but main order succeeded")
            
            # Update metrics
            execution_time = (time.perf_counter() - start_time) * 1000
            self.total_latency += execution_time
            self.success_count += 1
            
            self.logger.info(f"🎯 Trade execution completed in {execution_time:.2f}ms")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Order execution error: {e}")
            return False
    
    async def _get_product_id(self, symbol: str) -> Optional[int]:
        """Get product ID for symbol"""
        try:
            products = await self.delta_client.get_products()
            for product in products:
                if product.get("symbol") == symbol:
                    return product.get("id")
            return None
        except Exception as e:
            self.logger.error(f"Error getting product ID: {e}")
            return None
    
    async def _place_bracket_orders(self, product_id: int, symbol: str, 
        sl_price: float, tp_price: float) -> bool:
        """Place bracket orders (TP/SL)"""
        try:
            bracket_payload = {
                "product_id": product_id,
                "stop_loss_order": {
                    "order_type": "market_order",
                    "stop_price": str(sl_price)
                },
                "take_profit_order": {
                    "order_type": "limit_order", 
                    "limit_price": str(tp_price)
                },
                "bracket_stop_trigger_method": "last_traded_price"
            }
            
            self.logger.info(f"📤 Placing bracket orders - SL: ${sl_price:.4f}, TP: ${tp_price:.4f}")
            
            bracket_response = await self.delta_client.place_bracket_order(bracket_payload)
            
            if bracket_response:
                self.logger.info("✅ Bracket orders placed successfully")
                return True
            else:
                self.logger.warning("⚠️ Bracket order placement returned empty response")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Bracket order error: {e}")
            return False
    
    
    # Add to order_executor.py
    async def execute_iceberg_order(self, symbol, side, total_size, chunk_size):
        """Execute large orders in chunks to minimize market impact"""
        chunks = []
        remaining = total_size
    
        while remaining > 0:
            current_chunk = min(chunk_size, remaining)
            order_result = await self.place_order({
                "product_id": product_id,
                "side": side,
                "size": current_chunk,
                "order_type": "limit_order"
            })
            chunks.append(order_result)
            remaining -= current_chunk
            await asyncio.sleep(2)  # Delay between chunks
    
        return chunks

    
    
    async def _log_trade_to_database(self, symbol: str, side: str, size: int,
        entry_price: float, sl_price: float, tp_price: float,
        leverage: int) -> None:
        """Log trade to database"""
        try:
            trade_data = {
                'symbol': symbol,
                'side': side,
                'size': size,
                'entry_price': entry_price,
                'sl_price': sl_price,
                'tp_price': tp_price,
                'leverage': leverage,
                'strategy': 'Enhanced EMA Strategy',
                'status': 'Open'
            }
            
            await self.db_manager.save_trade_to_journal(trade_data)
            self.logger.info(f"✅ Trade logged to database")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Database logging failed: {e}")
    
    def get_execution_metrics(self) -> Dict[str, float]:
        """Get execution performance metrics"""
        if self.execution_count == 0:
            return {
                "total_executions": 0,
                "success_rate": 100.0,
                "average_latency_ms": 0.0
            }
        
        return {
            "total_executions": self.execution_count,
            "success_rate": (self.success_count / self.execution_count) * 100,
            "average_latency_ms": self.total_latency / self.execution_count
        }
