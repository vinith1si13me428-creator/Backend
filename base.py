# strategies/base.py - Clean cancellation pattern from your old working code

import asyncio
import time
from abc import ABC, abstractmethod

class Strategy(ABC):
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

    @abstractmethod
    async def run(self):
        """Main strategy logic - implement in subclasses"""
        pass

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
    
    async def save_state(self):
        """Save strategy state using the correct async method"""
        try:
            state = {
                'loop_count': getattr(self, 'loop_count', 0),
                'last_signal': getattr(self, 'last_signal', None),
                'last_trade_time': getattr(self, 'last_trade_time', 0),
                'instance_id': getattr(self, 'instance_id', str(uuid.uuid4())[:8]),
                'saved_at': time.time()
            }

            # Call the correct async method with await
            if hasattr(self, 'db_manager') and hasattr(self.db_manager, 'save_strategy_state'):
                await self.db_manager.save_strategy_state(self.config.symbol, state)
                print(f"[{self.config.symbol}] ✅ State saved successfully")
            else:
                print(f"[{self.config.symbol}] ⚠️ db_manager missing save_strategy_state method")

        except Exception as e:
            print(f"[{self.config.symbol}] ❌ Failed to save state: {e}")
