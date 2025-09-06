# delta_client.py - Professional Delta Exchange API Client

import os
import time
import hmac
import hashlib
import json
import aiohttp
import asyncio
from urllib.parse import urlencode
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum
import time

class DeltaEndpoints(Enum):
    """Delta Exchange API endpoints"""
    BASE_URL = "https://api.india.delta.exchange"
    PRODUCTS = "/v2/products"
    TICKERS = "/v2/tickers"
    ORDERS = "/v2/orders"
    BRACKET_ORDERS = "/v2/orders/bracket"
    POSITIONS = "/v2/positions/margined"
    WALLET = "/v2/wallet/balances"
    FILLS = "/v2/fills"
    CANDLES = "/v2/history/candles"
    LEVERAGE = "/v2/products/{product_id}/orders/leverage"
    WEBSOCKET = "wss://socket.india.delta.exchange"

@dataclass
class DeltaCredentials:
    """Delta API credentials"""
    api_key: str
    api_secret: str
    base_url: str = DeltaEndpoints.BASE_URL.value

class DeltaAPIClient:
    """
    🏛️ Professional Delta Exchange API Client
    Handles all API communications with proper authentication and error handling
    """

    def __init__(self, credentials: DeltaCredentials):
        self.credentials = credentials
        self.session_timeout = aiohttp.ClientTimeout(total=30)

    def _generate_signature(self, method: str, path: str, params: dict = None, body: dict = None) -> tuple:
        """Generate HMAC SHA256 signature for Delta API"""
        timestamp = str(int(time.time()))
        method = method.upper()
        params_str = '' if not params else '?' + urlencode(params)
        body_str = '' if not body else json.dumps(body, separators=(',', ':'))
        payload = f"{method}{timestamp}{path}{params_str}{body_str}"

        signature = hmac.new(
            self.credentials.api_secret.encode(),
            payload.encode(),
            hashlib.sha256
        ).hexdigest()

        return timestamp, signature, body_str

    async def request(self, method: str, endpoint: str, params: dict | None = None, body: dict | None = None, retries: int = 3) -> dict:
        """Make authenticated request to Delta API"""
        url = self.credentials.base_url + endpoint

        for attempt in range(retries):
            try:
                timestamp, signature, body_str = self._generate_signature(method, endpoint, params, body)

                headers = {
                    "api-key": self.credentials.api_key,
                    "timestamp": timestamp,
                    "signature": signature,
                    "Content-Type": "application/json"
                }

                async with aiohttp.ClientSession(timeout=self.session_timeout) as session:
                    async with session.request(
                        method, url,
                        headers=headers,
                        params=params,
                        data=body_str if body else None
                    ) as response:
                        if response.status == 401:
                            error_data = await response.json()
                            if error_data.get("error", {}).get("code") == "expired_signature" and attempt < retries - 1:
                                await asyncio.sleep(0.5)
                                continue

                        response.raise_for_status()
                        return await response.json()

            except Exception as e:
                if attempt < retries - 1:
                    await asyncio.sleep(1)
                    continue
                raise Exception(f"Delta API error after {retries} attempts: {str(e)}")

    # === MARKET DATA METHODS ===

    async def get_products(self) -> List[dict]:
        """Get all available products"""
        response = await self.request("GET", DeltaEndpoints.PRODUCTS.value)
        return response.get("result", [])

    async def get_ticker(self, symbol: str = None, product_id: int = None) -> dict:
        """Get ticker data for specific product"""
        params = {}
        if product_id:
            params["product_id"] = product_id

        endpoint = f"{DeltaEndpoints.TICKERS.value}/{symbol}" if symbol else DeltaEndpoints.TICKERS.value
        response = await self.request("GET", endpoint, params)
        return response.get("result", {})

    async def get_candles(self, symbol, timeframe, start_ts=None, end_ts=None, limit=100):
        """Get historical candlestick data from Delta Exchange"""
        try:
            # Convert timeframe to Delta Exchange resolution format
            resolution_map = {
                "1m": "1m",
                "5m": "5m", 
                "15m": "15m",
                "30m": "30m",
                "1h": "1h",
                "4h": "4h",
                "1d": "1d"
            }
            
            resolution = resolution_map.get(timeframe, "1m")
            
            # Build parameters for the API call
            params = {
                "symbol": symbol,
                "resolution": resolution
            }
            
            # Add time range if provided
            if start_ts:
                params["start"] = int(start_ts)
            if end_ts:
                params["end"] = int(end_ts)
            if limit:
                params["limit"] = limit
                
            # If no time range provided, get recent data (last 24 hours)
            if not start_ts and not end_ts:
                from datetime import datetime, timedelta
                end_time = int(time.time())
                start_time = int((datetime.now() - timedelta(hours=24)).timestamp())
                params["start"] = start_time
                params["end"] = end_time
            
            print(f"📊 Fetching candles: {symbol} {resolution} (limit: {limit})")
            
            # Make the authenticated API request
            response = await self.request("GET", DeltaEndpoints.CANDLES.value, params=params)
            
            if response and "result" in response:
                candles = response["result"]
                
                # Process candles to ensure consistent format
                processed_candles = []
                for candle in candles:
                    # Handle different possible formats from Delta API
                    if isinstance(candle, dict):
                        # If candle is already a dict with named fields
                        processed_candle = {
                            "time": int(candle.get("time", candle.get("timestamp", 0))),
                            "open": float(candle.get("open", 0)),
                            "high": float(candle.get("high", 0)), 
                            "low": float(candle.get("low", 0)),
                            "close": float(candle.get("close", 0)),
                            "volume": float(candle.get("volume", 0))
                        }
                    elif isinstance(candle, list) and len(candle) >= 6:
                        # If candle is array format [time, open, high, low, close, volume]
                        processed_candle = {
                            "time": int(candle[0]),
                            "open": float(candle[1]),
                            "high": float(candle[2]), 
                            "low": float(candle[3]),
                            "close": float(candle[4]),
                            "volume": float(candle[5])
                        }
                    else:
                        # Skip invalid candle data
                        continue
                    
                    processed_candles.append(processed_candle)
                
                print(f"✅ Retrieved {len(processed_candles)} candles for {symbol}")
                return processed_candles
            else:
                print(f"⚠️ No candle data received for {symbol}")
                return []
                
        except Exception as e:
            print(f"❌ Error fetching candles for {symbol}: {e}")
            return []

    # === WALLET METHODS ===

    async def get_wallet_balances(self) -> List[dict]:
        """Get wallet balances"""
        response = await self.request("GET", DeltaEndpoints.WALLET.value)
        return response.get("result", [])

    async def get_account_balance(self) -> float:
        """Get USD/USDT account balance"""
        balances = await self.get_wallet_balances()
        for balance in balances:
            if balance.get("asset", {}).get("symbol") in ["USDT", "USD"]:
                return float(balance.get("balance", 0))
        return 0.0 if not balances else float(balances[0].get("balance", 0))

    # === ORDER METHODS ===

    async def place_order(self, order_data: dict) -> dict:
        """Place a new order"""
        response = await self.request("POST", DeltaEndpoints.ORDERS.value, body=order_data)
        return response.get("result", {})

    async def place_bracket_order(self, bracket_data: dict) -> dict:
        """Place bracket order (TP/SL)"""
        response = await self.request("POST", DeltaEndpoints.BRACKET_ORDERS.value, body=bracket_data)
        return response.get("result", {})

    async def get_order(self, order_id: str) -> dict:
        """Get order by ID"""
        response = await self.request("GET", f"{DeltaEndpoints.ORDERS.value}/{order_id}")
        return response.get("result", {})

    async def get_open_orders(self, product_ids: str = None) -> List[dict]:
        """Get open orders"""
        params = {"state": "open"}
        if product_ids:
            params["product_ids"] = product_ids

        response = await self.request("GET", DeltaEndpoints.ORDERS.value, params)
        return response.get("result", [])

    async def cancel_order(self, order_id: str) -> dict:
        """Cancel an order"""
        response = await self.request("DELETE", f"{DeltaEndpoints.ORDERS.value}/{order_id}")
        return response.get("result", {})

    async def cancel_all_orders(self, product_id: int = None) -> dict:
        """Cancel all orders"""
        params = {}
        if product_id:
            params["product_id"] = product_id
        
        response = await self.request("DELETE", DeltaEndpoints.ORDERS.value, params=params)
        return response.get("result", {})

    async def set_leverage(self, product_id: int, leverage: int) -> dict:
        """Set leverage for product"""
        endpoint = DeltaEndpoints.LEVERAGE.value.format(product_id=product_id)
        body = {"leverage": leverage}
        response = await self.request("POST", endpoint, body=body)
        return response.get("result", {})

    # === POSITION METHODS ===

    async def get_positions(self, product_ids: str = None) -> List[dict]:
        """Get positions"""
        params = {}
        if product_ids:
            params["product_ids"] = product_ids

        response = await self.request("GET", DeltaEndpoints.POSITIONS.value, params)
        return response.get("result", [])

    async def get_position(self, product_id: int) -> dict:
        """Get position for specific product"""
        positions = await self.get_positions(str(product_id))
        for position in positions:
            if position.get("product_id") == product_id:
                return position
        return {}

    async def close_position(self, product_id: int, size: float = None) -> dict:
        """Close position (market order)"""
        position = await self.get_position(product_id)
        if not position:
            return {"error": "No position found"}
        
        current_size = float(position.get("size", 0))
        if current_size == 0:
            return {"error": "No position to close"}
        
        # Determine close size and side
        close_size = abs(size) if size else abs(current_size)
        side = "sell" if current_size > 0 else "buy"
        
        order_data = {
            "product_id": product_id,
            "size": close_size,
            "side": side,
            "order_type": "market_order",
            "reduce_only": True
        }
        
        return await self.place_order(order_data)

    async def get_fills(self, order_id: str = None, product_ids: str = None) -> List[dict]:
        """Get fill history"""
        params = {}
        if order_id:
            params["order_id"] = order_id
        if product_ids:
            params["product_ids"] = product_ids

        response = await self.request("GET", DeltaEndpoints.FILLS.value, params)
        return response.get("result", [])

    # === UTILITY METHODS ===

    async def get_server_time(self) -> dict:
        """Get server time"""
        try:
            response = await self.request("GET", "/v2/time")
            return response.get("result", {})
        except Exception as e:
            print(f"❌ Error getting server time: {e}")
            return {"timestamp": int(time.time())}

    async def get_trading_symbols(self) -> List[str]:
        """Get list of all trading symbols"""
        try:
            products = await self.get_products()
            symbols = []
            for product in products:
                if product.get("trading_status") == "operational":
                    symbols.append(product.get("symbol"))
            return symbols
        except Exception as e:
            print(f"❌ Error getting trading symbols: {e}")
            return []

    async def get_product_by_symbol(self, symbol: str) -> dict:
        """Get product information by symbol"""
        try:
            products = await self.get_products()
            for product in products:
                if product.get("symbol") == symbol:
                    return product
            return {}
        except Exception as e:
            print(f"❌ Error getting product for {symbol}: {e}")
            return {}

    async def health_check(self) -> bool:
        """Check if API connection is healthy"""
        try:
            await self.get_server_time()
            return True
        except Exception as e:
            print(f"❌ API health check failed: {e}")
            return False

    def __str__(self) -> str:
        """String representation"""
        return f"DeltaAPIClient(base_url='{self.credentials.base_url}')"

    def __repr__(self) -> str:
        """Debug representation"""
        return f"DeltaAPIClient(api_key='{self.credentials.api_key[:8]}...', base_url='{self.credentials.base_url}')"
