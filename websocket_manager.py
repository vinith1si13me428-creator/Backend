# websocket_manager.py - Professional WebSocket Management System

import asyncio
import json
import hmac
import hashlib
import time
import websockets
from typing import Dict, Callable, Optional, Any
import logging
from enum import Enum

class WSConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"

class DeltaWebSocketManager:
    """
    🔌 Professional WebSocket Management System
    Handles Delta Exchange WebSocket connections with auto-reconnection
    """

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.ws_url = "wss://socket.india.delta.exchange"
        
        # Connection management
        self.connections: Dict[str, websockets.WebSocketServerProtocol] = {}
        self.connection_states: Dict[str, WSConnectionState] = {}
        self.subscription_handlers: Dict[str, Callable] = {}
        
        # Reconnection settings
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 2  # seconds
        self.logger = logging.getLogger("WebSocketManager")

    async def connect_symbol(self, symbol: str, handler: Callable[[str, Dict], None]) -> bool:
        """Connect WebSocket for a specific symbol"""
        if symbol in self.connections:
            self.logger.warning(f"WebSocket already connected for {symbol}")
            return True

        self.subscription_handlers[symbol] = handler
        self.connection_states[symbol] = WSConnectionState.CONNECTING

        success = await self._establish_connection(symbol)
        if success:
            # Start message listener
            asyncio.create_task(self._message_listener(symbol))

        return success

    async def disconnect_symbol(self, symbol: str):
        """Disconnect WebSocket for a symbol"""
        if symbol in self.connections:
            try:
                await self.connections[symbol].close()
                del self.connections[symbol]
                del self.connection_states[symbol]
                if symbol in self.subscription_handlers:
                    del self.subscription_handlers[symbol]
                self.logger.info(f"Disconnected WebSocket for {symbol}")
            except Exception as e:
                self.logger.error(f"Error disconnecting {symbol}: {e}")

    async def _establish_connection(self, symbol: str) -> bool:
        """Establish WebSocket connection with authentication"""
        try:
            # Connect to WebSocket
            ws = await websockets.connect(
                self.ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5
            )

            # Authenticate
            timestamp = int(time.time() * 1000)
            path = "/v2/ws"
            payload = f"GET{path}{timestamp}"
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                payload.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()

            auth_msg = {
                "type": "auth",
                "api-key": self.api_key,
                "timestamp": timestamp,
                "signature": signature
            }

            await ws.send(json.dumps(auth_msg))

            # Wait for auth response (optional, can skip if stable)
            try:
                response = await asyncio.wait_for(ws.recv(), timeout=5.0)
                auth_response = json.loads(response)
                if auth_response.get("type") == "auth" and not auth_response.get("success", True):
                    self.logger.error(f"Authentication failed for {symbol}")
                    await ws.close()
                    return False
            except asyncio.TimeoutError:
                # Auth might be silent, continue
                pass

            # Subscribe to ticker
            subscribe_msg = {
                "type": "subscribe",
                "channels": [{"name": "v2/ticker", "symbols": [symbol]}]
            }

            await ws.send(json.dumps(subscribe_msg))

            # Store connection
            self.connections[symbol] = ws
            self.connection_states[symbol] = WSConnectionState.CONNECTED

            self.logger.info(f"✅ WebSocket connected and subscribed for {symbol}")
            return True

        except Exception as e:
            self.logger.error(f"❌ Failed to establish WebSocket connection for {symbol}: {e}")
            self.connection_states[symbol] = WSConnectionState.ERROR
            return False

    async def _message_listener(self, symbol: str):
        """Listen for WebSocket messages"""
        reconnect_attempts = 0

        while symbol in self.connections and reconnect_attempts < self.max_reconnect_attempts:
            try:
                ws = self.connections[symbol]
                async for message in ws:
                    try:
                        data = json.loads(message)

                        # Call the registered handler
                        if symbol in self.subscription_handlers:
                            await self.subscription_handlers[symbol](symbol, data)

                    except json.JSONDecodeError:
                        self.logger.warning(f"Invalid JSON received for {symbol}")
                    except Exception as e:
                        self.logger.error(f"Error processing message for {symbol}: {e}")

            except websockets.ConnectionClosed:
                self.logger.warning(f"WebSocket connection closed for {symbol}")
                self.connection_states[symbol] = WSConnectionState.RECONNECTING

                # Attempt reconnection
                reconnect_attempts += 1
                if reconnect_attempts < self.max_reconnect_attempts:
                    self.logger.info(f"Attempting reconnection {reconnect_attempts}/{self.max_reconnect_attempts} for {symbol}")
                    await asyncio.sleep(self.reconnect_delay * reconnect_attempts)  # Exponential backoff
                    if await self._establish_connection(symbol):
                        reconnect_attempts = 0  # Reset on successful reconnection
                        continue

            except Exception as e:
                self.logger.error(f"WebSocket error for {symbol}: {e}")
                reconnect_attempts += 1
                await asyncio.sleep(self.reconnect_delay)

        # Max reconnect attempts reached
        if symbol in self.connections:
            self.logger.error(f"Max reconnection attempts reached for {symbol}")
            self.connection_states[symbol] = WSConnectionState.ERROR
            await self.disconnect_symbol(symbol)

    def get_connection_status(self, symbol: str) -> WSConnectionState:
        """Get connection status for a symbol"""
        return self.connection_states.get(symbol, WSConnectionState.DISCONNECTED)

    def is_connected(self, symbol: str) -> bool:
        """Check if symbol is connected"""
        return self.get_connection_status(symbol) == WSConnectionState.CONNECTED
