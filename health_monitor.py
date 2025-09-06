# health_monitor.py - Comprehensive System Health Monitoring

import asyncio
import time
import psutil
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum

class HealthStatus(Enum):
    HEALTHY = "healthy"
    WARNING = "warning" 
    CRITICAL = "critical"
    UNKNOWN = "unknown"

@dataclass
class HealthMetric:
    name: str
    status: HealthStatus
    value: float
    threshold: float
    message: str
    timestamp: float
    details: Dict[str, Any] = None

class SystemHealthMonitor:
    """
    🏥 Comprehensive System Health Monitoring
    Monitors all aspects of the trading bot for optimal performance
    """
    
    def __init__(self, delta_client, db_manager, strategy_manager):
        self.delta_client = delta_client
        self.db_manager = db_manager
        self.strategy_manager = strategy_manager
        self.logger = logging.getLogger("HealthMonitor")
        
        # Health thresholds
        self.thresholds = {
            "cpu_usage": 80.0,          # %
            "memory_usage": 85.0,       # %
            "disk_usage": 90.0,         # %
            "api_latency": 1000.0,      # ms
            "api_success_rate": 95.0,   # %
            "db_response_time": 500.0,  # ms
            "websocket_lag": 5000.0,    # ms
        }
        
        # Metrics history
        self.metrics_history: Dict[str, List[HealthMetric]] = {}
        self.alerts_sent = set()
        
    async def check_system_health(self) -> Dict[str, HealthMetric]:
        """Perform comprehensive system health check"""
        health_checks = {}
        current_time = time.time()
        
        try:
            # System resource checks
            health_checks["cpu"] = await self._check_cpu_health()
            health_checks["memory"] = await self._check_memory_health()
            health_checks["disk"] = await self._check_disk_health()
            
            # Application checks  
            health_checks["database"] = await self._check_database_health()
            health_checks["api"] = await self._check_api_health()
            health_checks["websocket"] = await self._check_websocket_health()
            health_checks["strategies"] = await self._check_strategies_health()
            
            # Store metrics history
            for name, metric in health_checks.items():
                if name not in self.metrics_history:
                    self.metrics_history[name] = []
                
                self.metrics_history[name].append(metric)
                
                # Keep only last 100 metrics per type
                if len(self.metrics_history[name]) > 100:
                    self.metrics_history[name] = self.metrics_history[name][-100:]
            
            # Check for alerts
            await self._process_health_alerts(health_checks)
            
        except Exception as e:
            self.logger.error(f"Health check error: {e}")
            health_checks["monitor"] = HealthMetric(
                name="System Monitor",
                status=HealthStatus.CRITICAL,
                value=0,
                threshold=1,
                message=f"Health monitor error: {str(e)}",
                timestamp=current_time
            )
        
        return health_checks
    
    async def _check_cpu_health(self) -> HealthMetric:
        """Check CPU usage"""
        cpu_percent = psutil.cpu_percent(interval=1)
        threshold = self.thresholds["cpu_usage"]
        
        if cpu_percent > threshold:
            status = HealthStatus.CRITICAL
            message = f"High CPU usage: {cpu_percent:.1f}%"
        elif cpu_percent > threshold * 0.8:
            status = HealthStatus.WARNING
            message = f"Elevated CPU usage: {cpu_percent:.1f}%"
        else:
            status = HealthStatus.HEALTHY
            message = f"CPU usage normal: {cpu_percent:.1f}%"
        
        return HealthMetric(
            name="CPU Usage",
            status=status,
            value=cpu_percent,
            threshold=threshold,
            message=message,
            timestamp=time.time()
        )
    
    async def _check_memory_health(self) -> HealthMetric:
        """Check memory usage"""
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        threshold = self.thresholds["memory_usage"]
        
        if memory_percent > threshold:
            status = HealthStatus.CRITICAL
            message = f"High memory usage: {memory_percent:.1f}%"
        elif memory_percent > threshold * 0.8:
            status = HealthStatus.WARNING  
            message = f"Elevated memory usage: {memory_percent:.1f}%"
        else:
            status = HealthStatus.HEALTHY
            message = f"Memory usage normal: {memory_percent:.1f}%"
        
        return HealthMetric(
            name="Memory Usage",
            status=status,
            value=memory_percent,
            threshold=threshold,
            message=message,
            timestamp=time.time(),
            details={
                "total_gb": round(memory.total / (1024**3), 2),
                "available_gb": round(memory.available / (1024**3), 2),
                "used_gb": round(memory.used / (1024**3), 2)
            }
        )
    
    async def _check_disk_health(self) -> HealthMetric:
        """Check disk usage"""
        disk = psutil.disk_usage('/')
        disk_percent = (disk.used / disk.total) * 100
        threshold = self.thresholds["disk_usage"]
        
        if disk_percent > threshold:
            status = HealthStatus.CRITICAL
            message = f"Low disk space: {disk_percent:.1f}% used"
        elif disk_percent > threshold * 0.8:
            status = HealthStatus.WARNING
            message = f"Disk space getting low: {disk_percent:.1f}% used"
        else:
            status = HealthStatus.HEALTHY
            message = f"Disk usage normal: {disk_percent:.1f}% used"
        
        return HealthMetric(
            name="Disk Usage",
            status=status,
            value=disk_percent,
            threshold=threshold,
            message=message,
            timestamp=time.time(),
            details={
                "total_gb": round(disk.total / (1024**3), 2),
                "free_gb": round(disk.free / (1024**3), 2),
                "used_gb": round(disk.used / (1024**3), 2)
            }
        )
    
    async def _check_database_health(self) -> HealthMetric:
        """Check database connectivity and performance"""
        start_time = time.time()
        try:
            # Test database connection
            test_data = {"test": "health_check", "timestamp": start_time}
            await self.db_manager.save_strategy_state("_health_test", test_data)
            
            response_time = (time.time() - start_time) * 1000  # ms
            threshold = self.thresholds["db_response_time"]
            
            if response_time > threshold:
                status = HealthStatus.WARNING
                message = f"Slow database response: {response_time:.1f}ms"
            else:
                status = HealthStatus.HEALTHY
                message = f"Database responsive: {response_time:.1f}ms"
            
            return HealthMetric(
                name="Database",
                status=status,
                value=response_time,
                threshold=threshold,
                message=message,
                timestamp=time.time()
            )
            
        except Exception as e:
            return HealthMetric(
                name="Database",
                status=HealthStatus.CRITICAL,
                value=0,
                threshold=1,
                message=f"Database error: {str(e)}",
                timestamp=time.time()
            )
    
    async def _check_api_health(self) -> HealthMetric:
        """Check Delta Exchange API connectivity"""
        start_time = time.time()
        try:
            # Test API call
            health_data = await self.delta_client.get_products()
            response_time = (time.time() - start_time) * 1000  # ms
            
            threshold = self.thresholds["api_latency"]
            
            if response_time > threshold:
                status = HealthStatus.WARNING
                message = f"High API latency: {response_time:.1f}ms"
            else:
                status = HealthStatus.HEALTHY
                message = f"API responsive: {response_time:.1f}ms"
            
            return HealthMetric(
                name="Delta API",
                status=status,
                value=response_time,
                threshold=threshold,
                message=message,
                timestamp=time.time(),
                details={"products_count": len(health_data)}
            )
            
        except Exception as e:
            return HealthMetric(
                name="Delta API", 
                status=HealthStatus.CRITICAL,
                value=0,
                threshold=1,
                message=f"API error: {str(e)}",
                timestamp=time.time()
            )
    
    async def _check_websocket_health(self) -> HealthMetric:
        """Check WebSocket connections"""
        try:
            # This would depend on your WebSocket manager implementation
            connected_symbols = getattr(self.strategy_manager, 'active_strategies', {}).keys()
            total_symbols = len(connected_symbols)
            
            if total_symbols == 0:
                status = HealthStatus.HEALTHY
                message = "No active WebSocket connections"
            else:
                # Simplified check - in real implementation, check actual WS health
                status = HealthStatus.HEALTHY
                message = f"WebSocket connections active: {total_symbols}"
            
            return HealthMetric(
                name="WebSocket",
                status=status,
                value=total_symbols,
                threshold=10,  # Max expected connections
                message=message,
                timestamp=time.time()
            )
            
        except Exception as e:
            return HealthMetric(
                name="WebSocket",
                status=HealthStatus.WARNING,
                value=0,
                threshold=1,
                message=f"WebSocket check error: {str(e)}",
                timestamp=time.time()
            )
    
    async def _check_strategies_health(self) -> HealthMetric:
        """Check running strategies health"""
        try:
            active_strategies = getattr(self.strategy_manager, 'active_strategies', {})
            strategy_count = len(active_strategies)
            
            # Count healthy vs unhealthy strategies
            healthy_count = 0
            for symbol, strategy in active_strategies.items():
                strategy_status = strategy.get_status()
                if strategy_status.get("running", False) and not strategy_status.get("error"):
                    healthy_count += 1
            
            if strategy_count == 0:
                status = HealthStatus.HEALTHY
                message = "No strategies running"
            elif healthy_count == strategy_count:
                status = HealthStatus.HEALTHY
                message = f"All {strategy_count} strategies healthy"
            elif healthy_count > 0:
                status = HealthStatus.WARNING
                message = f"{healthy_count}/{strategy_count} strategies healthy"
            else:
                status = HealthStatus.CRITICAL
                message = f"All {strategy_count} strategies have issues"
            
            return HealthMetric(
                name="Strategies",
                status=status,
                value=healthy_count,
                threshold=strategy_count,
                message=message,
                timestamp=time.time(),
                details={
                    "total_strategies": strategy_count,
                    "healthy_strategies": healthy_count,
                    "strategy_symbols": list(active_strategies.keys())
                }
            )
            
        except Exception as e:
            return HealthMetric(
                name="Strategies",
                status=HealthStatus.CRITICAL,
                value=0,
                threshold=1,
                message=f"Strategy health check error: {str(e)}",
                timestamp=time.time()
            )
    
    async def _process_health_alerts(self, health_checks: Dict[str, HealthMetric]):
        """Process health alerts and notifications"""
        critical_alerts = []
        warning_alerts = []
        
        for name, metric in health_checks.items():
            alert_key = f"{name}_{metric.status.value}"
            
            if metric.status == HealthStatus.CRITICAL:
                critical_alerts.append(f"🚨 CRITICAL: {metric.message}")
                if alert_key not in self.alerts_sent:
                    await self._send_alert(f"CRITICAL: {name}", metric.message)
                    self.alerts_sent.add(alert_key)
            elif metric.status == HealthStatus.WARNING:
                warning_alerts.append(f"⚠️ WARNING: {metric.message}")
                if alert_key not in self.alerts_sent:
                    await self._send_alert(f"WARNING: {name}", metric.message)
                    self.alerts_sent.add(alert_key)
            else:
                # Clear alert if system is healthy again
                if alert_key in self.alerts_sent:
                    self.alerts_sent.remove(alert_key)
        
        # Log alerts
        if critical_alerts:
            self.logger.error("Critical health issues: " + "; ".join(critical_alerts))
        if warning_alerts:
            self.logger.warning("Health warnings: " + "; ".join(warning_alerts))
    
    async def _send_alert(self, title: str, message: str):
        """Send alert notification (implement your preferred notification method)"""
        self.logger.error(f"HEALTH ALERT - {title}: {message}")
        
        # Here you could implement:
        # - Email notifications
        # - Slack/Discord webhooks  
        # - SMS alerts
        # - Database logging for dashboard
        
    def get_health_summary(self) -> Dict[str, Any]:
        """Get overall health summary"""
        if not self.metrics_history:
            return {"status": "unknown", "message": "No health data available"}
        
        # Get latest metrics for each component
        latest_metrics = {}
        for component, history in self.metrics_history.items():
            if history:
                latest_metrics[component] = history[-1]
        
        # Determine overall status
        has_critical = any(m.status == HealthStatus.CRITICAL for m in latest_metrics.values())
        has_warning = any(m.status == HealthStatus.WARNING for m in latest_metrics.values())
        
        if has_critical:
            overall_status = "critical"
            status_message = "System has critical issues requiring immediate attention"
        elif has_warning:
            overall_status = "warning"
            status_message = "System has warnings that should be addressed"
        else:
            overall_status = "healthy"
            status_message = "All systems operating normally"
        
        return {
            "status": overall_status,
            "message": status_message,
            "timestamp": time.time(),
            "component_count": len(latest_metrics),
            "components": {name: asdict(metric) for name, metric in latest_metrics.items()}
        }
