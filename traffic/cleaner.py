"""
清洗验证层：实现数据清洗管道和传感器熔断器。

解决场景二：脏数据导致的统计偏差
- 实现动态阈值过滤（超过历史均值3个标准差视为异常）
- SensorCircuitBreaker 在连续错误后自动屏蔽传感器

关键组件：
- DataCleaningPipeline: 清洗管道，串联多个处理步骤
- SensorCircuitBreaker: 熔断器，保护系统免受故障传感器影响
"""
import logging
import threading
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Callable, Any

from models import (
    TrafficRecord,
    CleaningResult,
    AnomalyType,
    AnomalyLevel,
    Alert,
    SensorStatus,
)

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """
    熔断器状态枚举。
    
    状态转换图：
    ┌─────────┐    错误率>20%    ┌─────────┐
    │ CLOSED  │ ───────────────► │  OPEN   │
    │ (正常)  │                  │ (熔断)  │
    └─────────┘                  └────┬────┘
         ▲                            │
         │                       30秒后
         │                      自动进入半开
         │                            │
         │                            ▼
         │      探测成功        ┌─────────────┐
         └──────────────────────│  HALF_OPEN  │
                                │   (试探)    │
                                └─────────────┘
    """
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class SensorCircuitBreaker:
    """
    传感器熔断器。
    
    实现熔断器模式，当某个传感器的数据质量持续异常时，
    自动切断该传感器的数据流，防止脏数据污染整体统计。
    
    工作原理：
    1. CLOSED 状态：正常处理所有数据，记录错误计数
    2. OPEN 状态：拒绝所有数据，记录熔断日志
    3. HALF_OPEN 状态：允许少量数据通过，探测传感器是否恢复
    
    配置参数：
    - FAILURE_THRESHOLD: 连续错误阈值 (默认: 5)
    - ERROR_RATE_THRESHOLD: 错误率阈值 (默认: 0.20)
    - RECOVERY_TIMEOUT: 恢复试探时间 (默认: 30秒)
    - SAMPLE_WINDOW: 统计窗口大小 (默认: 100条)
    """
    
    FAILURE_THRESHOLD = 5
    ERROR_RATE_THRESHOLD = 0.20
    RECOVERY_TIMEOUT = 30
    SAMPLE_WINDOW = 100
    
    def __init__(self, sensor_id: str):
        """
        初始化熔断器。
        
        Args:
            sensor_id: 传感器ID
        """
        self.sensor_id = sensor_id
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._total_count = 0
        self._error_count = 0
        self._last_failure_time: Optional[float] = None
        self._last_state_change: float = time.time()
        self._lock = threading.Lock()
        
        self._history_counts: list[int] = []
        self._history_speeds: list[float] = []
    
    @property
    def state(self) -> CircuitState:
        """
        获取当前状态。
        """
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.time() - self._last_state_change >= self.RECOVERY_TIMEOUT:
                    self._transition_to(CircuitState.HALF_OPEN)
            return self._state
    
    @property
    def is_open(self) -> bool:
        """
        检查熔断器是否打开。
        """
        return self.state == CircuitState.OPEN
    
    @property
    def error_rate(self) -> float:
        """
        计算错误率。
        """
        with self._lock:
            if self._total_count == 0:
                return 0.0
            return self._error_count / self._total_count
    
    def record_success(self, record: TrafficRecord) -> None:
        """
        记录成功处理。
        
        在 HALF_OPEN 状态下，成功处理会触发状态恢复。
        """
        with self._lock:
            self._consecutive_failures = 0
            self._total_count += 1
            
            self._history_counts.append(record.vehicle_count)
            self._history_speeds.append(record.speed)
            
            if len(self._history_counts) > self.SAMPLE_WINDOW:
                self._history_counts.pop(0)
                self._history_speeds.pop(0)
            
            if self._state == CircuitState.HALF_OPEN:
                logger.info(f"传感器 [{self.sensor_id}] 探测成功，恢复正常")
                self._transition_to(CircuitState.CLOSED)
    
    def record_failure(self, error: str) -> Optional[Alert]:
        """
        记录处理失败。
        
        Returns:
            Optional[Alert]: 如果触发熔断，返回报警对象
        """
        alert = None
        with self._lock:
            self._consecutive_failures += 1
            self._total_count += 1
            self._error_count += 1
            self._last_failure_time = time.time()
            
            if self._should_trip():
                self._transition_to(CircuitState.OPEN)
                alert = Alert(
                    timestamp=datetime.now(),
                    sensor_id=self.sensor_id,
                    anomaly_level=AnomalyLevel.LEVEL_1,
                    anomaly_type=AnomalyType.SENSOR_MALFUNCTION,
                    message=f"传感器 [{self.sensor_id}] 已熔断",
                    current_value=self.error_rate,
                    threshold_value=self.ERROR_RATE_THRESHOLD,
                    suggestion="请检查传感器硬件或网络连接",
                )
                logger.warning(
                    f"传感器 [{self.sensor_id}] 已熔断，"
                    f"错误率: {self.error_rate:.2%}, "
                    f"连续错误: {self._consecutive_failures}"
                )
        
        return alert
    
    def _should_trip(self) -> bool:
        """
        判断是否应该触发熔断。
        """
        if self._consecutive_failures >= self.FAILURE_THRESHOLD:
            return True
        
        if self._total_count >= 10 and self.error_rate > self.ERROR_RATE_THRESHOLD:
            return True
        
        return False
    
    def _transition_to(self, new_state: CircuitState) -> None:
        """
        状态转换。
        """
        old_state = self._state
        self._state = new_state
        self._last_state_change = time.time()
        logger.info(
            f"传感器 [{self.sensor_id}] 状态变更: "
            f"{old_state.value} -> {new_state.value}"
        )
    
    def allow_request(self) -> bool:
        """
        检查是否允许处理请求。
        
        Returns:
            bool: True 允许处理，False 拒绝处理
        """
        current_state = self.state
        
        if current_state == CircuitState.CLOSED:
            return True
        
        if current_state == CircuitState.OPEN:
            return False
        
        if current_state == CircuitState.HALF_OPEN:
            return True
        
        return False
    
    def get_statistics(self) -> dict[str, Any]:
        """
        获取统计信息。
        """
        with self._lock:
            return {
                "sensor_id": self.sensor_id,
                "state": self._state.value,
                "total_count": self._total_count,
                "error_count": self._error_count,
                "error_rate": self.error_rate,
                "consecutive_failures": self._consecutive_failures,
                "is_circuit_open": self.is_open,
            }
    
    def get_dynamic_threshold(self) -> tuple[float, float]:
        """
        计算动态阈值。
        
        基于历史数据计算均值和标准差，
        用于检测异常值（超过均值±3σ视为异常）。
        
        解决场景二：某个传感器故障一直上报"9999"辆车
        
        Returns:
            tuple[float, float]: (均值, 标准差)
        """
        if len(self._history_counts) < 10:
            return (100.0, 50.0)
        
        counts = self._history_counts
        mean = sum(counts) / len(counts)
        variance = sum((x - mean) ** 2 for x in counts) / len(counts)
        std = variance ** 0.5
        
        return (mean, std)


class CleaningStep(ABC):
    """
    清洗步骤抽象基类。
    """
    
    @abstractmethod
    def process(self, result: CleaningResult) -> CleaningResult:
        """
        处理数据。
        
        Args:
            result: 输入的清洗结果
            
        Returns:
            CleaningResult: 处理后的结果
        """
        pass


class RemoveDuplicates(CleaningStep):
    """
    去重步骤。
    
    基于传感器ID和时间戳去除重复记录。
    """
    
    def __init__(self, window_seconds: int = 60):
        """
        初始化去重器。
        
        Args:
            window_seconds: 去重时间窗口（秒）
        """
        self.window_seconds = window_seconds
        self._seen: dict[str, datetime] = {}
        self._lock = threading.Lock()
    
    def process(self, result: CleaningResult) -> CleaningResult:
        """
        检查并去除重复记录。
        """
        if not result.is_valid or result.record is None:
            return result
        
        record = result.record
        key = f"{record.sensor_id}_{record.timestamp.strftime('%Y%m%d%H%M%S')}"
        
        with self._lock:
            if key in self._seen:
                last_seen = self._seen[key]
                if (record.timestamp - last_seen).total_seconds() < self.window_seconds:
                    return CleaningResult(
                        record=None,
                        is_valid=False,
                        anomaly_type=AnomalyType.DUPLICATE_RECORD,
                        error_message=f"重复记录: {key}",
                        original_data=result.original_data,
                    )
            
            self._seen[key] = record.timestamp
            
            self._cleanup_old_entries(record.timestamp)
        
        return result
    
    def _cleanup_old_entries(self, current_time: datetime) -> None:
        """
        清理过期的缓存条目。
        """
        cutoff = current_time - timedelta(seconds=self.window_seconds * 10)
        expired_keys = [
            k for k, v in self._seen.items()
            if v < cutoff
        ]
        for k in expired_keys:
            del self._seen[k]


class FixTimestamps(CleaningStep):
    """
    时间戳修正步骤。
    
    处理时间戳跳跃和异常时间。
    """
    
    def __init__(self, max_future_seconds: int = 300):
        """
        初始化时间戳修正器。
        
        Args:
            max_future_seconds: 允许的最大未来时间偏移（秒）
        """
        self.max_future_seconds = max_future_seconds
    
    def process(self, result: CleaningResult) -> CleaningResult:
        """
        修正时间戳异常。
        """
        if not result.is_valid or result.record is None:
            return result
        
        record = result.record
        now = datetime.now()
        
        if record.timestamp > now:
            future_seconds = (record.timestamp - now).total_seconds()
            if future_seconds > self.max_future_seconds:
                return CleaningResult(
                    record=None,
                    is_valid=False,
                    anomaly_type=AnomalyType.FUTURE_TIMESTAMP,
                    error_message=f"时间戳过于超前: {record.timestamp}",
                    original_data=result.original_data,
                )
            else:
                record.timestamp = now
                logger.debug(f"修正未来时间戳: {record.timestamp}")
        
        return result


class FilterOutliers(CleaningStep):
    """
    异常值过滤步骤。
    
    解决场景二：某个传感器故障一直上报"9999"辆车
    
    使用动态阈值（均值±3σ）过滤异常值。
    """
    
    def __init__(
        self,
        min_count: int = 0,
        max_count: int = 500,
        min_speed: float = 0,
        max_speed: float = 200,
        use_dynamic_threshold: bool = True,
    ):
        """
        初始化异常值过滤器。
        
        Args:
            min_count: 最小车流量
            max_count: 最大车流量
            min_speed: 最小速度
            max_speed: 最大速度
            use_dynamic_threshold: 是否使用动态阈值
        """
        self.min_count = min_count
        self.max_count = max_count
        self.min_speed = min_speed
        self.max_speed = max_speed
        self.use_dynamic_threshold = use_dynamic_threshold
        
        self._circuit_breakers: dict[str, SensorCircuitBreaker] = {}
        self._lock = threading.Lock()
    
    def get_circuit_breaker(self, sensor_id: str) -> SensorCircuitBreaker:
        """
        获取或创建传感器的熔断器。
        """
        with self._lock:
            if sensor_id not in self._circuit_breakers:
                self._circuit_breakers[sensor_id] = SensorCircuitBreaker(sensor_id)
            return self._circuit_breakers[sensor_id]
    
    def process(self, result: CleaningResult) -> CleaningResult:
        """
        过滤异常值。
        """
        if not result.is_valid or result.record is None:
            return result
        
        record = result.record
        sensor_id = record.sensor_id
        
        cb = self.get_circuit_breaker(sensor_id)
        
        if not cb.allow_request():
            return CleaningResult(
                record=None,
                is_valid=False,
                anomaly_type=AnomalyType.SENSOR_MALFUNCTION,
                error_message=f"传感器 [{sensor_id}] 已熔断，跳过处理",
                original_data=result.original_data,
            )
        
        is_outlier = False
        anomaly_type = None
        error_message = None
        
        if record.vehicle_count < self.min_count:
            is_outlier = True
            anomaly_type = AnomalyType.NEGATIVE_COUNT
            error_message = f"车流量过小: {record.vehicle_count}"
        
        if self.use_dynamic_threshold:
            mean, std = cb.get_dynamic_threshold()
            upper_bound = mean + 3 * std
            if std > 0 and record.vehicle_count > upper_bound:
                is_outlier = True
                anomaly_type = AnomalyType.OUTLIER_COUNT
                error_message = (
                    f"车流量异常（超过动态阈值）: {record.vehicle_count}, "
                    f"阈值: {upper_bound:.2f} (均值: {mean:.2f}, 标准差: {std:.2f})"
                )
        else:
            if record.vehicle_count > self.max_count:
                is_outlier = True
                anomaly_type = AnomalyType.OUTLIER_COUNT
                error_message = f"车流量过大: {record.vehicle_count}"
        
        if record.speed < self.min_speed:
            is_outlier = True
            anomaly_type = AnomalyType.NEGATIVE_SPEED
            error_message = f"速度过小: {record.speed}"
        
        if record.speed > self.max_speed:
            is_outlier = True
            anomaly_type = AnomalyType.OUTLIER_SPEED
            error_message = f"速度过大: {record.speed}"
        
        if is_outlier:
            cb.record_failure(error_message or "异常值")
            return CleaningResult(
                record=None,
                is_valid=False,
                anomaly_type=anomaly_type,
                error_message=error_message,
                original_data=result.original_data,
            )
        else:
            cb.record_success(record)
            return result
    
    def get_all_circuit_breaker_stats(self) -> dict[str, dict[str, Any]]:
        """
        获取所有熔断器的统计信息。
        """
        with self._lock:
            return {
                sensor_id: cb.get_statistics()
                for sensor_id, cb in self._circuit_breakers.items()
            }


class DataCleaningPipeline:
    """
    数据清洗管道。
    
    串联多个清洗步骤，按顺序处理数据。
    
    使用示例：
    ┌─────────────────────────────────────────────────────────────────┐
    │  pipeline = DataCleaningPipeline()                             │
    │  pipeline.add_step(RemoveDuplicates())                         │
    │  pipeline.add_step(FixTimestamps())                            │
    │  pipeline.add_step(FilterOutliers())                           │
    │                                                                 │
    │  for result in data_stream:                                    │
    │      cleaned = pipeline.process(result)                        │
    │      if cleaned.is_valid:                                       │
    │          process(cleaned.record)                               │
    └─────────────────────────────────────────────────────────────────┘
    """
    
    def __init__(self):
        """
        初始化清洗管道。
        """
        self._steps: list[CleaningStep] = []
        self._stats = {
            "total_processed": 0,
            "total_valid": 0,
            "total_invalid": 0,
            "errors_by_type": defaultdict(int),
        }
    
    def add_step(self, step: CleaningStep) -> "DataCleaningPipeline":
        """
        添加清洗步骤。
        
        Args:
            step: 清洗步骤实例
            
        Returns:
            self: 支持链式调用
        """
        self._steps.append(step)
        return self
    
    def process(self, result: CleaningResult) -> CleaningResult:
        """
        执行清洗管道。
        
        按顺序执行所有清洗步骤，任一步骤失败则停止。
        
        Args:
            result: 输入的清洗结果
            
        Returns:
            CleaningResult: 最终的清洗结果
        """
        self._stats["total_processed"] += 1
        
        current_result = result
        
        for step in self._steps:
            try:
                current_result = step.process(current_result)
            except Exception as e:
                logger.error(f"清洗步骤执行失败: {e}")
                current_result = CleaningResult(
                    record=None,
                    is_valid=False,
                    error_message=f"清洗步骤异常: {e}",
                    original_data=current_result.original_data,
                )
            
            if not current_result.is_valid:
                break
        
        if current_result.is_valid:
            self._stats["total_valid"] += 1
        else:
            self._stats["total_invalid"] += 1
            if current_result.anomaly_type:
                self._stats["errors_by_type"][current_result.anomaly_type.value] += 1
        
        return current_result
    
    def get_stats(self) -> dict[str, Any]:
        """
        获取清洗统计信息。
        """
        return {
            "total_processed": self._stats["total_processed"],
            "total_valid": self._stats["total_valid"],
            "total_invalid": self._stats["total_invalid"],
            "valid_rate": (
                self._stats["total_valid"] / self._stats["total_processed"]
                if self._stats["total_processed"] > 0 else 0
            ),
            "errors_by_type": dict(self._stats["errors_by_type"]),
        }
    
    def get_filter_outliers(self) -> Optional[FilterOutliers]:
        """
        获取 FilterOutliers 步骤（用于访问熔断器统计）。
        """
        for step in self._steps:
            if isinstance(step, FilterOutliers):
                return step
        return None
