"""
数据清洗与验证层模块 (cleaner.py)

实现数据清洗管道和熔断器机制，确保数据质量并防止脏数据污染整体统计。

核心组件:
1. DataCleaningPipeline: 数据清洗管道，支持多步骤处理
2. SensorCircuitBreaker: 传感器熔断器，实现动态熔断机制
3. 多个清洗步骤: 去重、时间修正、异常值过滤
"""

import logging
import statistics
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Iterator, Optional, Dict, List, Set, Callable, Any, Deque
from dataclasses import dataclass, field
from enum import Enum

from models import TrafficRecord, DataQuality, SensorStatus

logger = logging.getLogger(__name__)


class CircuitBreakerState(Enum):
    """熔断器状态"""
    CLOSED = "closed"       # 关闭状态 - 正常处理
    OPEN = "open"           # 打开状态 - 熔断，拒绝请求
    HALF_OPEN = "half_open" # 半开状态 - 试探性恢复


@dataclass
class SensorMetrics:
    """传感器指标追踪"""
    sensor_id: str
    total_records: int = 0
    error_count: int = 0
    record_history: Deque[TrafficRecord] = field(default_factory=lambda: deque(maxlen=100))
    error_history: Deque[datetime] = field(default_factory=lambda: deque(maxlen=50))
    last_error_time: Optional[datetime] = None
    tripped_at: Optional[datetime] = None
    recovered_at: Optional[datetime] = None
    
    @property
    def error_rate(self) -> float:
        """计算错误率"""
        if self.total_records == 0:
            return 0.0
        return self.error_count / self.total_records
    
    @property
    def recent_error_rate(self, window_minutes: int = 10) -> float:
        """计算最近窗口内的错误率"""
        cutoff = datetime.now() - timedelta(minutes=window_minutes)
        recent_errors = sum(1 for t in self.error_history if t > cutoff)
        recent_records = sum(1 for r in self.record_history if r.timestamp > cutoff)
        if recent_records == 0:
            return 0.0
        return recent_errors / recent_records


class SensorCircuitBreaker:
    """
    传感器熔断器
    
    实现熔断器模式，保护系统免受故障传感器的影响。
    
    状态机:
    CLOSED (关闭) -> OPEN (打开): 错误率超过阈值
    OPEN (打开) -> HALF_OPEN (半开): 经过冷却时间
    HALF_OPEN (半开) -> CLOSED (关闭): 试探成功
    HALF_OPEN (半开) -> OPEN (打开): 试探失败
    
    关键设计:
    - 每个传感器独立追踪指标
    - 支持动态阈值配置
    - 自动恢复机制
    """
    
    def __init__(
        self,
        error_threshold: float = 0.2,        # 错误率阈值 (20%)
        min_sample_size: int = 10,            # 最小样本量
        cooldown_seconds: int = 300,          # 冷却时间 (5分钟)
        half_open_max_requests: int = 5       # 半开状态最大试探请求数
    ):
        """
        初始化熔断器
        
        Args:
            error_threshold: 触发熔断的错误率阈值
            min_sample_size: 触发熔断所需的最小样本量
            cooldown_seconds: 熔断后冷却时间
            half_open_max_requests: 半开状态最大试探请求数
        """
        self.error_threshold = error_threshold
        self.min_sample_size = min_sample_size
        self.cooldown_seconds = cooldown_seconds
        self.half_open_max_requests = half_open_max_requests
        
        # 传感器状态存储
        self._sensor_states: Dict[str, CircuitBreakerState] = {}
        self._sensor_metrics: Dict[str, SensorMetrics] = {}
        self._half_open_requests: Dict[str, int] = {}
    
    def _get_metrics(self, sensor_id: str) -> SensorMetrics:
        """获取或创建传感器指标"""
        if sensor_id not in self._sensor_metrics:
            self._sensor_metrics[sensor_id] = SensorMetrics(sensor_id=sensor_id)
            self._sensor_states[sensor_id] = CircuitBreakerState.CLOSED
        return self._sensor_metrics[sensor_id]
    
    def can_process(self, sensor_id: str) -> bool:
        """
        检查是否允许处理该传感器的数据
        
        Args:
            sensor_id: 传感器ID
        
        Returns:
            是否允许处理
        """
        state = self._sensor_states.get(sensor_id, CircuitBreakerState.CLOSED)
        
        if state == CircuitBreakerState.CLOSED:
            return True
        
        if state == CircuitBreakerState.OPEN:
            # 检查是否已过冷却时间
            metrics = self._get_metrics(sensor_id)
            if metrics.tripped_at:
                elapsed = (datetime.now() - metrics.tripped_at).total_seconds()
                if elapsed >= self.cooldown_seconds:
                    # 切换到半开状态
                    self._sensor_states[sensor_id] = CircuitBreakerState.HALF_OPEN
                    self._half_open_requests[sensor_id] = 0
                    logger.info(f"传感器 {sensor_id} 进入半开状态，开始试探性恢复")
                    return True
            return False
        
        if state == CircuitBreakerState.HALF_OPEN:
            # 限制半开状态的请求数
            if self._half_open_requests.get(sensor_id, 0) < self.half_open_max_requests:
                self._half_open_requests[sensor_id] = self._half_open_requests.get(sensor_id, 0) + 1
                return True
            return False
        
        return True
    
    def record_success(self, sensor_id: str, record: TrafficRecord) -> None:
        """记录成功处理"""
        metrics = self._get_metrics(sensor_id)
        metrics.total_records += 1
        metrics.record_history.append(record)
        
        state = self._sensor_states.get(sensor_id, CircuitBreakerState.CLOSED)
        
        # 半开状态下成功次数足够，关闭熔断
        if state == CircuitBreakerState.HALF_OPEN:
            success_count = sum(
                1 for r in metrics.record_history
                if r.quality == DataQuality.VALID
            )
            if success_count >= self.half_open_max_requests // 2:
                self._sensor_states[sensor_id] = CircuitBreakerState.CLOSED
                metrics.recovered_at = datetime.now()
                self._half_open_requests[sensor_id] = 0
                logger.info(f"传感器 {sensor_id} 恢复成功，熔断器关闭")
    
    def record_error(self, sensor_id: str, reason: str) -> None:
        """记录处理错误"""
        metrics = self._get_metrics(sensor_id)
        metrics.total_records += 1
        metrics.error_count += 1
        metrics.error_history.append(datetime.now())
        metrics.last_error_time = datetime.now()
        
        state = self._sensor_states.get(sensor_id, CircuitBreakerState.CLOSED)
        
        # 检查是否需要熔断
        if state == CircuitBreakerState.CLOSED:
            if (metrics.total_records >= self.min_sample_size and 
                metrics.error_rate > self.error_threshold):
                self._trip(sensor_id, f"错误率 {metrics.error_rate:.2%} 超过阈值 {self.error_threshold:.2%}")
        
        elif state == CircuitBreakerState.HALF_OPEN:
            # 半开状态下失败，重新熔断
            self._trip(sensor_id, f"半开状态下恢复失败: {reason}")
    
    def _trip(self, sensor_id: str, reason: str) -> None:
        """触发熔断"""
        self._sensor_states[sensor_id] = CircuitBreakerState.OPEN
        metrics = self._get_metrics(sensor_id)
        metrics.tripped_at = datetime.now()
        logger.warning(f"传感器 {sensor_id} 已熔断! 原因: {reason}")
    
    def get_sensor_status(self, sensor_id: str) -> SensorStatus:
        """获取传感器状态"""
        metrics = self._get_metrics(sensor_id)
        state = self._sensor_states.get(sensor_id, CircuitBreakerState.CLOSED)
        
        return SensorStatus(
            sensor_id=sensor_id,
            is_active=state != CircuitBreakerState.OPEN,
            is_tripped=state == CircuitBreakerState.OPEN,
            total_records=metrics.total_records,
            error_count=metrics.error_count,
            error_rate=metrics.error_rate,
            last_seen=metrics.record_history[-1].timestamp if metrics.record_history else None,
            trip_reason=f"错误率超过阈值 {self.error_threshold}" if state == CircuitBreakerState.OPEN else None,
            tripped_at=metrics.tripped_at
        )
    
    def get_all_sensor_status(self) -> List[SensorStatus]:
        """获取所有传感器状态"""
        return [self.get_sensor_status(sid) for sid in self._sensor_metrics.keys()]
    
    def reset(self, sensor_id: str) -> None:
        """手动重置传感器状态"""
        if sensor_id in self._sensor_metrics:
            del self._sensor_metrics[sensor_id]
        if sensor_id in self._sensor_states:
            del self._sensor_states[sensor_id]
        if sensor_id in self._half_open_requests:
            del self._half_open_requests[sensor_id]
        logger.info(f"传感器 {sensor_id} 状态已重置")


class CleaningStep(ABC):
    """数据清洗步骤抽象基类"""
    
    @abstractmethod
    def process(self, record: TrafficRecord) -> Optional[TrafficRecord]:
        """
        处理单条记录
        
        Args:
            record: 输入记录
        
        Returns:
            处理后的记录，如果记录无效返回 None
        """
        pass
    
    @abstractmethod
    def get_name(self) -> str:
        """获取步骤名称"""
        pass


class RemoveDuplicates(CleaningStep):
    """去重步骤: 基于传感器ID和时间戳去重"""
    
    def __init__(self, window_seconds: int = 60):
        """
        初始化去重器
        
        Args:
            window_seconds: 去重窗口大小(秒)
        """
        self.window_seconds = window_seconds
        # 使用 deque 存储最近看到的记录标识，自动淘汰旧数据
        self._seen: Deque[tuple] = deque(maxlen=10000)
    
    def get_name(self) -> str:
        return "RemoveDuplicates"
    
    def process(self, record: TrafficRecord) -> Optional[TrafficRecord]:
        """处理记录，检测重复"""
        # 创建记录标识 (sensor_id, timestamp_bucket)
        key = (
            record.sensor_id,
            record.timestamp.replace(second=0, microsecond=0)
        )
        
        # 清理过期记录
        cutoff = datetime.now() - timedelta(seconds=self.window_seconds)
        while self._seen and self._seen[0][1] < cutoff:
            self._seen.popleft()
        
        # 检查是否重复
        for seen_key, seen_time in self._seen:
            if seen_key == key:
                logger.debug(f"检测到重复记录 [sensor={record.sensor_id}, time={record.timestamp}]")
                return None  # 丢弃重复记录
        
        self._seen.append((key, datetime.now()))
        return record


class FixTimestamps(CleaningStep):
    """时间戳修正步骤"""
    
    def __init__(self, max_future_seconds: int = 300, max_past_minutes: int = 60):
        """
        初始化时间戳修正器
        
        Args:
            max_future_seconds: 允许的最大未来时间偏差(秒)
            max_past_minutes: 允许的最大过去时间偏差(分钟)
        """
        self.max_future_seconds = max_future_seconds
        self.max_past_minutes = max_past_minutes
    
    def get_name(self) -> str:
        return "FixTimestamps"
    
    def process(self, record: TrafficRecord) -> Optional[TrafficRecord]:
        """修正时间戳异常"""
        now = datetime.now()
        
        # 处理未来时间
        if record.timestamp > now:
            diff = (record.timestamp - now).total_seconds()
            if diff > self.max_future_seconds:
                # 时间戳在未来太远，标记为无效
                return record.mark_invalid(f"时间戳在未来 {diff:.0f} 秒")
            else:
                # 小幅偏差，修正为当前时间
                record.timestamp = now
                record.mark_suspicious(f"时间戳修正: 未来时间 {diff:.0f} 秒")
        
        # 处理过期时间
        past_limit = now - timedelta(minutes=self.max_past_minutes)
        if record.timestamp < past_limit:
            # 数据太旧，标记为可疑但保留
            record.mark_suspicious(f"时间戳过期: {record.timestamp}")
        
        return record


class FilterOutliers(CleaningStep):
    """
    异常值过滤步骤
    
    使用动态阈值检测异常值:
    - 基于历史数据的均值和标准差
    - 超过3个标准差视为异常
    - 支持传感器级别的独立统计
    """
    
    def __init__(
        self,
        z_score_threshold: float = 3.0,
        min_history: int = 5,
        max_count: int = 1000,
        max_speed: float = 150.0
    ):
        """
        初始化异常值过滤器
        
        Args:
            z_score_threshold: Z-score 阈值
            min_history: 计算统计所需的最小历史记录数
            max_count: 绝对最大车辆数阈值
            max_speed: 绝对最大速度阈值
        """
        self.z_score_threshold = z_score_threshold
        self.min_history = min_history
        self.max_count = max_count
        self.max_speed = max_speed
        
        # 每个传感器的历史数据
        self._history: Dict[str, Deque[int]] = defaultdict(lambda: deque(maxlen=100))
    
    def get_name(self) -> str:
        return "FilterOutliers"
    
    def process(self, record: TrafficRecord) -> Optional[TrafficRecord]:
        """过滤异常值"""
        sensor_id = record.sensor_id
        
        # 绝对阈值检查
        if record.vehicle_count > self.max_count:
            return record.mark_invalid(f"车辆数 {record.vehicle_count} 超过绝对阈值 {self.max_count}")
        
        if record.vehicle_count < 0:
            return record.mark_invalid(f"车辆数不能为负数: {record.vehicle_count}")
        
        if record.speed is not None and record.speed > self.max_speed:
            return record.mark_invalid(f"速度 {record.speed} 超过绝对阈值 {self.max_speed}")
        
        # 动态阈值检查 (基于历史统计)
        history = self._history[sensor_id]
        
        if len(history) >= self.min_history:
            mean = statistics.mean(history)
            std = statistics.stdev(history) if len(history) > 1 else 0
            
            if std > 0:
                z_score = abs(record.vehicle_count - mean) / std
                if z_score > self.z_score_threshold:
                    return record.mark_suspicious(
                        f"车辆数 {record.vehicle_count} 偏离均值 {mean:.1f} 超过 {self.z_score_threshold} 个标准差"
                    )
        
        # 记录有效数据到历史
        if record.quality == DataQuality.VALID:
            history.append(record.vehicle_count)
        
        return record


class DataCleaningPipeline:
    """
    数据清洗管道
    
    协调多个清洗步骤，实现可配置的数据清洗流程。
    集成熔断器机制，保护系统免受故障传感器影响。
    
    使用示例:
        pipeline = DataCleaningPipeline([
            RemoveDuplicates(),
            FixTimestamps(),
            FilterOutliers()
        ])
        
        for record in stream.records():
            cleaned = pipeline.process(record)
            if cleaned:
                # 处理清洗后的记录
                pass
    """
    
    def __init__(
        self,
        steps: Optional[List[CleaningStep]] = None,
        circuit_breaker: Optional[SensorCircuitBreaker] = None,
        skip_invalid: bool = True
    ):
        """
        初始化清洗管道
        
        Args:
            steps: 清洗步骤列表
            circuit_breaker: 熔断器实例
            skip_invalid: 是否跳过无效记录
        """
        self.steps = steps or []
        self.circuit_breaker = circuit_breaker or SensorCircuitBreaker()
        self.skip_invalid = skip_invalid
        
        # 统计信息
        self._processed_count = 0
        self._cleaned_count = 0
        self._invalid_count = 0
        self._tripped_count = 0
    
    def add_step(self, step: CleaningStep) -> "DataCleaningPipeline":
        """添加清洗步骤"""
        self.steps.append(step)
        return self
    
    def process(self, record: TrafficRecord) -> Optional[TrafficRecord]:
        """
        处理单条记录
        
        Args:
            record: 输入记录
        
        Returns:
            清洗后的记录，如果无效或被熔断返回 None
        """
        self._processed_count += 1
        sensor_id = record.sensor_id
        
        # 检查熔断器
        if not self.circuit_breaker.can_process(sensor_id):
            self._tripped_count += 1
            logger.debug(f"传感器 {sensor_id} 已熔断，跳过记录")
            return None
        
        # 基础验证
        if not record.is_valid():
            self.circuit_breaker.record_error(sensor_id, "基础验证失败")
            self._invalid_count += 1
            if self.skip_invalid:
                return None
            return record
        
        # 执行清洗步骤
        current_record = record
        for step in self.steps:
            try:
                current_record = step.process(current_record)
                if current_record is None:
                    # 记录被步骤丢弃 (如重复)
                    return None
            except Exception as e:
                logger.error(f"清洗步骤 {step.get_name()} 失败: {e}")
                self.circuit_breaker.record_error(sensor_id, f"清洗失败: {e}")
                return None
        
        # 最终验证
        if not current_record.is_valid():
            self.circuit_breaker.record_error(sensor_id, "清洗后验证失败")
            self._invalid_count += 1
            if self.skip_invalid:
                return None
        else:
            self.circuit_breaker.record_success(sensor_id, current_record)
            self._cleaned_count += 1
        
        return current_record
    
    def process_stream(
        self,
        records: Iterator[TrafficRecord]
    ) -> Iterator[TrafficRecord]:
        """
        处理记录流
        
        Args:
            records: 输入记录迭代器
        
        Yields:
            清洗后的有效记录
        """
        for record in records:
            cleaned = self.process(record)
            if cleaned:
                yield cleaned
    
    @property
    def stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "processed_count": self._processed_count,
            "cleaned_count": self._cleaned_count,
            "invalid_count": self._invalid_count,
            "tripped_count": self._tripped_count,
            "steps": [step.get_name() for step in self.steps],
            "circuit_breaker": {
                "total_sensors": len(self.circuit_breaker._sensor_metrics),
                "tripped_sensors": sum(
                    1 for s in self.circuit_breaker._sensor_states.values()
                    if s == CircuitBreakerState.OPEN
                )
            }
        }
    
    def reset_stats(self) -> None:
        """重置统计信息"""
        self._processed_count = 0
        self._cleaned_count = 0
        self._invalid_count = 0
        self._tripped_count = 0


def create_default_pipeline(
    error_threshold: float = 0.2,
    dedup_window: int = 60,
    z_score: float = 3.0
) -> DataCleaningPipeline:
    """
    创建默认清洗管道
    
    Args:
        error_threshold: 熔断器错误率阈值
        dedup_window: 去重窗口(秒)
        z_score: Z-score 阈值
    
    Returns:
        配置好的清洗管道
    """
    circuit_breaker = SensorCircuitBreaker(error_threshold=error_threshold)
    
    steps: List[CleaningStep] = [
        RemoveDuplicates(window_seconds=dedup_window),
        FixTimestamps(),
        FilterOutliers(z_score_threshold=z_score)
    ]
    
    return DataCleaningPipeline(
        steps=steps,
        circuit_breaker=circuit_breaker,
        skip_invalid=True
    )
