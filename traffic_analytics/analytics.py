"""
核心计算层模块 (analytics.py)

实现滑动窗口聚合和异常检测算法，支持实时统计和趋势分析。

核心组件:
1. SlidingWindow: 基于 deque 的滑动窗口实现
2. TrafficAggregator: 流量聚合器，支持多维度分组
3. AnomalyDetector: 异常检测器，检测流量突增等异常

内存优化关键:
- 使用 collections.deque 替代 list，实现 O(1) 的队列操作
- 固定窗口大小，空间复杂度从 O(N) 降为 O(WindowSize)
- 惰性计算，避免重复计算
"""

import logging
import statistics
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any, Iterator, Tuple, Deque
from dataclasses import dataclass, field
from enum import Enum

from models import (
    TrafficRecord, AggregationResult, Alert, 
    VehicleType, DataQuality
)

logger = logging.getLogger(__name__)


@dataclass
class WindowEntry:
    """滑动窗口条目"""
    timestamp: datetime
    value: int                    # 车辆数
    speed: Optional[float]        # 速度
    record: TrafficRecord         # 原始记录


class SlidingWindow:
    """
    滑动窗口实现
    
    基于 collections.deque 的高效滑动窗口，支持:
    - 固定时间窗口 (如过去15分钟)
    - 固定数量窗口 (如最近100条记录)
    - 自动淘汰过期数据
    
    内存优化说明:
    - 使用 deque 替代 list，popleft() 操作从 O(N) 降为 O(1)
    - 固定最大长度，内存占用恒定 O(WindowSize)
    - 对比存储全量数据: 处理1天数据，内存从 O(86400) 降为 O(100)
    """
    
    def __init__(
        self,
        window_minutes: Optional[int] = None,
        max_size: Optional[int] = None
    ):
        """
        初始化滑动窗口
        
        Args:
            window_minutes: 时间窗口大小(分钟)，None 则只使用 max_size
            max_size: 最大记录数，None 则只使用 window_minutes
        """
        if window_minutes is None and max_size is None:
            raise ValueError("必须指定 window_minutes 或 max_size")
        
        self.window_minutes = window_minutes
        self.max_size = max_size
        
        # 使用 deque 存储窗口数据
        # 当指定 max_size 时，deque 会自动淘汰最旧的数据
        self._data: Deque[WindowEntry] = deque(maxlen=max_size)
        
        # 缓存统计值，避免重复计算
        self._cache_valid = False
        self._cached_stats: Dict[str, Any] = {}
    
    def add(self, record: TrafficRecord) -> None:
        """
        添加记录到窗口
        
        Args:
            record: 交通记录
        """
        entry = WindowEntry(
            timestamp=record.timestamp,
            value=record.vehicle_count,
            speed=record.speed,
            record=record
        )
        
        self._data.append(entry)
        self._cache_valid = False
        
        # 清理过期数据 (基于时间窗口)
        if self.window_minutes:
            cutoff = datetime.now() - timedelta(minutes=self.window_minutes)
            while self._data and self._data[0].timestamp < cutoff:
                self._data.popleft()
            self._cache_valid = False
    
    def _ensure_stats(self) -> None:
        """确保统计值已计算"""
        if self._cache_valid:
            return
        
        if not self._data:
            self._cached_stats = {
                "count": 0,
                "total": 0,
                "avg": 0.0,
                "max": 0,
                "min": 0,
                "avg_speed": None
            }
        else:
            values = [e.value for e in self._data]
            speeds = [e.speed for e in self._data if e.speed is not None]
            
            self._cached_stats = {
                "count": len(values),
                "total": sum(values),
                "avg": statistics.mean(values) if values else 0.0,
                "max": max(values) if values else 0,
                "min": min(values) if values else 0,
                "avg_speed": statistics.mean(speeds) if speeds else None
            }
        
        self._cache_valid = True
    
    @property
    def count(self) -> int:
        """窗口内记录数"""
        self._ensure_stats()
        return self._cached_stats["count"]
    
    @property
    def total(self) -> int:
        """窗口内车辆总数"""
        self._ensure_stats()
        return self._cached_stats["total"]
    
    @property
    def average(self) -> float:
        """窗口内平均车辆数"""
        self._ensure_stats()
        return self._cached_stats["avg"]
    
    @property
    def max_value(self) -> int:
        """窗口内最大值"""
        self._ensure_stats()
        return self._cached_stats["max"]
    
    @property
    def min_value(self) -> int:
        """窗口内最小值"""
        self._ensure_stats()
        return self._cached_stats["min"]
    
    @property
    def average_speed(self) -> Optional[float]:
        """窗口内平均速度"""
        self._ensure_stats()
        return self._cached_stats["avg_speed"]
    
    def get_values(self) -> List[int]:
        """获取窗口内所有值"""
        return [e.value for e in self._data]
    
    def is_empty(self) -> bool:
        """检查窗口是否为空"""
        return len(self._data) == 0
    
    def clear(self) -> None:
        """清空窗口"""
        self._data.clear()
        self._cache_valid = False
    
    def __len__(self) -> int:
        """窗口大小"""
        return len(self._data)


class TrafficAggregator:
    """
    流量聚合器
    
    支持多维度聚合:
    - 按时间窗口 (15分钟、1小时等)
    - 按传感器
    - 按区域
    - 按车辆类型
    
    实现滑动窗口实时统计，内存高效。
    """
    
    def __init__(
        self,
        window_minutes: int = 15,
        windows_to_keep: int = 4  # 保留4个窗口用于计算增长率
    ):
        """
        初始化聚合器
        
        Args:
            window_minutes: 聚合窗口大小(分钟)
            windows_to_keep: 保留的历史窗口数
        """
        self.window_minutes = window_minutes
        self.windows_to_keep = windows_to_keep
        
        # 多维度滑动窗口
        # 结构: {dimension_key: {group_key: SlidingWindow}}
        self._windows: Dict[str, Dict[str, SlidingWindow]] = defaultdict(dict)
        
        # 历史聚合结果，用于计算增长率
        self._history: Dict[str, Deque[AggregationResult]] = defaultdict(
            lambda: deque(maxlen=windows_to_keep)
        )
        
        # 统计信息
        self._total_processed = 0
    
    def _get_window_key(self, record: TrafficRecord, dimension: str) -> str:
        """
        获取窗口键
        
        Args:
            record: 交通记录
            dimension: 维度 (sensor/region/vehicle_type/all)
        
        Returns:
            窗口键
        """
        if dimension == "sensor":
            return record.sensor_id
        elif dimension == "region":
            return record.region or "unknown"
        elif dimension == "vehicle_type":
            return record.vehicle_type.value
        elif dimension == "all":
            return "global"
        else:
            return "unknown"
    
    def _get_or_create_window(self, dimension: str, key: str) -> SlidingWindow:
        """获取或创建滑动窗口"""
        if key not in self._windows[dimension]:
            self._windows[dimension][key] = SlidingWindow(
                window_minutes=self.window_minutes
            )
        return self._windows[dimension][key]
    
    def add(self, record: TrafficRecord) -> None:
        """
        添加记录到聚合器
        
        Args:
            record: 交通记录
        """
        self._total_processed += 1
        
        # 更新各维度窗口
        dimensions = ["sensor", "region", "vehicle_type", "all"]
        for dim in dimensions:
            key = self._get_window_key(record, dim)
            window = self._get_or_create_window(dim, key)
            window.add(record)
    
    def aggregate(
        self,
        dimension: str = "sensor",
        end_time: Optional[datetime] = None
    ) -> List[AggregationResult]:
        """
        执行聚合计算
        
        Args:
            dimension: 聚合维度
            end_time: 聚合结束时间，None 则使用当前时间
        
        Returns:
            聚合结果列表
        """
        end_time = end_time or datetime.now()
        start_time = end_time - timedelta(minutes=self.window_minutes)
        
        results = []
        
        for key, window in self._windows[dimension].items():
            if window.is_empty():
                continue
            
            # 计算增长率
            growth_rate = self._calculate_growth_rate(dimension, key, window.total)
            
            result = AggregationResult(
                window_start=start_time,
                window_end=end_time,
                window_minutes=self.window_minutes,
                sensor_id=key if dimension == "sensor" else None,
                region=key if dimension == "region" else None,
                vehicle_type=VehicleType(key) if dimension == "vehicle_type" else None,
                total_count=window.total,
                record_count=window.count,
                avg_speed=window.average_speed,
                max_count=window.max_value,
                min_count=window.min_value,
                growth_rate=growth_rate
            )
            
            results.append(result)
            
            # 保存到历史
            self._history[f"{dimension}:{key}"].append(result)
        
        return results
    
    def _calculate_growth_rate(
        self,
        dimension: str,
        key: str,
        current_value: int
    ) -> Optional[float]:
        """
        计算环比增长率
        
        Args:
            dimension: 维度
            key: 键
            current_value: 当前值
        
        Returns:
            增长率，无法计算返回 None
        """
        history_key = f"{dimension}:{key}"
        history = self._history[history_key]
        
        if not history:
            return None
        
        previous = history[-1].total_count
        
        # 边界处理: 防止除以零
        if previous == 0:
            if current_value == 0:
                return 0.0  # 0 -> 0，增长率为 0
            else:
                return float('inf')  # 0 -> N，视为无限增长
        
        growth_rate = (current_value - previous) / previous
        return round(growth_rate, 4)
    
    def get_window_stats(self, dimension: str = "sensor") -> Dict[str, Any]:
        """获取窗口统计信息"""
        return {
            "dimension": dimension,
            "groups": len(self._windows[dimension]),
            "total_processed": self._total_processed,
            "window_minutes": self.window_minutes
        }
    
    def clear(self) -> None:
        """清空所有数据"""
        self._windows.clear()
        self._history.clear()
        self._total_processed = 0


class AnomalyDetector:
    """
    异常检测器
    
    实现多级异常检测:
    - Level 1: 单条数据异常 (已在 cleaner 中处理)
    - Level 2: 趋势异常 (流量突增)
    - Level 3: 模式异常 (拥堵检测)
    """
    
    def __init__(
        self,
        surge_threshold: float = 0.5,        # 突增阈值 50%
        surge_window_minutes: int = 10,      # 突增检测窗口
        congestion_threshold: int = 80,      # 拥堵阈值 (车辆数)
        congestion_speed_threshold: float = 20.0,  # 拥堵速度阈值
        min_records_for_detection: int = 5   # 检测所需最小记录数
    ):
        """
        初始化异常检测器
        
        Args:
            surge_threshold: 流量突增阈值 (增长率)
            surge_window_minutes: 突增检测时间窗口
            congestion_threshold: 拥堵车辆数阈值
            congestion_speed_threshold: 拥堵速度阈值
            min_records_for_detection: 检测所需最小记录数
        """
        self.surge_threshold = surge_threshold
        self.surge_window_minutes = surge_window_minutes
        self.congestion_threshold = congestion_threshold
        self.congestion_speed_threshold = congestion_speed_threshold
        self.min_records_for_detection = min_records_for_detection
        
        # 历史数据用于趋势检测
        self._history: Dict[str, Deque[Tuple[datetime, int]]] = defaultdict(
            lambda: deque(maxlen=100)
        )
        
        # 已报警记录，防止重复报警
        self._alerted: Dict[str, datetime] = {}
        self._alert_cooldown_minutes = 30  # 报警冷却时间
    
    def _should_alert(self, alert_key: str) -> bool:
        """检查是否应该报警 (防止重复报警)"""
        now = datetime.now()
        if alert_key in self._alerted:
            last_alert = self._alerted[alert_key]
            if now - last_alert < timedelta(minutes=self._alert_cooldown_minutes):
                return False
        self._alerted[alert_key] = now
        return True
    
    def detect_surge(
        self,
        record: TrafficRecord,
        context_window: Optional[SlidingWindow] = None
    ) -> Optional[Alert]:
        """
        检测流量突增
        
        场景: 某路段车流在10分钟内激增超过50%
        
        Args:
            record: 当前记录
            context_window: 上下文窗口
        
        Returns:
            报警对象，无异常返回 None
        """
        sensor_id = record.sensor_id
        now = record.timestamp
        
        # 获取历史数据
        history = self._history[sensor_id]
        
        # 计算窗口内的基准值
        cutoff = now - timedelta(minutes=self.surge_window_minutes)
        recent_values = [v for t, v in history if t > cutoff]
        
        if len(recent_values) < self.min_records_for_detection:
            # 数据不足，只记录不检测
            history.append((now, record.vehicle_count))
            return None
        
        # 计算基准值 (使用窗口平均值)
        baseline = statistics.mean(recent_values)
        
        # 边界处理: 防止除以零
        if baseline == 0:
            if record.vehicle_count > 0:
                growth_rate = float('inf')
            else:
                growth_rate = 0.0
        else:
            growth_rate = (record.vehicle_count - baseline) / baseline
        
        # 记录当前值
        history.append((now, record.vehicle_count))
        
        # 检查是否超过阈值
        if growth_rate > self.surge_threshold:
            alert_key = f"surge:{sensor_id}"
            if self._should_alert(alert_key):
                return Alert(
                    alert_type="TRAFFIC_SURGE",
                    severity="high" if growth_rate > 1.0 else "medium",
                    sensor_id=sensor_id,
                    region=record.region,
                    timestamp=now,
                    message=f"流量突增检测: {sensor_id} 车流量在 {self.surge_window_minutes} 分钟内增长 {growth_rate:.1%}",
                    details={
                        "baseline": round(baseline, 2),
                        "current": record.vehicle_count,
                        "growth_rate": round(growth_rate, 4),
                        "window_minutes": self.surge_window_minutes
                    },
                    suggested_action="建议派遣交警疏导交通，并检查是否有事故或施工"
                )
        
        return None
    
    def detect_congestion(
        self,
        record: TrafficRecord,
        context_window: Optional[SlidingWindow] = None
    ) -> Optional[Alert]:
        """
        检测拥堵
        
        场景: 车辆数超过阈值且速度低于阈值
        
        Args:
            record: 当前记录
            context_window: 上下文窗口
        
        Returns:
            报警对象，无异常返回 None
        """
        # 拥堵判定: 高流量 + 低速
        if (record.vehicle_count >= self.congestion_threshold and
            record.speed is not None and
            record.speed <= self.congestion_speed_threshold):
            
            alert_key = f"congestion:{record.sensor_id}"
            if self._should_alert(alert_key):
                return Alert(
                    alert_type="CONGESTION",
                    severity="medium",
                    sensor_id=record.sensor_id,
                    region=record.region,
                    timestamp=record.timestamp,
                    message=f"拥堵检测: {record.sensor_id} 流量 {record.vehicle_count}，平均速度 {record.speed:.1f} km/h",
                    details={
                        "vehicle_count": record.vehicle_count,
                        "speed": record.speed,
                        "threshold_count": self.congestion_threshold,
                        "threshold_speed": self.congestion_speed_threshold
                    },
                    suggested_action="建议开启拥堵预警，引导车辆绕行"
                )
        
        return None
    
    def detect_sensor_failure(
        self,
        record: TrafficRecord
    ) -> Optional[Alert]:
        """
        检测传感器故障
        
        场景: 长时间无变化或异常值
        """
        sensor_id = record.sensor_id
        history = self._history[sensor_id]
        
        if len(history) >= 10:
            # 检查最近10条记录是否完全相同
            recent_values = [v for t, v in list(history)[-10:]]
            if len(set(recent_values)) == 1:
                alert_key = f"sensor_stuck:{sensor_id}"
                if self._should_alert(alert_key):
                    return Alert(
                        alert_type="SENSOR_STUCK",
                        severity="low",
                        sensor_id=sensor_id,
                        region=record.region,
                        timestamp=record.timestamp,
                        message=f"传感器可能故障: {sensor_id} 连续10次上报相同值 {recent_values[0]}",
                        details={"stuck_value": recent_values[0]},
                        suggested_action="建议检查传感器状态，可能需要维护"
                    )
        
        return None
    
    def detect(
        self,
        record: TrafficRecord,
        context_window: Optional[SlidingWindow] = None
    ) -> List[Alert]:
        """
        执行全量异常检测
        
        Args:
            record: 当前记录
            context_window: 上下文窗口
        
        Returns:
            报警列表
        """
        alerts = []
        
        # 检测流量突增
        surge_alert = self.detect_surge(record, context_window)
        if surge_alert:
            alerts.append(surge_alert)
        
        # 检测拥堵
        congestion_alert = self.detect_congestion(record, context_window)
        if congestion_alert:
            alerts.append(congestion_alert)
        
        # 检测传感器故障
        failure_alert = self.detect_sensor_failure(record)
        if failure_alert:
            alerts.append(failure_alert)
        
        return alerts


class AnalyticsEngine:
    """
    分析引擎
    
    整合聚合器和异常检测器，提供统一的分析接口。
    """
    
    def __init__(
        self,
        window_minutes: int = 15,
        surge_threshold: float = 0.5,
        **kwargs
    ):
        """
        初始化分析引擎
        
        Args:
            window_minutes: 聚合窗口大小
            surge_threshold: 突增检测阈值
        """
        self.aggregator = TrafficAggregator(window_minutes=window_minutes)
        self.detector = AnomalyDetector(
            surge_threshold=surge_threshold,           **kwargs
        )
        
        # 报警收集
        self._alerts: List[Alert] = []
    
    def process(self, record: TrafficRecord) -> List[Alert]:
        """
        处理单条记录
        
        Args:
            record: 交通记录
        
        Returns:
            报警列表
        """
        # 添加到聚合器
        self.aggregator.add(record)
        
        # 异常检测
        alerts = self.detector.detect(record)
        self._alerts.extend(alerts)
        
        return alerts
    
    def aggregate(self, dimension: str = "sensor") -> List[AggregationResult]:
        """执行聚合"""
        return self.aggregator.aggregate(dimension=dimension)
    
    def get_alerts(self, clear: bool = False) -> List[Alert]:
        """获取报警列表"""
        alerts = self._alerts.copy()
        if clear:
            self._alerts.clear()
        return alerts
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "aggregator": self.aggregator.get_window_stats(),
            "total_alerts": len(self._alerts)
        }
    
    def reset(self) -> None:
        """重置引擎状态"""
        self.aggregator.clear()
        self._alerts.clear()
