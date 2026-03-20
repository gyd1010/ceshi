"""
核心计算层：实现滑动窗口聚合和异常检测。

解决场景一：内存泄漏风险
- 使用 collections.deque 实现固定长度的滑动窗口
- 空间复杂度从 O(N) 降为 O(WindowSize)

解决场景三：时间窗口边界处理
- 处理数据稀疏时的除零错误
- 定义"无数据"状态的默认行为
"""
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Any
import threading

from models import (
    TrafficRecord,
    AggregationResult,
    Alert,
    AnomalyLevel,
    AnomalyType,
    VehicleType,
)

logger = logging.getLogger(__name__)


@dataclass
class WindowData:
    """
    窗口数据点。
    """
    timestamp: datetime
    vehicle_count: int
    speed: float
    vehicle_type: VehicleType
    region: Optional[str] = None


class SlidingWindow:
    """
    滑动窗口实现。
    
    使用 collections.deque 实现固定长度的滑动窗口，
    高效计算滚动统计量。
    
    内存优化原理：
    ┌─────────────────────────────────────────────────────────────────┐
    │  传统方式:                                                       │
    │  history = []                                                   │
    │  history.append(data)  # 无限增长                               │
    │  内存占用: O(N)，N = 总数据量                                    │
    │  问题: 处理一天数据会撑爆内存                                     │
    │                                                                 │
    │  滑动窗口方式:                                                   │
    │  window = deque(maxlen=1000)  # 固定长度                        │
    │  window.append(data)  # 自动淘汰最旧数据                         │
    │  内存占用: O(WindowSize)，与总数据量无关                          │
    │  优势: 无论处理多少数据，内存占用恒定                              │
    │                                                                 │
    │  空间复杂度对比:                                                 │
    │  - 传统方式: O(N)                                               │
    │  - 滑动窗口: O(WindowSize)                                      │
    │                                                                 │
    │  示例: 处理 1亿 条数据                                           │
    │  - 传统方式: 需要 ~10GB 内存                                     │
    │  - 滑动窗口(1000): 只需 ~100KB 内存                              │
    └─────────────────────────────────────────────────────────────────┘
    
    时间复杂度：
    - 添加数据: O(1)
    - 计算均值: O(WindowSize)，可通过增量计算优化为 O(1)
    - 计算增长率: O(1)
    """
    
    def __init__(self, window_size: int = 1000):
        """
        初始化滑动窗口。
        
        Args:
            window_size: 窗口大小（最大数据点数）
        """
        self.window_size = window_size
        self._data: deque[WindowData] = deque(maxlen=window_size)
        self._lock = threading.Lock()
        
        self._sum_count = 0
        self._sum_speed = 0.0
    
    def add(self, data: WindowData) -> None:
        """
        添加数据点。
        
        当窗口满时，自动淘汰最旧的数据。
        使用增量更新优化均值计算。
        """
        with self._lock:
            if len(self._data) == self.window_size:
                oldest = self._data[0]
                self._sum_count -= oldest.vehicle_count
                self._sum_speed -= oldest.speed
            
            self._data.append(data)
            self._sum_count += data.vehicle_count
            self._sum_speed += data.speed
    
    def get_average_count(self) -> float:
        """
        计算平均车流量。
        
        解决场景三：时间窗口边界处理
        - 处理空窗口情况
        - 返回 0.0 而非抛出异常
        """
        with self._lock:
            if len(self._data) == 0:
                return 0.0
            return self._sum_count / len(self._data)
    
    def get_average_speed(self) -> float:
        """
        计算平均速度。
        """
        with self._lock:
            if len(self._data) == 0:
                return 0.0
            return self._sum_speed / len(self._data)
    
    def get_total_count(self) -> int:
        """
        获取总车流量。
        """
        with self._lock:
            return self._sum_count
    
    def get_count(self) -> int:
        """
        获取数据点数量。
        """
        with self._lock:
            return len(self._data)
    
    def get_latest(self, n: int = 1) -> list[WindowData]:
        """
        获取最近的 n 个数据点。
        """
        with self._lock:
            return list(self._data)[-n:]
    
    def get_oldest_timestamp(self) -> Optional[datetime]:
        """
        获取最旧数据的时间戳。
        """
        with self._lock:
            if len(self._data) == 0:
                return None
            return self._data[0].timestamp
    
    def get_newest_timestamp(self) -> Optional[datetime]:
        """
        获取最新数据的时间戳。
        """
        with self._lock:
            if len(self._data) == 0:
                return None
            return self._data[-1].timestamp
    
    def clear(self) -> None:
        """
        清空窗口。
        """
        with self._lock:
            self._data.clear()
            self._sum_count = 0
            self._sum_speed = 0.0


class TimeWindowAggregator:
    """
    时间窗口聚合器。
    
    按时间窗口（如每分钟、每小时）聚合数据。
    """
    
    def __init__(self, window_minutes: int = 15):
        """
        初始化时间窗口聚合器。
        
        Args:
            window_minutes: 时间窗口大小（分钟）
        """
        self.window_minutes = window_minutes
        self._windows: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "counts": [],
                "speeds": [],
                "types": defaultdict(int),
                "start_time": None,
                "end_time": None,
            }
        )
        self._lock = threading.Lock()
    
    def _get_window_key(self, timestamp: datetime) -> str:
        """
        计算时间窗口键。
        """
        window_start = timestamp.replace(
            second=0, microsecond=0
        )
        window_start = window_start.replace(
            minute=(window_start.minute // self.window_minutes) * self.window_minutes
        )
        return window_start.strftime("%Y%m%d_%H%M")
    
    def add(self, record: TrafficRecord) -> str:
        """
        添加记录到时间窗口。
        
        Returns:
            str: 时间窗口键
        """
        window_key = self._get_window_key(record.timestamp)
        
        with self._lock:
            window = self._windows[window_key]
            window["counts"].append(record.vehicle_count)
            window["speeds"].append(record.speed)
            window["types"][record.vehicle_type.value] += 1
            
            if window["start_time"] is None:
                window["start_time"] = record.timestamp
            window["end_time"] = record.timestamp
        
        return window_key
    
    def get_aggregation(self, window_key: str) -> Optional[AggregationResult]:
        """
        获取指定窗口的聚合结果。
        """
        with self._lock:
            if window_key not in self._windows:
                return None
            
            window = self._windows[window_key]
            counts = window["counts"]
            speeds = window["speeds"]
            
            if not counts:
                return None
            
            return AggregationResult(
                sensor_id="AGGREGATED",
                window_start=window["start_time"],
                window_end=window["end_time"],
                total_count=sum(counts),
                avg_speed=sum(speeds) / len(speeds) if speeds else 0.0,
                record_count=len(counts),
                vehicle_type_breakdown=dict(window["types"]),
            )
    
    def get_all_aggregations(self) -> list[AggregationResult]:
        """
        获取所有窗口的聚合结果。
        """
        results = []
        with self._lock:
            for window_key in sorted(self._windows.keys()):
                result = self.get_aggregation(window_key)
                if result:
                    results.append(result)
        return results
    
    def clear_old_windows(self, before: datetime) -> int:
        """
        清理旧的时间窗口。
        
        Returns:
            int: 清理的窗口数量
        """
        cleared = 0
        with self._lock:
            keys_to_remove = []
            for window_key, window in self._windows.items():
                if window["end_time"] and window["end_time"] < before:
                    keys_to_remove.append(window_key)
            
            for key in keys_to_remove:
                del self._windows[key]
                cleared += 1
        
        return cleared


class AnomalyDetector:
    """
    异常检测器。
    
    实现多级异常检测：
    - 级别1：单条数据异常（已在 models.py 中验证）
    - 级别2：趋势异常（10分钟内流量环比增长 > 50%）
    
    解决场景三：时间窗口边界处理
    - 处理分母为零的情况
    - 定义"无数据"状态的默认行为
    """
    
    def __init__(
        self,
        surge_threshold: float = 0.5,
        surge_window_minutes: int = 10,
        min_samples_for_detection: int = 5,
    ):
        """
        初始化异常检测器。
        
        Args:
            surge_threshold: 激增阈值（增长率，如 0.5 表示 50%）
            surge_window_minutes: 激增检测窗口（分钟）
            min_samples_for_detection: 最小样本数
        """
        self.surge_threshold = surge_threshold
        self.surge_window_minutes = surge_window_minutes
        self.min_samples_for_detection = min_samples_for_detection
        
        self._sensor_windows: dict[str, SlidingWindow] = {}
        self._sensor_baselines: dict[str, float] = {}
        self._lock = threading.Lock()
    
    def _get_window(self, sensor_id: str) -> SlidingWindow:
        """
        获取或创建传感器的滑动窗口。
        """
        if sensor_id not in self._sensor_windows:
            window_size = self.surge_window_minutes * 6
            self._sensor_windows[sensor_id] = SlidingWindow(window_size)
        return self._sensor_windows[sensor_id]
    
    def detect(self, record: TrafficRecord) -> Optional[Alert]:
        """
        检测异常。
        
        Args:
            record: 交通记录
            
        Returns:
            Optional[Alert]: 如果检测到异常，返回报警对象
        """
        with self._lock:
            window = self._get_window(record.sensor_id)
            
            data = WindowData(
                timestamp=record.timestamp,
                vehicle_count=record.vehicle_count,
                speed=record.speed,
                vehicle_type=record.vehicle_type,
                region=record.region,
            )
            
            alert = self._check_surge(record, window)
            
            window.add(data)
            
            self._update_baseline(record.sensor_id, record.vehicle_count)
            
            return alert
    
    def _check_surge(
        self, record: TrafficRecord, window: SlidingWindow
    ) -> Optional[Alert]:
        """
        检测流量激增。
        
        解决场景三：时间窗口边界处理
        
        边界情况：
        1. 窗口为空 → 返回 None（无基准）
        2. 窗口数据不足 → 返回 None（样本太少）
        3. 基准值为 0 → 返回 None（无法计算增长率）
        4. 正常情况 → 计算增长率并判断
        """
        if window.get_count() < self.min_samples_for_detection:
            return None
        
        current_count = record.vehicle_count
        avg_count = window.get_average_count()
        
        if avg_count == 0:
            if current_count > 0:
                logger.debug(
                    f"传感器 [{record.sensor_id}] 从零流量变为有流量，"
                    f"当前: {current_count}"
                )
            return None
        
        growth_rate = (current_count - avg_count) / avg_count
        
        if growth_rate > self.surge_threshold:
            return Alert(
                timestamp=record.timestamp,
                sensor_id=record.sensor_id,
                anomaly_level=AnomalyLevel.LEVEL_2,
                anomaly_type=AnomalyType.SUDDEN_SURGE,
                message=(
                    f"传感器 [{record.sensor_id}] 检测到流量激增，"
                    f"增长率: {growth_rate:.1%}"
                ),
                current_value=current_count,
                threshold_value=avg_count * (1 + self.surge_threshold),
                suggestion="建议检查该路段是否有异常事件（事故、活动等）",
            )
        
        return None
    
    def _update_baseline(self, sensor_id: str, count: int) -> None:
        """
        更新基准值。
        
        使用指数移动平均更新基准值。
        """
        if sensor_id not in self._sensor_baselines:
            self._sensor_baselines[sensor_id] = count
        else:
            alpha = 0.1
            self._sensor_baselines[sensor_id] = (
                alpha * count + (1 - alpha) * self._sensor_baselines[sensor_id]
            )
    
    def get_baseline(self, sensor_id: str) -> Optional[float]:
        """
        获取传感器的基准值。
        """
        return self._sensor_baselines.get(sensor_id)


class TrafficAggregator:
    """
    交通流量聚合器。
    
    整合滑动窗口、时间窗口聚合和异常检测。
    """
    
    def __init__(
        self,
        window_size: int = 1000,
        time_window_minutes: int = 15,
        surge_threshold: float = 0.5,
    ):
        """
        初始化聚合器。
        
        Args:
            window_size: 滑动窗口大小
            time_window_minutes: 时间窗口大小（分钟）
            surge_threshold: 激增阈值
        """
        self.window_size = window_size
        self.time_window_minutes = time_window_minutes
        
        self._sensor_windows: dict[str, SlidingWindow] = {}
        self._time_aggregator = TimeWindowAggregator(time_window_minutes)
        self._anomaly_detector = AnomalyDetector(surge_threshold)
        
        self._alerts: list[Alert] = []
        self._stats = {
            "total_records": 0,
            "total_alerts": 0,
            "alerts_by_type": defaultdict(int),
        }
        self._lock = threading.Lock()
    
    def _get_window(self, sensor_id: str) -> SlidingWindow:
        """
        获取或创建传感器的滑动窗口。
        """
        if sensor_id not in self._sensor_windows:
            self._sensor_windows[sensor_id] = SlidingWindow(self.window_size)
        return self._sensor_windows[sensor_id]
    
    def process(
        self, record: TrafficRecord
    ) -> tuple[Optional[AggregationResult], Optional[Alert]]:
        """
        处理单条记录。
        
        Args:
            record: 交通记录
            
        Returns:
            tuple: (聚合结果, 报警信息)
        """
        with self._lock:
            self._stats["total_records"] += 1
            
            window = self._get_window(record.sensor_id)
            
            data = WindowData(
                timestamp=record.timestamp,
                vehicle_count=record.vehicle_count,
                speed=record.speed,
                vehicle_type=record.vehicle_type,
                region=record.region,
            )
            window.add(data)
            
            self._time_aggregator.add(record)
            
            alert = self._anomaly_detector.detect(record)
            
            if alert:
                self._alerts.append(alert)
                self._stats["total_alerts"] += 1
                self._stats["alerts_by_type"][alert.anomaly_type.value] += 1
                logger.warning(
                    f"检测到异常: {alert.message}"
                )
            
            aggregation = self._get_current_aggregation(record.sensor_id, window)
            
            return aggregation, alert
    
    def _get_current_aggregation(
        self, sensor_id: str, window: SlidingWindow
    ) -> Optional[AggregationResult]:
        """
        获取当前聚合结果。
        """
        if window.get_count() == 0:
            return None
        
        oldest = window.get_oldest_timestamp()
        newest = window.get_newest_timestamp()
        
        if not oldest or not newest:
            return None
        
        return AggregationResult(
            sensor_id=sensor_id,
            window_start=oldest,
            window_end=newest,
            total_count=window.get_total_count(),
            avg_speed=window.get_average_speed(),
            record_count=window.get_count(),
        )
    
    def get_all_alerts(self) -> list[Alert]:
        """
        获取所有报警。
        """
        with self._lock:
            return list(self._alerts)
    
    def get_time_aggregations(self) -> list[AggregationResult]:
        """
        获取时间窗口聚合结果。
        """
        return self._time_aggregator.get_all_aggregations()
    
    def get_sensor_stats(self, sensor_id: str) -> Optional[dict[str, Any]]:
        """
        获取传感器统计信息。
        """
        with self._lock:
            if sensor_id not in self._sensor_windows:
                return None
            
            window = self._sensor_windows[sensor_id]
            baseline = self._anomaly_detector.get_baseline(sensor_id)
            
            return {
                "sensor_id": sensor_id,
                "record_count": window.get_count(),
                "total_vehicles": window.get_total_count(),
                "avg_count": window.get_average_count(),
                "avg_speed": window.get_average_speed(),
                "baseline": baseline,
            }
    
    def get_stats(self) -> dict[str, Any]:
        """
        获取整体统计信息。
        """
        with self._lock:
            return {
                "total_records": self._stats["total_records"],
                "total_alerts": self._stats["total_alerts"],
                "alerts_by_type": dict(self._stats["alerts_by_type"]),
                "active_sensors": len(self._sensor_windows),
            }
    
    def clear_old_data(self, before: datetime) -> dict[str, int]:
        """
        清理旧数据。
        """
        cleared = {
            "time_windows": self._time_aggregator.clear_old_windows(before),
            "alerts": 0,
        }
        
        with self._lock:
            old_alerts = [a for a in self._alerts if a.timestamp < before]
            cleared["alerts"] = len(old_alerts)
            self._alerts = [a for a in self._alerts if a.timestamp >= before]
        
        return cleared
