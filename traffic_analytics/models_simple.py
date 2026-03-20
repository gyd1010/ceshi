"""
交通数据模型模块 (models_simple.py)

不使用 Pydantic 的纯 Python 实现，确保最大兼容性。
"""

from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class VehicleType(str, Enum):
    """车辆类型枚举"""
    CAR = "car"
    TRUCK = "truck"
    BUS = "bus"
    MOTORCYCLE = "motorcycle"
    UNKNOWN = "unknown"


class DataQuality(str, Enum):
    """数据质量标记"""
    VALID = "valid"
    SUSPICIOUS = "suspicious"
    INVALID = "invalid"


class TrafficRecord:
    """交通流量记录数据模型 (纯 Python 实现)"""
    
    def __init__(
        self,
        sensor_id: str,
        timestamp: datetime,
        vehicle_count: int,
        vehicle_type: VehicleType = VehicleType.UNKNOWN,
        speed: Optional[float] = None,
        lane_id: Optional[int] = None,
        region: Optional[str] = None,
        quality: DataQuality = DataQuality.VALID,
        raw_data: Optional[Dict[str, Any]] = None,
        validation_errors: Optional[List[str]] = None
    ):
        # 验证和标准化
        if not sensor_id or not str(sensor_id).strip():
            raise ValueError("传感器ID不能为空")
        
        self.sensor_id = str(sensor_id).strip().upper()
        
        if timestamp is None:
            raise ValueError("时间戳不能为空")
        
        now = datetime.now()
        if timestamp > now + timedelta(minutes=5):
            raise ValueError(f"时间戳不能是未来时间: {timestamp}")
        if timestamp < now - timedelta(days=7):
            raise ValueError(f"时间戳已过期: {timestamp}")
        
        self.timestamp = timestamp
        
        # 验证车辆数
        if vehicle_count < 0:
            raise ValueError("车辆数量不能为负数")
        if vehicle_count > 10000:
            raise ValueError("车辆数量异常，超过10000")
        
        self.vehicle_count = vehicle_count
        
        # 验证速度
        if speed is not None:
            if speed < 0:
                raise ValueError("速度不能为负数")
            if speed > 200:
                raise ValueError("速度异常，超过200km/h")
            self.speed = round(speed, 2)
        else:
            self.speed = None
        
        self.vehicle_type = vehicle_type if isinstance(vehicle_type, VehicleType) else VehicleType.UNKNOWN
        self.lane_id = lane_id
        self.region = region
        self.quality = quality if isinstance(quality, DataQuality) else DataQuality.VALID
        self.raw_data = raw_data
        self.validation_errors = validation_errors or []
    
    def mark_suspicious(self, reason: str) -> "TrafficRecord":
        """标记数据为可疑"""
        self.quality = DataQuality.SUSPICIOUS
        self.validation_errors.append(reason)
        logger.warning(f"记录被标记为可疑 [sensor={self.sensor_id}]: {reason}")
        return self
    
    def mark_invalid(self, reason: str) -> "TrafficRecord":
        """标记数据为无效"""
        self.quality = DataQuality.INVALID
        self.validation_errors.append(reason)
        logger.warning(f"记录被标记为无效 [sensor={self.sensor_id}]: {reason}")
        return self
    
    def is_valid(self) -> bool:
        """检查记录是否有效"""
        return self.quality != DataQuality.INVALID
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "sensor_id": self.sensor_id,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "vehicle_count": self.vehicle_count,
            "vehicle_type": self.vehicle_type.value if self.vehicle_type else None,
            "speed": self.speed,
            "lane_id": self.lane_id,
            "region": self.region,
            "quality": self.quality.value if self.quality else None,
            "validation_errors": self.validation_errors
        }
    
    def get_time_bucket(self, bucket_minutes: int = 15) -> datetime:
        """获取时间桶"""
        minute = (self.timestamp.minute // bucket_minutes) * bucket_minutes
        return self.timestamp.replace(minute=minute, second=0, microsecond=0)
    
    def __repr__(self) -> str:
        return f"TrafficRecord(sensor={self.sensor_id}, count={self.vehicle_count}, time={self.timestamp})"


class Alert:
    """异常报警模型"""
    
    def __init__(
        self,
        alert_type: str,
        severity: str,
        timestamp: datetime,
        message: str,
        sensor_id: Optional[str] = None,
        region: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        suggested_action: Optional[str] = None
    ):
        self.alert_type = alert_type
        self.severity = severity
        self.sensor_id = sensor_id
        self.region = region
        self.timestamp = timestamp
        self.message = message
        self.details = details or {}
        self.suggested_action = suggested_action
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_type": self.alert_type,
            "severity": self.severity,
            "sensor_id": self.sensor_id,
            "region": self.region,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "message": self.message,
            "details": self.details,
            "suggested_action": self.suggested_action
        }


class SensorStatus:
    """传感器状态模型"""
    
    def __init__(
        self,
        sensor_id: str,
        is_active: bool = True,
        is_tripped: bool = False,
        total_records: int = 0,
        error_count: int = 0,
        error_rate: float = 0.0,
        last_seen: Optional[datetime] = None,
        trip_reason: Optional[str] = None,
        tripped_at: Optional[datetime] = None
    ):
        self.sensor_id = sensor_id
        self.is_active = is_active
        self.is_tripped = is_tripped
        self.total_records = total_records
        self.error_count = error_count
        self.error_rate = error_rate
        self.last_seen = last_seen
        self.trip_reason = trip_reason
        self.tripped_at = tripped_at


class AggregationResult:
    """聚合结果模型"""
    
    def __init__(
        self,
        window_start: datetime,
        window_end: datetime,
        window_minutes: int,
        sensor_id: Optional[str] = None,
        region: Optional[str] = None,
        vehicle_type: Optional[VehicleType] = None,
        total_count: int = 0,
        record_count: int = 0,
        avg_speed: Optional[float] = None,
        max_count: int = 0,
        min_count: int = 0,
        growth_rate: Optional[float] = None,
        year_over_year: Optional[float] = None
    ):
        self.window_start = window_start
        self.window_end = window_end
        self.window_minutes = window_minutes
        self.sensor_id = sensor_id
        self.region = region
        self.vehicle_type = vehicle_type
        self.total_count = total_count
        self.record_count = record_count
        self.avg_speed = avg_speed
        self.max_count = max_count
        self.min_count = min_count
        self.growth_rate = growth_rate
        self.year_over_year = year_over_year
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "window_start": self.window_start.isoformat() if self.window_start else None,
            "window_end": self.window_end.isoformat() if self.window_end else None,
            "window_minutes": self.window_minutes,
            "sensor_id": self.sensor_id,
            "region": self.region,
            "vehicle_type": self.vehicle_type.value if self.vehicle_type else None,
            "total_count": self.total_count,
            "record_count": self.record_count,
            "avg_speed": self.avg_speed,
            "max_count": self.max_count,
            "min_count": self.min_count,
            "growth_rate": self.growth_rate,
            "year_over_year": self.year_over_year
        }
