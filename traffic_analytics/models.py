"""
交通数据模型模块 (models.py)

定义严格的数据模型 TrafficRecord，使用 Pydantic 进行数据验证。
包含字段验证、类型注解和基础业务规则验证。
"""

from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List

# 尝试导入 Pydantic，如果失败则使用纯 Python 实现
try:
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

import logging

logger = logging.getLogger(__name__)


class VehicleType(str, Enum):
    """车辆类型枚举"""
    CAR = "car"           # 轿车
    TRUCK = "truck"       # 卡车
    BUS = "bus"           # 公交
    MOTORCYCLE = "motorcycle"  # 摩托车
    UNKNOWN = "unknown"   # 未知


class DataQuality(str, Enum):
    """数据质量标记"""
    VALID = "valid"           # 有效数据
    SUSPICIOUS = "suspicious" # 可疑但可接受
    INVALID = "invalid"       # 无效数据


if PYDANTIC_AVAILABLE:
    # Pydantic 版本
    class TrafficRecord(BaseModel):
        """
        交通流量记录数据模型
        
        字段说明:
            sensor_id: 传感器唯一标识
            timestamp: 数据采集时间戳
            vehicle_count: 车辆计数
            vehicle_type: 车辆类型
            speed: 平均速度 (km/h)
            lane_id: 车道编号 (可选)
            region: 区域代码 (可选)
            quality: 数据质量标记
            raw_data: 原始数据备份
            validation_errors: 验证错误列表
        """
        
        class Config:
            validate_assignment = True
        
        sensor_id: str = Field(..., min_length=1, max_length=50, description="传感器ID")
        timestamp: datetime = Field(..., description="数据采集时间戳")
        vehicle_count: int = Field(..., ge=0, le=10000, description="车辆数量")
        vehicle_type: VehicleType = Field(default=VehicleType.UNKNOWN, description="车辆类型")
        speed: Optional[float] = Field(default=None, ge=0, le=200, description="平均速度(km/h)")
        lane_id: Optional[int] = Field(default=None, ge=1, le=10, description="车道编号")
        region: Optional[str] = Field(default=None, max_length=50, description="区域代码")
        quality: DataQuality = Field(default=DataQuality.VALID, description="数据质量")
        raw_data: Optional[Dict[str, Any]] = Field(default=None, description="原始数据")
        validation_errors: List[str] = Field(default_factory=list, description="验证错误")
        
        @validator('sensor_id')
        def validate_sensor_id(cls, v):
            if not v or not str(v).strip():
                raise ValueError("传感器ID不能为空")
            return str(v).strip().upper()
        
        @validator('timestamp')
        def validate_timestamp(cls, v):
            if v is None:
                raise ValueError("时间戳不能为空")
            now = datetime.now()
            if v > now.replace(minute=now.minute + 5):
                raise ValueError(f"时间戳不能是未来时间: {v}")
            from datetime import timedelta
            if v < now - timedelta(days=7):
                raise ValueError(f"时间戳已过期: {v}")
            return v
        
        @validator('vehicle_count')
        def validate_vehicle_count(cls, v):
            if v < 0:
                raise ValueError("车辆数量不能为负数")
            if v > 10000:
                raise ValueError("车辆数量异常，超过10000")
            return v
        
        @validator('speed')
        def validate_speed(cls, v):
            if v is None:
                return v
            if v < 0:
                raise ValueError("速度不能为负数")
            if v > 200:
                raise ValueError("速度异常，超过200km/h")
            return round(v, 2) if v else v
        
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

else:
    # 纯 Python 版本
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
            if timestamp > now.replace(minute=now.minute + 5):
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


# Alert 和 AggregationResult 也使用条件定义
if PYDANTIC_AVAILABLE:
    class Alert(BaseModel):
        """异常报警模型"""
        alert_type: str = Field(..., description="报警类型")
        severity: str = Field(..., description="严重程度: low/medium/high/critical")
        sensor_id: Optional[str] = Field(default=None, description="相关传感器ID")
        region: Optional[str] = Field(default=None, description="区域")
        timestamp: datetime = Field(..., description="报警时间")
        message: str = Field(..., description="报警消息")
        details: Dict[str, Any] = Field(default_factory=dict, description="详细信息")
        suggested_action: Optional[str] = Field(default=None, description="建议措施")
        
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
    
    class SensorStatus(BaseModel):
        """传感器状态模型"""
        sensor_id: str = Field(..., description="传感器ID")
        is_active: bool = Field(default=True, description="是否活跃")
        is_tripped: bool = Field(default=False, description="是否熔断")
        total_records: int = Field(default=0, description="总记录数")
        error_count: int = Field(default=0, description="错误记录数")
        error_rate: float = Field(default=0.0, description="错误率")
        last_seen: Optional[datetime] = Field(default=None, description="最后活跃时间")
        trip_reason: Optional[str] = Field(default=None, description="熔断原因")
        tripped_at: Optional[datetime] = Field(default=None, description="熔断时间")
        
        def update_error_rate(self) -> None:
            """更新错误率"""
            if self.total_records > 0:
                self.error_rate = round(self.error_count / self.total_records, 4)
        
        def should_trip(self, threshold: float = 0.2) -> bool:
            """检查是否应该熔断"""
            if self.total_records < 10:
                return False
            return self.error_rate > threshold
    
    class AggregationResult(BaseModel):
        """聚合结果模型"""
        window_start: datetime = Field(..., description="窗口开始时间")
        window_end: datetime = Field(..., description="窗口结束时间")
        window_minutes: int = Field(..., description="窗口大小(分钟)")
        sensor_id: Optional[str] = Field(default=None, description="传感器ID")
        region: Optional[str] = Field(default=None, description="区域")
        vehicle_type: Optional[VehicleType] = Field(default=None, description="车辆类型")
        total_count: int = Field(default=0, description="总车辆数")
        record_count: int = Field(default=0, description="记录数")
        avg_speed: Optional[float] = Field(default=None, description="平均速度")
        max_count: int = Field(default=0, description="最大流量")
        min_count: int = Field(default=0, description="最小流量")
        growth_rate: Optional[float] = Field(default=None, description="环比增长率")
        year_over_year: Optional[float] = Field(default=None, description="同比增长率")
        
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

else:
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
        
        def update_error_rate(self) -> None:
            if self.total_records > 0:
                self.error_rate = round(self.error_count / self.total_records, 4)
        
        def should_trip(self, threshold: float = 0.2) -> bool:
            if self.total_records < 10:
                return False
            return self.error_rate > threshold
    
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


# 导入 timedelta 用于纯 Python 版本
from datetime import timedelta
