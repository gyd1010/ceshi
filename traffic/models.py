"""
数据模型层：定义交通流量数据模型，使用Pydantic进行严格的数据验证。

解决场景四：模块解耦测试
- 使用 Optional 类型注解处理可能为空的清洗结果
- 下游模块可防御性处理 None 值
"""
import re
from datetime import datetime
from enum import Enum
from typing import Optional, Any

from pydantic import BaseModel, field_validator, model_validator


class VehicleType(str, Enum):
    """
    车辆类型枚举。
    """
    CAR = "car"
    TRUCK = "truck"
    BUS = "bus"
    MOTORCYCLE = "motorcycle"
    UNKNOWN = "unknown"


class AnomalyLevel(int, Enum):
    """
    异常级别枚举。
    """
    LEVEL_1 = 1  # 单条数据异常
    LEVEL_2 = 2  # 趋势异常


class AnomalyType(str, Enum):
    """
    异常类型枚举。
    """
    NEGATIVE_COUNT = "negative_count"
    NEGATIVE_SPEED = "negative_speed"
    FUTURE_TIMESTAMP = "future_timestamp"
    INVALID_TIMESTAMP = "invalid_timestamp"
    DUPLICATE_RECORD = "duplicate_record"
    OUTLIER_COUNT = "outlier_count"
    OUTLIER_SPEED = "outlier_speed"
    SUDDEN_SURGE = "sudden_surge"
    SENSOR_MALFUNCTION = "sensor_malfunction"


class TrafficRecord(BaseModel):
    """
    交通流量记录数据模型。
    
    Attributes:
        sensor_id: 传感器ID，格式为 "SENSOR_XXX"
        timestamp: 时间戳，ISO 8601格式
        vehicle_count: 车辆数量，必须 >= 0
        vehicle_type: 车辆类型
        speed: 平均速度 (km/h)，必须 >= 0
        region: 区域标识（可选，用于分组聚合）
        raw_data: 原始数据（用于调试和追溯）
    """
    sensor_id: str
    timestamp: datetime
    vehicle_count: int
    vehicle_type: VehicleType = VehicleType.UNKNOWN
    speed: float
    region: Optional[str] = None
    raw_data: Optional[dict[str, Any]] = None

    @field_validator("sensor_id")
    @classmethod
    def validate_sensor_id(cls, v: str) -> str:
        """
        验证传感器ID格式。
        格式要求：SENSOR_XXX 或 SENSOR-XXX-XXX
        """
        if not v:
            raise ValueError("传感器ID不能为空")
        pattern = r"^SENSOR[_-][A-Z0-9]+([-_][A-Z0-9]+)*$"
        if not re.match(pattern, v.upper()):
            raise ValueError(f"无效的传感器ID格式: {v}，期望格式: SENSOR_XXX")
        return v.upper()

    @field_validator("vehicle_count")
    @classmethod
    def validate_vehicle_count(cls, v: int) -> int:
        """
        验证车辆数量。
        
        解决场景一（级别1异常）：负数车流检测
        """
        if v < 0:
            raise ValueError(f"车辆数量不能为负数: {v}")
        if v > 10000:
            raise ValueError(f"车辆数量异常（可能传感器故障）: {v}")
        return v

    @field_validator("speed")
    @classmethod
    def validate_speed(cls, v: float) -> float:
        """
        验证速度值。
        
        解决场景一（级别1异常）：负数速度检测
        """
        if v < 0:
            raise ValueError(f"速度不能为负数: {v}")
        if v > 300:
            raise ValueError(f"速度值异常（超过300km/h）: {v}")
        return round(v, 2)

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: datetime) -> datetime:
        """
        验证时间戳。
        
        解决场景一（级别1异常）：未来时间检测
        """
        if v > datetime.now():
            raise ValueError(f"时间戳不能是未来时间: {v}")
        if v.year < 2020:
            raise ValueError(f"时间戳年份过旧: {v}")
        return v

    def to_dict(self) -> dict[str, Any]:
        """
        转换为字典格式。
        """
        return {
            "sensor_id": self.sensor_id,
            "timestamp": self.timestamp.isoformat(),
            "vehicle_count": self.vehicle_count,
            "vehicle_type": self.vehicle_type.value,
            "speed": self.speed,
            "region": self.region,
        }


class CleaningResult(BaseModel):
    """
    数据清洗结果模型。
    
    解决场景四：模块解耦测试
    - 使用 Optional[TrafficRecord] 允许清洗失败返回 None
    - 记录清洗过程中的错误信息
    """
    record: Optional[TrafficRecord] = None
    is_valid: bool = True
    anomaly_type: Optional[AnomalyType] = None
    error_message: Optional[str] = None
    original_data: Optional[dict[str, Any]] = None


class AggregationResult(BaseModel):
    """
    聚合计算结果模型。
    """
    sensor_id: str
    window_start: datetime
    window_end: datetime
    total_count: int
    avg_speed: float
    record_count: int
    vehicle_type_breakdown: dict[str, int] = {}
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "sensor_id": self.sensor_id,
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "total_count": self.total_count,
            "avg_speed": round(self.avg_speed, 2),
            "record_count": self.record_count,
            "vehicle_type_breakdown": self.vehicle_type_breakdown,
        }


class Alert(BaseModel):
    """
    报警信息模型。
    """
    timestamp: datetime
    sensor_id: str
    anomaly_level: AnomalyLevel
    anomaly_type: AnomalyType
    message: str
    current_value: Optional[float] = None
    threshold_value: Optional[float] = None
    suggestion: Optional[str] = None
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "sensor_id": self.sensor_id,
            "anomaly_level": self.anomaly_level.value,
            "anomaly_type": self.anomaly_type.value,
            "message": self.message,
            "current_value": self.current_value,
            "threshold_value": self.threshold_value,
            "suggestion": self.suggestion,
        }


class SensorStatus(BaseModel):
    """
    传感器状态模型。
    """
    sensor_id: str
    is_active: bool = True
    total_records: int = 0
    error_count: int = 0
    last_seen: Optional[datetime] = None
    last_error: Optional[str] = None
    is_circuit_open: bool = False
    
    @property
    def error_rate(self) -> float:
        """
        计算错误率。
        """
        if self.total_records == 0:
            return 0.0
        return self.error_count / self.total_records


class TrafficReport(BaseModel):
    """
    交通流量报告模型。
    """
    generated_at: datetime
    time_range: tuple[datetime, datetime]
    total_records_processed: int
    total_errors: int
    sensors_active: int
    sensors_circuit_open: int
    aggregations: list[AggregationResult] = []
    alerts: list[Alert] = []
    sensor_statuses: dict[str, SensorStatus] = {}
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at.isoformat(),
            "time_range": [
                self.time_range[0].isoformat(),
                self.time_range[1].isoformat(),
            ],
            "total_records_processed": self.total_records_processed,
            "total_errors": self.total_errors,
            "sensors_active": self.sensors_active,
            "sensors_circuit_open": self.sensors_circuit_open,
            "aggregations": [a.to_dict() for a in self.aggregations],
            "alerts": [a.to_dict() for a in self.alerts],
            "sensor_statuses": {
                k: v.model_dump() for k, v in self.sensor_statuses.items()
            },
        }
