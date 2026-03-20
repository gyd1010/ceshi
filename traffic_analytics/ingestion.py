"""
数据接入层模块 (ingestion.py)

实现数据源的抽象和具体实现，使用 Python Generator 实现流式读取，
支持大文件处理而不占用大量内存。
"""

import csv
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional, Dict, Any, Callable, Union, List
from contextlib import contextmanager

from models import TrafficRecord, VehicleType

logger = logging.getLogger(__name__)


class DataStream(ABC):
    """
    数据流抽象基类
    
    定义数据源的通用接口，所有具体数据源实现都必须继承此类。
    支持上下文管理器协议，确保资源正确释放。
    """
    
    def __init__(self, source: Union[str, Path], **kwargs):
        """
        初始化数据流
        
        Args:
            source: 数据源标识(文件路径、连接字符串等)
            **kwargs: 额外配置参数
        """
        self.source = source
        self.config = kwargs
        self._is_open = False
        self._record_count = 0
        self._error_count = 0
    
    @abstractmethod
    def open(self) -> None:
        """打开数据源连接"""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """关闭数据源连接"""
        pass
    
    @abstractmethod
    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """
        返回原始数据迭代器
        
        Yields:
            原始数据字典
        """
        pass
    
    def records(self) -> Iterator[TrafficRecord]:
        """
        返回解析后的 TrafficRecord 迭代器
        
        Yields:
            TrafficRecord 对象
        """
        for raw_data in self:
            try:
                record = self._parse_record(raw_data)
                if record:
                    self._record_count += 1
                    yield record
            except Exception as e:
                self._error_count += 1
                logger.error(f"解析记录失败: {e}, 原始数据: {raw_data}")
                continue
    
    @abstractmethod
    def _parse_record(self, raw_data: Dict[str, Any]) -> Optional[TrafficRecord]:
        """
        将原始数据解析为 TrafficRecord
        
        Args:
            raw_data: 原始数据字典
        
        Returns:
            TrafficRecord 对象，解析失败返回 None
        """
        pass
    
    def __enter__(self):
        """上下文管理器入口"""
        self.open()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()
    
    @property
    def stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "source": str(self.source),
            "record_count": self._record_count,
            "error_count": self._error_count,
            "is_open": self._is_open
        }


class FileDataStream(DataStream):
    """
    文件数据流实现
    
    支持 CSV 和 JSONL 格式，使用 Generator 逐行读取，
    内存占用恒定，可处理 GB 级大文件。
    
    关键设计:
    - 使用 yield 实现惰性求值，避免一次性加载所有数据
    - 支持 chunk_size 配置，控制每次读取的数据块大小
    - 自动检测文件格式，根据扩展名选择解析器
    """
    
    def __init__(
        self,
        source: Union[str, Path],
        file_format: Optional[str] = None,
        encoding: str = "utf-8",
        delimiter: str = ",",
        skip_header: bool = True,
        **kwargs
    ):
        """
        初始化文件数据流
        
        Args:
            source: 文件路径
            file_format: 文件格式 (csv/jsonl)，None 则自动检测
            encoding: 文件编码
            delimiter: CSV 分隔符
            skip_header: 是否跳过 CSV 表头
        """
        super().__init__(source, **kwargs)
        self.file_path = Path(source)
        self.encoding = encoding
        self.delimiter = delimiter
        self.skip_header = skip_header
        
        # 自动检测文件格式
        if file_format is None:
            self.file_format = self._detect_format()
        else:
            self.file_format = file_format.lower()
        
        self._file_handle = None
        self._reader = None
    
    def _detect_format(self) -> str:
        """根据文件扩展名检测格式"""
        suffix = self.file_path.suffix.lower()
        if suffix == ".csv":
            return "csv"
        elif suffix in [".jsonl", ".jsonlines"]:
            return "jsonl"
        elif suffix == ".json":
            return "json"
        else:
            # 默认尝试 CSV
            logger.warning(f"无法检测文件格式，默认使用 CSV: {self.file_path}")
            return "csv"
    
    def open(self) -> None:
        """打开文件"""
        if self._is_open:
            return
        
        if not self.file_path.exists():
            raise FileNotFoundError(f"文件不存在: {self.file_path}")
        
        try:
            self._file_handle = open(self.file_path, "r", encoding=self.encoding)
            
            if self.file_format == "csv":
                self._reader = csv.DictReader(
                    self._file_handle,
                    delimiter=self.delimiter
                )
            elif self.file_format == "jsonl":
                # JSONL 不需要预创建 reader，逐行解析
                self._reader = None
            else:
                raise ValueError(f"不支持的文件格式: {self.file_format}")
            
            self._is_open = True
            logger.info(f"成功打开文件: {self.file_path} (格式: {self.file_format})")
            
        except Exception as e:
            logger.error(f"打开文件失败: {e}")
            if self._file_handle:
                self._file_handle.close()
            raise
    
    def close(self) -> None:
        """关闭文件"""
        if self._file_handle:
            self._file_handle.close()
            self._file_handle = None
            self._reader = None
            self._is_open = False
            logger.info(f"关闭文件: {self.file_path}")
    
    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """
        逐行读取并返回原始数据
        
        Yields:
            原始数据字典
        
        内存优化说明:
        - 使用 yield 实现惰性求值，每次只处理一行数据
        - 空间复杂度 O(1)，与文件大小无关
        - 对比一次性加载: 内存占用从 O(N) 降为 O(1)
        """
        if not self._is_open:
            raise RuntimeError("文件未打开，请使用上下文管理器或先调用 open()")
        
        if self.file_format == "csv":
            yield from self._iter_csv()
        elif self.file_format == "jsonl":
            yield from self._iter_jsonl()
    
    def _iter_csv(self) -> Iterator[Dict[str, Any]]:
        """CSV 格式迭代器"""
        for row in self._reader:
            yield dict(row)
    
    def _iter_jsonl(self) -> Iterator[Dict[str, Any]]:
        """JSONL 格式迭代器"""
        for line_num, line in enumerate(self._file_handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                logger.warning(f"JSON 解析错误 [行 {line_num}]: {e}")
                self._error_count += 1
    
    def _parse_record(self, raw_data: Dict[str, Any]) -> Optional[TrafficRecord]:
        """
        解析原始数据为 TrafficRecord
        
        支持灵活的字段映射，处理不同格式的输入数据
        """
        try:
            # 字段映射: 支持多种可能的字段名
            field_mapping = {
                "sensor_id": ["sensor_id", "sensor", "device_id", "camera_id", "id"],
                "timestamp": ["timestamp", "time", "datetime", "created_at", "date"],
                "vehicle_count": ["vehicle_count", "count", "volume", "traffic", "vehicles"],
                "vehicle_type": ["vehicle_type", "type", "vehicle", "category"],
                "speed": ["speed", "avg_speed", "velocity", "speed_kmh"],
                "lane_id": ["lane_id", "lane", "lane_no"],
                "region": ["region", "area", "district", "zone"]
            }
            
            # 提取字段值
            def get_field(field_name: str, default=None):
                for key in field_mapping.get(field_name, [field_name]):
                    if key in raw_data:
                        return raw_data[key]
                return default
            
            # 解析时间戳
            timestamp = self._parse_timestamp(get_field("timestamp"))
            if timestamp is None:
                raise ValueError("无法解析时间戳")
            
            # 解析车辆类型
            vehicle_type = self._parse_vehicle_type(get_field("vehicle_type", "unknown"))
            
            # 解析速度
            speed = self._parse_float(get_field("speed"))
            
            # 解析车道ID
            lane_id = self._parse_int(get_field("lane_id"))
            
            # 创建记录
            record = TrafficRecord(
                sensor_id=str(get_field("sensor_id", "unknown")),
                timestamp=timestamp,
                vehicle_count=int(get_field("vehicle_count", 0)),
                vehicle_type=vehicle_type,
                speed=speed,
                lane_id=lane_id,
                region=get_field("region"),
                raw_data=raw_data
            )
            
            return record
            
        except Exception as e:
            logger.error(f"解析记录失败: {e}, 数据: {raw_data}")
            return None
    
    def _parse_timestamp(self, value: Any) -> Optional[datetime]:
        """解析时间戳，支持多种格式"""
        if value is None:
            return None
        
        if isinstance(value, datetime):
            return value
        
        if isinstance(value, (int, float)):
            # 尝试解析为 Unix 时间戳
            try:
                if value > 1e10:  # 毫秒时间戳
                    value = value / 1000
                return datetime.fromtimestamp(value)
            except (ValueError, OSError):
                pass
        
        if isinstance(value, str):
            # 尝试多种格式
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y/%m/%d %H:%M:%S",
                "%d/%m/%Y %H:%M:%S",
                "%m/%d/%Y %H:%M:%S",
                "%Y%m%d%H%M%S",
                "%Y-%m-%d"
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
        
        logger.warning(f"无法解析时间戳: {value}")
        return None
    
    def _parse_vehicle_type(self, value: Any) -> VehicleType:
        """解析车辆类型"""
        if value is None:
            return VehicleType.UNKNOWN
        
        value = str(value).lower().strip()
        
        mapping = {
            "car": VehicleType.CAR,
            "轿车": VehicleType.CAR,
            "sedan": VehicleType.CAR,
            "truck": VehicleType.TRUCK,
            "卡车": VehicleType.TRUCK,
            "lorry": VehicleType.TRUCK,
            "bus": VehicleType.BUS,
            "公交": VehicleType.BUS,
            "公交车": VehicleType.BUS,
            "motorcycle": VehicleType.MOTORCYCLE,
            "摩托车": VehicleType.MOTORCYCLE,
            "bike": VehicleType.MOTORCYCLE,
        }
        
        return mapping.get(value, VehicleType.UNKNOWN)
    
    def _parse_float(self, value: Any) -> Optional[float]:
        """解析浮点数"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _parse_int(self, value: Any) -> Optional[int]:
        """解析整数"""
        if value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None


class MockDataStream(DataStream):
    """
    模拟数据流
    
    用于测试和演示，生成模拟的交通数据
    """
    
    def __init__(
        self,
        num_records: int = 1000,
        num_sensors: int = 10,
        start_time: Optional[datetime] = None,
        anomaly_ratio: float = 0.05,
        **kwargs
    ):
        """
        初始化模拟数据流
        
        Args:
            num_records: 生成记录数
            num_sensors: 传感器数量
            start_time: 开始时间
            anomaly_ratio: 异常数据比例
        """
        super().__init__("mock://traffic_data", **kwargs)
        self.num_records = num_records
        self.num_sensors = num_sensors
        self.start_time = start_time or datetime.now()
        self.anomaly_ratio = anomaly_ratio
        self._current_index = 0
    
    def open(self) -> None:
        """打开模拟流"""
        self._is_open = True
        self._current_index = 0
        logger.info(f"打开模拟数据流: {self.num_records} 条记录, {self.num_sensors} 个传感器")
    
    def close(self) -> None:
        """关闭模拟流"""
        self._is_open = False
        logger.info("关闭模拟数据流")
    
    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """生成模拟数据"""
        import random
        from datetime import timedelta
        
        if not self._is_open:
            raise RuntimeError("数据流未打开")
        
        # 计算结束时间，确保不超出当前时间太多
        # 使用过去的时间生成数据，避免未来时间验证失败
        end_time = datetime.now() - timedelta(minutes=5)
        start_time = end_time - timedelta(minutes=self.num_records)
        
        for i in range(self.num_records):
            sensor_id = f"SENSOR_{i % self.num_sensors:03d}"
            timestamp = start_time + timedelta(minutes=i)
            
            # 生成正常数据或异常数据
            is_anomaly = random.random() < self.anomaly_ratio
            
            if is_anomaly:
                # 异常数据: 负数、超大值等
                vehicle_count = random.choice([-10, 9999, -1])
            else:
                # 正常数据: 0-100 辆车
                vehicle_count = random.randint(0, 100)
            
            vehicle_type = random.choice(["car", "truck", "bus", "motorcycle"])
            speed = random.uniform(20, 80) if not is_anomaly else random.choice([-5, 300])
            
            yield {
                "sensor_id": sensor_id,
                "timestamp": timestamp.isoformat(),
                "vehicle_count": vehicle_count,
                "vehicle_type": vehicle_type,
                "speed": round(speed, 2),
                "lane_id": random.randint(1, 4),
                "region": f"REGION_{i % 5:02d}"
            }
    
    def _parse_record(self, raw_data: Dict[str, Any]) -> Optional[TrafficRecord]:
        """解析模拟数据"""
        try:
            return TrafficRecord(
                sensor_id=raw_data["sensor_id"],
                timestamp=datetime.fromisoformat(raw_data["timestamp"]),
                vehicle_count=raw_data["vehicle_count"],
                vehicle_type=VehicleType(raw_data["vehicle_type"]),
                speed=raw_data["speed"],
                lane_id=raw_data.get("lane_id"),
                region=raw_data.get("region"),
                raw_data=raw_data
            )
        except Exception as e:
            logger.error(f"解析模拟数据失败: {e}")
            return None


def create_data_stream(
    source: Union[str, Path],
    stream_type: str = "auto",
    **kwargs
) -> DataStream:
    """
    工厂函数: 创建数据流实例
    
    Args:
        source: 数据源
        stream_type: 流类型 (file/mock/auto)
        **kwargs: 额外参数
    
    Returns:
        DataStream 实例
    """
    if stream_type == "mock":
        return MockDataStream(**kwargs)
    elif stream_type == "file" or stream_type == "auto":
        return FileDataStream(source, **kwargs)
    else:
        raise ValueError(f"未知的数据流类型: {stream_type}")
