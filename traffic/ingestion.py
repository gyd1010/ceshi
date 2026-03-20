"""
数据接入层：抽象数据源接口，实现流式读取大文件。

关键特性：
- 使用 Python Generator 实现惰性求值
- 内存占用 O(1)，可处理 GB 级文件
- 支持多种数据格式（CSV/JSONL）
- 预留数据库和消息队列接口
"""
import csv
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional, Iterator, Any

from models import TrafficRecord, VehicleType, CleaningResult

logger = logging.getLogger(__name__)


class DataStream(ABC):
    """
    数据流抽象基类。
    
    定义统一的数据接入接口，支持多种数据源实现。
    所有实现必须使用 Generator 模式，确保内存效率。
    """
    
    @abstractmethod
    def stream(self) -> Generator[CleaningResult, None, None]:
        """
        流式读取数据。
        
        Yields:
            CleaningResult: 包含原始数据和解析结果的对象
            
        Note:
            必须使用 yield 实现，不能一次性返回所有数据
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """
        关闭数据流，释放资源。
        """
        pass
    
    def __enter__(self) -> "DataStream":
        """
        上下文管理器入口。
        """
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        上下文管理器退出。
        """
        self.close()
    
    def __iter__(self) -> Generator[CleaningResult, None, None]:
        """
        使数据流可迭代。
        """
        return self.stream()


class FileDataStream(DataStream):
    """
    文件数据流实现。
    
    支持从本地文件逐行读取数据，使用 Generator 实现流式处理。
    
    内存优化原理：
    ┌─────────────────────────────────────────────────────────────────┐
    │  传统方式: records = [parse(line) for line in file]            │
    │  内存占用: O(N)，N = 文件行数                                   │
    │                                                                 │
    │  生成器方式: yield parse(line)                                  │
    │  内存占用: O(1)，每次只处理一行                                  │
    │                                                                 │
    │  示例: 1GB 文件约 1000万行                                       │
    │  - 传统方式: 需要 10GB+ 内存                                     │
    │  - 生成器方式: 只需 ~1KB 内存                                    │
    └─────────────────────────────────────────────────────────────────┘
    """
    
    def __init__(
        self,
        file_path: str | Path,
        file_format: str = "csv",
        encoding: str = "utf-8",
        batch_size: int = 1000,
    ):
        """
        初始化文件数据流。
        
        Args:
            file_path: 文件路径
            file_format: 文件格式 (csv, jsonl)
            encoding: 文件编码
            batch_size: 批量读取大小（用于日志记录）
        """
        self.file_path = Path(file_path)
        self.file_format = file_format.lower()
        self.encoding = encoding
        self.batch_size = batch_size
        self._file_handle: Optional[Any] = None
        self._line_count = 0
        self._error_count = 0
        
        if not self.file_path.exists():
            raise FileNotFoundError(f"数据文件不存在: {self.file_path}")
    
    def stream(self) -> Generator[CleaningResult, None, None]:
        """
        流式读取文件数据。
        
        使用 Generator 实现惰性求值，每次只读取一行到内存。
        
        Yields:
            CleaningResult: 包含解析结果的对象
        """
        logger.info(f"开始读取数据文件: {self.file_path}")
        
        if self.file_format == "csv":
            yield from self._stream_csv()
        elif self.file_format == "jsonl":
            yield from self._stream_jsonl()
        else:
            raise ValueError(f"不支持的文件格式: {self.file_format}")
        
        logger.info(
            f"文件读取完成: {self.file_path}, "
            f"总行数: {self._line_count}, 错误: {self._error_count}"
        )
    
    def _stream_csv(self) -> Generator[CleaningResult, None, None]:
        """
        流式读取 CSV 文件。
        """
        with open(self.file_path, "r", encoding=self.encoding) as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                self._line_count += 1
                
                result = self._parse_csv_row(row)
                
                if not result.is_valid:
                    self._error_count += 1
                
                if self._line_count % self.batch_size == 0:
                    logger.debug(f"已处理 {self._line_count} 行")
                
                yield result
    
    def _stream_jsonl(self) -> Generator[CleaningResult, None, None]:
        """
        流式读取 JSONL 文件。
        """
        with open(self.file_path, "r", encoding=self.encoding) as f:
            for line in f:
                self._line_count += 1
                line = line.strip()
                
                if not line:
                    continue
                
                result = self._parse_jsonl_line(line)
                
                if not result.is_valid:
                    self._error_count += 1
                
                if self._line_count % self.batch_size == 0:
                    logger.debug(f"已处理 {self._line_count} 行")
                
                yield result
    
    def _parse_csv_row(self, row: dict[str, str]) -> CleaningResult:
        """
        解析 CSV 行数据。
        
        Args:
            row: CSV 行数据字典
            
        Returns:
            CleaningResult: 解析结果
        """
        original_data = dict(row)
        
        try:
            sensor_id = row.get("sensor_id", "").strip()
            timestamp_str = row.get("timestamp", "").strip()
            vehicle_count_str = row.get("vehicle_count", "0").strip()
            vehicle_type_str = row.get("vehicle_type", "unknown").strip().lower()
            speed_str = row.get("speed", "0").strip()
            region = row.get("region", "").strip() or None
            
            timestamp = self._parse_timestamp(timestamp_str)
            vehicle_count = int(vehicle_count_str)
            speed = float(speed_str)
            vehicle_type = VehicleType(vehicle_type_str)
            
            record = TrafficRecord(
                sensor_id=sensor_id,
                timestamp=timestamp,
                vehicle_count=vehicle_count,
                vehicle_type=vehicle_type,
                speed=speed,
                region=region,
                raw_data=original_data,
            )
            
            return CleaningResult(
                record=record,
                is_valid=True,
                original_data=original_data,
            )
            
        except ValueError as e:
            return CleaningResult(
                record=None,
                is_valid=False,
                error_message=str(e),
                original_data=original_data,
            )
        except Exception as e:
            logger.error(f"解析 CSV 行失败: {e}, 原始数据: {row}")
            return CleaningResult(
                record=None,
                is_valid=False,
                error_message=f"解析错误: {e}",
                original_data=original_data,
            )
    
    def _parse_jsonl_line(self, line: str) -> CleaningResult:
        """
        解析 JSONL 行数据。
        
        Args:
            line: JSON 行字符串
            
        Returns:
            CleaningResult: 解析结果
        """
        try:
            data = json.loads(line)
            original_data = dict(data)
            
            sensor_id = data.get("sensor_id", "")
            timestamp_str = data.get("timestamp", "")
            
            if isinstance(timestamp_str, str):
                timestamp = self._parse_timestamp(timestamp_str)
            elif isinstance(timestamp_str, (int, float)):
                timestamp = datetime.fromtimestamp(timestamp_str)
            else:
                timestamp = datetime.now()
            
            vehicle_count = int(data.get("vehicle_count", 0))
            speed = float(data.get("speed", 0))
            vehicle_type_str = str(data.get("vehicle_type", "unknown")).lower()
            
            try:
                vehicle_type = VehicleType(vehicle_type_str)
            except ValueError:
                vehicle_type = VehicleType.UNKNOWN
            
            region = data.get("region") or None
            
            record = TrafficRecord(
                sensor_id=sensor_id,
                timestamp=timestamp,
                vehicle_count=vehicle_count,
                vehicle_type=vehicle_type,
                speed=speed,
                region=region,
                raw_data=original_data,
            )
            
            return CleaningResult(
                record=record,
                is_valid=True,
                original_data=original_data,
            )
            
        except json.JSONDecodeError as e:
            return CleaningResult(
                record=None,
                is_valid=False,
                error_message=f"JSON 解析错误: {e}",
                original_data={"raw_line": line},
            )
        except Exception as e:
            logger.error(f"解析 JSONL 行失败: {e}, 原始数据: {line}")
            return CleaningResult(
                record=None,
                is_valid=False,
                error_message=f"解析错误: {e}",
                original_data={"raw_line": line},
            )
    
    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """
        解析时间戳字符串。
        
        支持多种格式：
        - ISO 8601: 2024-01-15T10:30:00
        - 常规格式: 2024-01-15 10:30:00
        - Unix 时间戳
        """
        timestamp_str = timestamp_str.strip()
        
        if not timestamp_str:
            return datetime.now()
        
        formats = [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y/%m/%d %H:%M:%S",
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(timestamp_str, fmt)
            except ValueError:
                continue
        
        try:
            unix_ts = float(timestamp_str)
            return datetime.fromtimestamp(unix_ts)
        except ValueError:
            pass
        
        raise ValueError(f"无法解析时间戳: {timestamp_str}")
    
    def close(self) -> None:
        """
        关闭文件句柄。
        """
        if self._file_handle:
            self._file_handle.close()
            self._file_handle = None
    
    @property
    def stats(self) -> dict[str, int]:
        """
        获取读取统计信息。
        """
        return {
            "total_lines": self._line_count,
            "errors": self._error_count,
        }


class MockDataStream(DataStream):
    """
    模拟数据流，用于测试。
    
    生成指定数量的模拟交通记录。
    """
    
    def __init__(
        self,
        num_records: int = 100,
        error_rate: float = 0.1,
        surge_start: int = 50,
        surge_multiplier: float = 3.0,
    ):
        """
        初始化模拟数据流。
        
        Args:
            num_records: 总记录数
            error_rate: 错误率 (0.0 - 1.0)
            surge_start: 流量激增开始位置
            surge_multiplier: 激增倍数
        """
        self.num_records = num_records
        self.error_rate = error_rate
        self.surge_start = surge_start
        self.surge_multiplier = surge_multiplier
        self._generated = 0
    
    def stream(self) -> Generator[CleaningResult, None, None]:
        """
        生成模拟数据流。
        """
        import random
        
        sensors = ["SENSOR_001", "SENSOR_002", "SENSOR_003", "SENSOR_FAULT"]
        vehicle_types = [VehicleType.CAR, VehicleType.TRUCK, VehicleType.BUS]
        base_time = datetime(2024, 1, 15, 8, 0, 0)
        
        for i in range(self.num_records):
            self._generated += 1
            
            sensor_id = random.choice(sensors)
            timestamp = base_time + timedelta(minutes=i)
            
            if i >= self.surge_start:
                base_count = 50
                vehicle_count = int(base_count * self.surge_multiplier)
            else:
                vehicle_count = random.randint(10, 100)
            
            if sensor_id == "SENSOR_FAULT" or random.random() < self.error_rate:
                vehicle_count = -1 if random.random() < 0.5 else 99999
            
            speed = random.uniform(20, 80)
            vehicle_type = random.choice(vehicle_types)
            
            try:
                record = TrafficRecord(
                    sensor_id=sensor_id,
                    timestamp=timestamp,
                    vehicle_count=vehicle_count,
                    vehicle_type=vehicle_type,
                    speed=speed,
                )
                yield CleaningResult(record=record, is_valid=True)
            except ValueError as e:
                yield CleaningResult(
                    record=None,
                    is_valid=False,
                    error_message=str(e),
                    original_data={
                        "sensor_id": sensor_id,
                        "vehicle_count": vehicle_count,
                    },
                )
    
    def close(self) -> None:
        pass


from datetime import timedelta
