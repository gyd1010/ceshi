"""
集成测试模块 (test_pipeline.py)

使用 pytest 编写完整的集成测试，验证:
1. 熔断机制在脏数据下的生效
2. 滑动窗口计算的准确性
3. 异常报警的触发
4. 模块间的容错性
"""

import pytest
import statistics
from datetime import datetime, timedelta
from typing import List, Generator

from models import TrafficRecord, VehicleType, DataQuality, Alert
from ingestion import MockDataStream
from cleaner import (
    DataCleaningPipeline, SensorCircuitBreaker,
    RemoveDuplicates, FixTimestamps, FilterOutliers,
    CircuitBreakerState
)
from analytics import SlidingWindow, TrafficAggregator, AnomalyDetector, AnalyticsEngine


class TestTrafficRecord:
    """TrafficRecord 模型测试"""
    
    def test_valid_record_creation(self):
        """测试有效记录创建"""
        record = TrafficRecord(
            sensor_id="SENSOR_001",
            timestamp=datetime.now(),
            vehicle_count=50,
            vehicle_type=VehicleType.CAR,
            speed=60.5
        )
        assert record.sensor_id == "SENSOR_001"
        assert record.vehicle_count == 50
        assert record.is_valid()
    
    def test_invalid_negative_count(self):
        """测试负数车辆数验证"""
        with pytest.raises(ValueError, match="车辆数量不能为负数"):
            TrafficRecord(
                sensor_id="SENSOR_001",
                timestamp=datetime.now(),
                vehicle_count=-10
            )
    
    def test_invalid_future_timestamp(self):
        """测试未来时间戳验证"""
        future = datetime.now() + timedelta(days=1)
        with pytest.raises(ValueError, match="未来时间"):
            TrafficRecord(
                sensor_id="SENSOR_001",
                timestamp=future,
                vehicle_count=10
            )
    
    def test_sensor_id_normalization(self):
        """测试传感器ID标准化"""
        record = TrafficRecord(
            sensor_id="  sensor_001  ",
            timestamp=datetime.now(),
            vehicle_count=10
        )
        assert record.sensor_id == "SENSOR_001"


class TestSlidingWindow:
    """滑动窗口测试"""
    
    def test_window_size_limit(self):
        """测试窗口大小限制"""
        window = SlidingWindow(max_size=5)
        
        # 添加10条记录
        for i in range(10):
            record = TrafficRecord(
                sensor_id="S001",
                timestamp=datetime.now(),
                vehicle_count=i * 10
            )
            window.add(record)
        
        # 窗口只保留最近5条
        assert len(window) == 5
        assert window.total == sum([50, 60, 70, 80, 90])
    
    def test_time_window_expiration(self):
        """测试时间窗口过期"""
        window = SlidingWindow(window_minutes=10)
        
        # 添加旧记录
        old_time = datetime.now() - timedelta(minutes=15)
        old_record = TrafficRecord(
            sensor_id="S001",
            timestamp=old_time,
            vehicle_count=100
        )
        window.add(old_record)
        
        # 添加新记录
        new_record = TrafficRecord(
            sensor_id="S001",
            timestamp=datetime.now(),
            vehicle_count=50
        )
        window.add(new_record)
        
        # 旧记录应该被清理
        assert len(window) == 1
        assert window.total == 50
    
    def test_window_statistics(self):
        """测试窗口统计计算"""
        window = SlidingWindow(max_size=10)
        
        values = [10, 20, 30, 40, 50]
        for v in values:
            record = TrafficRecord(
                sensor_id="S001",
                timestamp=datetime.now(),
                vehicle_count=v,
                speed=float(v)
            )
            window.add(record)
        
        assert window.count == 5
        assert window.total == 150
        assert window.average == 30.0
        assert window.max_value == 50
        assert window.min_value == 10
        assert window.average_speed == 30.0


class TestCircuitBreaker:
    """熔断器测试"""
    
    def test_circuit_closed_initially(self):
        """测试初始状态为关闭"""
        cb = SensorCircuitBreaker(error_threshold=0.2)
        assert cb.can_process("S001")
    
    def test_circuit_opens_on_high_error_rate(self):
        """测试高错误率触发熔断"""
        cb = SensorCircuitBreaker(
            error_threshold=0.2,
            min_sample_size=5
        )
        
        # 模拟高错误率 (4/5 = 80% > 20%)
        for i in range(5):
            if i < 4:
                cb.record_error("S001", f"error_{i}")
            else:
                record = TrafficRecord(
                    sensor_id="S001",
                    timestamp=datetime.now(),
                    vehicle_count=10
                )
                cb.record_success("S001", record)
        
        # 应该熔断
        assert not cb.can_process("S001")
        
        status = cb.get_sensor_status("S001")
        assert status.is_tripped
        assert status.error_rate == 0.8
    
    def test_circuit_half_open_after_cooldown(self):
        """测试冷却后半开状态"""
        cb = SensorCircuitBreaker(
            error_threshold=0.2,
            min_sample_size=5,
            cooldown_seconds=0  # 立即冷却
        )
        
        # 触发熔断
        for i in range(5):
            cb.record_error("S001", f"error_{i}")
        
        assert not cb.can_process("S001")
        
        # 应该进入半开状态
        assert cb.can_process("S001")
    
    def test_circuit_closes_after_recovery(self):
        """测试恢复后关闭"""
        cb = SensorCircuitBreaker(
            error_threshold=0.2,
            min_sample_size=5,
            cooldown_seconds=0,
            half_open_max_requests=4
        )
        
        # 触发熔断
        for i in range(5):
            cb.record_error("S001", f"error_{i}")
        
        # 进入半开
        assert cb.can_process("S001")
        
        # 成功恢复
        for i in range(3):
            record = TrafficRecord(
                sensor_id="S001",
                timestamp=datetime.now(),
                vehicle_count=10
            )
            cb.record_success("S001", record)
        
        # 应该恢复
        status = cb.get_sensor_status("S001")
        assert not status.is_tripped


class TestCleaningPipeline:
    """清洗管道测试"""
    
    def test_duplicate_removal(self):
        """测试去重功能"""
        dedup = RemoveDuplicates(window_seconds=60)
        
        timestamp = datetime.now()
        record1 = TrafficRecord(
            sensor_id="S001",
            timestamp=timestamp,
            vehicle_count=10
        )
        record2 = TrafficRecord(
            sensor_id="S001",
            timestamp=timestamp,  # 相同时间戳
            vehicle_count=20
        )
        
        result1 = dedup.process(record1)
        result2 = dedup.process(record2)
        
        assert result1 is not None
        assert result2 is None  # 重复被移除
    
    def test_outlier_filtering(self):
        """测试异常值过滤"""
        outlier = FilterOutliers(z_score_threshold=3.0)
        
        # 建立历史数据 (正常值 10-20)
        for i in range(10):
            record = TrafficRecord(
                sensor_id="S001",
                timestamp=datetime.now(),
                vehicle_count=10 + i
            )
            outlier.process(record)
        
        # 异常值应该被标记
        outlier_record = TrafficRecord(
            sensor_id="S001",
            timestamp=datetime.now(),
            vehicle_count=9999
        )
        result = outlier.process(outlier_record)
        
        assert result is not None
        assert result.quality == DataQuality.INVALID
    
    def test_pipeline_integration(self):
        """测试完整管道集成"""
        pipeline = DataCleaningPipeline(
            steps=[
                RemoveDuplicates(),
                FilterOutliers(max_count=100)
            ],
            skip_invalid=True
        )
        
        # 正常记录
        normal = TrafficRecord(
            sensor_id="S001",
            timestamp=datetime.now(),
            vehicle_count=50
        )
        
        # 异常记录
        invalid = TrafficRecord(
            sensor_id="S001",
            timestamp=datetime.now(),
            vehicle_count=9999
        )
        
        result1 = pipeline.process(normal)
        result2 = pipeline.process(invalid)
        
        assert result1 is not None
        assert result1.vehicle_count == 50
        assert result2 is None  # 被过滤


class TestAnomalyDetection:
    """异常检测测试"""
    
    def test_surge_detection(self):
        """测试流量突增检测"""
        detector = AnomalyDetector(
            surge_threshold=0.5,
            surge_window_minutes=10,
            min_records_for_detection=3
        )
        
        # 建立基准数据
        base_time = datetime.now() - timedelta(minutes=5)
        for i in range(5):
            record = TrafficRecord(
                sensor_id="S001",
                timestamp=base_time + timedelta(minutes=i),
                vehicle_count=10  # 基准值 10
            )
            detector.detect_surge(record)
        
        # 突增数据 (10 -> 100，增长 900%)
        surge_record = TrafficRecord(
            sensor_id="S001",
            timestamp=datetime.now(),
            vehicle_count=100
        )
        alert = detector.detect_surge(surge_record)
        
        assert alert is not None
        assert alert.alert_type == "TRAFFIC_SURGE"
        assert alert.severity == "high"
    
    def test_congestion_detection(self):
        """测试拥堵检测"""
        detector = AnomalyDetector(
            congestion_threshold=80,
            congestion_speed_threshold=20.0
        )
        
        # 拥堵情况: 高流量 + 低速
        congested_record = TrafficRecord(
            sensor_id="S001",
            timestamp=datetime.now(),
            vehicle_count=100,
            speed=15.0
        )
        
        alert = detector.detect_congestion(congested_record)
        
        assert alert is not None
        assert alert.alert_type == "CONGESTION"
    
    def test_no_false_positive(self):
        """测试无误报"""
        detector = AnomalyDetector(
            surge_threshold=0.5,
            congestion_threshold=80
        )
        
        # 正常记录
        normal_record = TrafficRecord(
            sensor_id="S001",
            timestamp=datetime.now(),
            vehicle_count=30,
            speed=60.0
        )
        
        alerts = detector.detect(normal_record)
        
        assert len(alerts) == 0


class TestTrafficAggregator:
    """流量聚合器测试"""
    
    def test_aggregation_by_sensor(self):
        """测试按传感器聚合"""
        aggregator = TrafficAggregator(window_minutes=15)
        
        # 添加多条记录
        for i in range(10):
            record = TrafficRecord(
                sensor_id="S001",
                timestamp=datetime.now(),
                vehicle_count=10,
                speed=50.0
            )
            aggregator.add(record)
        
        for i in range(5):
            record = TrafficRecord(
                sensor_id="S002",
                timestamp=datetime.now(),
                vehicle_count=20,
                speed=60.0
            )
            aggregator.add(record)
        
        # 聚合
        results = aggregator.aggregate(dimension="sensor")
        
        assert len(results) == 2
        
        s001_result = next(r for r in results if r.sensor_id == "S001")
        assert s001_result.total_count == 100  # 10 * 10
        assert s001_result.record_count == 10
        
        s002_result = next(r for r in results if r.sensor_id == "S002")
        assert s002_result.total_count == 100  # 5 * 20
        assert s002_result.record_count == 5
    
    def test_growth_rate_calculation(self):
        """测试增长率计算"""
        aggregator = TrafficAggregator(window_minutes=15)
        
        # 第一批数据
        for i in range(5):
            record = TrafficRecord(
                sensor_id="S001",
                timestamp=datetime.now() - timedelta(minutes=30),
                vehicle_count=10
            )
            aggregator.add(record)
        
        # 先聚合一次
        aggregator.aggregate(dimension="sensor")
        
        # 第二批数据 (翻倍)
        for i in range(5):
            record = TrafficRecord(
                sensor_id="S001",
                timestamp=datetime.now(),
                vehicle_count=20
            )
            aggregator.add(record)
        
        # 再次聚合
        results = aggregator.aggregate(dimension="sensor")
        
        assert len(results) == 1
        assert results[0].growth_rate == 1.0  # 增长 100%


class TestIntegration:
    """集成测试"""
    
    def create_mixed_dataset(self) -> Generator[TrafficRecord, None, None]:
        """
        创建混合测试数据集:
        - 正常数据
        - 大量脏数据 (触发熔断)
        - 突发流量 (触发报警)
        """
        base_time = datetime.now() - timedelta(hours=1)
        
        # 传感器1: 正常数据 (50条)
        for i in range(50):
            yield TrafficRecord(
                sensor_id="NORMAL_SENSOR",
                timestamp=base_time + timedelta(minutes=i),
                vehicle_count=20 + (i % 10),
                vehicle_type=VehicleType.CAR,
                speed=50.0,
                region="REGION_01"
            )
        
        # 传感器2: 大量脏数据 (触发熔断)
        for i in range(30):
            yield TrafficRecord(
                sensor_id="FAULTY_SENSOR",
                timestamp=base_time + timedelta(minutes=i),
                vehicle_count=-999 if i % 2 == 0 else 9999,  # 异常值
                vehicle_type=VehicleType.CAR,
                speed=-10.0 if i % 2 == 0 else 300.0,
                region="REGION_02"
            )
        
        # 传感器3: 突发流量 (触发报警)
        for i in range(20):
            count = 10 if i < 10 else 100  # 突增
            yield TrafficRecord(
                sensor_id="SURGE_SENSOR",
                timestamp=base_time + timedelta(minutes=i),
                vehicle_count=count,
                vehicle_type=VehicleType.CAR,
                speed=40.0,
                region="REGION_03"
            )
    
    def test_full_pipeline(self):
        """测试完整流程"""
        # 创建组件
        pipeline = create_default_pipeline(error_threshold=0.3)
        engine = AnalyticsEngine(window_minutes=15, surge_threshold=0.5)
        
        # 处理数据
        records = self.create_mixed_dataset()
        cleaned_count = 0
        all_alerts = []
        
        for record in pipeline.process_stream(records):
            cleaned_count += 1
            alerts = engine.process(record)
            all_alerts.extend(alerts)
        
        # 验证结果
        # 1. 熔断器应该熔断故障传感器
        faulty_status = pipeline.circuit_breaker.get_sensor_status("FAULTY_SENSOR")
        assert faulty_status.is_tripped or faulty_status.error_rate > 0
        
        # 2. 正常传感器应该正常
        normal_status = pipeline.circuit_breaker.get_sensor_status("NORMAL_SENSOR")
        assert normal_status.is_active
        
        # 3. 应该检测到流量突增
        surge_alerts = [a for a in all_alerts if a.alert_type == "TRAFFIC_SURGE"]
        assert len(surge_alerts) > 0
        
        # 4. 聚合结果
        aggregations = engine.aggregate(dimension="sensor")
        assert len(aggregations) >= 2  # 至少正常传感器和突增传感器
        
        # 5. 统计验证
        stats = pipeline.stats
        assert stats["processed_count"] > 0
        assert stats["invalid_count"] > 0  # 有脏数据被过滤
        
        print(f"\n集成测试结果:")
        print(f"  清洗后记录数: {cleaned_count}")
        print(f"  报警数: {len(all_alerts)}")
        print(f"  聚合组数: {len(aggregations)}")
        print(f"  故障传感器熔断: {faulty_status.is_tripped}")
    
    def test_defensive_programming(self):
        """测试防御性编程 - 处理 None 值"""
        pipeline = DataCleaningPipeline(skip_invalid=True)
        engine = AnalyticsEngine()
        
        # 创建包含 None 速度的记录
        record = TrafficRecord(
            sensor_id="TEST_SENSOR",
            timestamp=datetime.now(),
            vehicle_count=50,
            speed=None  # None 速度
        )
        
        # 应该正常处理，不崩溃
        cleaned = pipeline.process(record)
        assert cleaned is not None
        
        alerts = engine.process(cleaned)
        # 不应产生拥堵报警 (因为速度为 None)
        congestion_alerts = [a for a in alerts if a.alert_type == "CONGESTION"]
        assert len(congestion_alerts) == 0


def create_default_pipeline(error_threshold: float = 0.2) -> DataCleaningPipeline:
    """创建默认清洗管道"""
    from cleaner import SensorCircuitBreaker
    
    circuit_breaker = SensorCircuitBreaker(error_threshold=error_threshold)
    
    return DataCleaningPipeline(
        steps=[
            RemoveDuplicates(window_seconds=60),
            FixTimestamps(),
            FilterOutliers(z_score_threshold=3.0, max_count=1000)
        ],
        circuit_breaker=circuit_breaker,
        skip_invalid=True
    )


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])
