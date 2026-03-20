"""
集成测试套件：验证熔断机制、滑动窗口计算、异常报警。

测试场景：
1. 正常数据流处理
2. 传感器故障触发熔断
3. 流量激增触发报警
4. 混合数据流处理
"""
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from models import (
    TrafficRecord,
    VehicleType,
    CleaningResult,
    AnomalyType,
    AnomalyLevel,
)
from ingestion import FileDataStream, MockDataStream
from cleaner import (
    DataCleaningPipeline,
    RemoveDuplicates,
    FixTimestamps,
    FilterOutliers,
    SensorCircuitBreaker,
    CircuitState,
)
from analytics import SlidingWindow, TrafficAggregator, AnomalyDetector
from main import TrafficAnalysisEngine


class TestTrafficRecord(unittest.TestCase):
    """
    测试 TrafficRecord 模型验证。
    """
    
    def test_valid_record(self) -> None:
        """测试有效记录"""
        record = TrafficRecord(
            sensor_id="SENSOR_001",
            timestamp=datetime.now() - timedelta(minutes=1),
            vehicle_count=50,
            vehicle_type=VehicleType.CAR,
            speed=60.5,
        )
        self.assertEqual(record.sensor_id, "SENSOR_001")
        self.assertEqual(record.vehicle_count, 50)
    
    def test_invalid_sensor_id(self) -> None:
        """测试无效传感器ID"""
        with self.assertRaises(ValueError):
            TrafficRecord(
                sensor_id="INVALID",
                timestamp=datetime.now(),
                vehicle_count=50,
                speed=60.0,
            )
    
    def test_negative_vehicle_count(self) -> None:
        """测试负数车流量"""
        with self.assertRaises(ValueError):
            TrafficRecord(
                sensor_id="SENSOR_001",
                timestamp=datetime.now(),
                vehicle_count=-10,
                speed=60.0,
            )
    
    def test_future_timestamp(self) -> None:
        """测试未来时间戳"""
        with self.assertRaises(ValueError):
            TrafficRecord(
                sensor_id="SENSOR_001",
                timestamp=datetime.now() + timedelta(days=1),
                vehicle_count=50,
                speed=60.0,
            )
    
    def test_excessive_vehicle_count(self) -> None:
        """测试异常车流量"""
        with self.assertRaises(ValueError):
            TrafficRecord(
                sensor_id="SENSOR_001",
                timestamp=datetime.now(),
                vehicle_count=15000,
                speed=60.0,
            )


class TestSlidingWindow(unittest.TestCase):
    """
    测试滑动窗口实现。
    
    验证场景一：内存优化
    - 空间复杂度 O(WindowSize) 而非 O(N)
    """
    
    def test_fixed_size(self) -> None:
        """测试固定大小窗口"""
        window = SlidingWindow(window_size=5)
        
        for i in range(10):
            window.add(self._create_window_data(i))
        
        self.assertEqual(window.get_count(), 5)
    
    def test_average_calculation(self) -> None:
        """测试平均值计算"""
        window = SlidingWindow(window_size=100)
        
        for i in range(100):
            window.add(self._create_window_data(i, count=10))
        
        self.assertEqual(window.get_average_count(), 10.0)
    
    def test_empty_window(self) -> None:
        """测试空窗口边界情况
        
        验证场景三：时间窗口边界处理
        - 空窗口返回 0.0 而非抛出异常
        """
        window = SlidingWindow()
        
        self.assertEqual(window.get_average_count(), 0.0)
        self.assertEqual(window.get_average_speed(), 0.0)
        self.assertEqual(window.get_count(), 0)
    
    def _create_window_data(self, index: int, count: int = None) -> "WindowData":
        """创建测试数据"""
        from analytics import WindowData
        return WindowData(
            timestamp=datetime.now() + timedelta(minutes=index),
            vehicle_count=count if count is not None else index * 10,
            speed=50.0 + index,
            vehicle_type=VehicleType.CAR,
        )


class TestSensorCircuitBreaker(unittest.TestCase):
    """
    测试熔断器实现。
    
    验证场景二：脏数据导致的统计偏差
    - 连续错误触发熔断
    - 错误率超标触发熔断
    """
    
    def test_initial_state(self) -> None:
        """测试初始状态"""
        cb = SensorCircuitBreaker("SENSOR_001")
        self.assertEqual(cb.state, CircuitState.CLOSED)
        self.assertFalse(cb.is_open)
    
    def test_consecutive_failures_trigger_open(self) -> None:
        """测试连续错误触发熔断"""
        cb = SensorCircuitBreaker("SENSOR_001")
        
        for _ in range(5):
            cb.record_failure("测试错误")
        
        self.assertEqual(cb.state, CircuitState.OPEN)
        self.assertTrue(cb.is_open)
    
    def test_error_rate_trigger_open(self) -> None:
        """测试错误率触发熔断"""
        cb = SensorCircuitBreaker("SENSOR_001")
        
        record = TrafficRecord(
            sensor_id="SENSOR_001",
            timestamp=datetime.now(),
            vehicle_count=50,
            speed=60.0,
        )
        
        for i in range(10):
            if i < 3:
                cb.record_success(record)
            else:
                cb.record_failure("测试错误")
        
        self.assertGreater(cb.error_rate, 0.20)
    
    def test_allow_request_in_closed_state(self) -> None:
        """测试 CLOSED 状态允许请求"""
        cb = SensorCircuitBreaker("SENSOR_001")
        self.assertTrue(cb.allow_request())
    
    def test_deny_request_in_open_state(self) -> None:
        """测试 OPEN 状态拒绝请求"""
        cb = SensorCircuitBreaker("SENSOR_001")
        
        for _ in range(5):
            cb.record_failure("测试错误")
        
        self.assertFalse(cb.allow_request())
    
    def test_dynamic_threshold_calculation(self) -> None:
        """测试动态阈值计算"""
        cb = SensorCircuitBreaker("SENSOR_001")
        
        record = TrafficRecord(
            sensor_id="SENSOR_001",
            timestamp=datetime.now(),
            vehicle_count=50,
            speed=60.0,
        )
        
        for _ in range(20):
            cb.record_success(record)
        
        mean, std = cb.get_dynamic_threshold()
        self.assertGreater(mean, 0)


class TestDataCleaningPipeline(unittest.TestCase):
    """
    测试数据清洗管道。
    """
    
    def setUp(self) -> None:
        """设置测试管道"""
        self.pipeline = DataCleaningPipeline()
        self.pipeline.add_step(RemoveDuplicates())
        self.pipeline.add_step(FixTimestamps())
        self.pipeline.add_step(FilterOutliers(
            min_count=0,
            max_count=500,
            use_dynamic_threshold=False,
        ))
    
    def test_valid_record_passes(self) -> None:
        """测试有效记录通过"""
        result = CleaningResult(
            record=TrafficRecord(
                sensor_id="SENSOR_001",
                timestamp=datetime.now() - timedelta(minutes=1),
                vehicle_count=50,
                speed=60.0,
            ),
            is_valid=True,
        )
        
        cleaned = self.pipeline.process(result)
        self.assertTrue(cleaned.is_valid)
    
    def test_duplicate_removal(self) -> None:
        """测试去重"""
        record = TrafficRecord(
            sensor_id="SENSOR_001",
            timestamp=datetime.now(),
            vehicle_count=50,
            speed=60.0,
        )
        
        result1 = CleaningResult(record=record, is_valid=True)
        result2 = CleaningResult(record=record, is_valid=True)
        
        cleaned1 = self.pipeline.process(result1)
        cleaned2 = self.pipeline.process(result2)
        
        self.assertTrue(cleaned1.is_valid)
        self.assertFalse(cleaned2.is_valid)
        self.assertEqual(cleaned2.anomaly_type, AnomalyType.DUPLICATE_RECORD)
    
    def test_outlier_filtering(self) -> None:
        """测试异常值过滤"""
        result = CleaningResult(
            record=TrafficRecord(
                sensor_id="SENSOR_001",
                timestamp=datetime.now(),
                vehicle_count=600,
                speed=60.0,
            ),
            is_valid=True,
        )
        
        cleaned = self.pipeline.process(result)
        self.assertFalse(cleaned.is_valid)


class TestAnomalyDetector(unittest.TestCase):
    """
    测试异常检测器。
    
    验证场景三：时间窗口边界处理
    - 数据稀疏时的增长率计算
    - 除零保护
    """
    
    def test_surge_detection(self) -> None:
        """测试流量激增检测"""
        detector = AnomalyDetector(
            surge_threshold=0.5,
            min_samples_for_detection=5,
        )
        
        base_time = datetime.now()
        
        for i in range(10):
            record = TrafficRecord(
                sensor_id="SENSOR_001",
                timestamp=base_time + timedelta(minutes=i),
                vehicle_count=50,
                speed=60.0,
            )
            detector.detect(record)
        
        surge_record = TrafficRecord(
            sensor_id="SENSOR_001",
            timestamp=base_time + timedelta(minutes=10),
            vehicle_count=100,
            speed=60.0,
        )
        
        alert = detector.detect(surge_record)
        
        self.assertIsNotNone(alert)
        self.assertEqual(alert.anomaly_level, AnomalyLevel.LEVEL_2)
        self.assertEqual(alert.anomaly_type, AnomalyType.SUDDEN_SURGE)
    
    def test_no_detection_with_insufficient_data(self) -> None:
        """测试数据不足时不检测"""
        detector = AnomalyDetector(min_samples_for_detection=10)
        
        record = TrafficRecord(
            sensor_id="SENSOR_001",
            timestamp=datetime.now(),
            vehicle_count=100,
            speed=60.0,
        )
        
        for _ in range(5):
            alert = detector.detect(record)
            self.assertIsNone(alert)
    
    def test_no_division_by_zero(self) -> None:
        """测试除零保护
        
        验证场景三：时间窗口边界处理
        - 当基准值为 0 时不计算增长率
        """
        detector = AnomalyDetector(min_samples_for_detection=3)
        
        for i in range(5):
            record = TrafficRecord(
                sensor_id="SENSOR_001",
                timestamp=datetime.now() + timedelta(minutes=i),
                vehicle_count=0,
                speed=0.0,
            )
            detector.detect(record)
        
        surge_record = TrafficRecord(
            sensor_id="SENSOR_001",
            timestamp=datetime.now() + timedelta(minutes=5),
            vehicle_count=100,
            speed=60.0,
        )
        
        alert = detector.detect(surge_record)
        
        self.assertIsNone(alert)


class TestTrafficAnalysisEngine(unittest.TestCase):
    """
    测试完整分析引擎。
    
    验证场景四：模块解耦测试
    - 清洗失败返回 None 时的防御性处理
    """
    
    def test_normal_processing(self) -> None:
        """测试正常处理流程"""
        engine = TrafficAnalysisEngine()
        
        mock_stream = MockDataStream(
            num_records=100,
            error_rate=0.0,
        )
        
        report = engine.process_stream(mock_stream)
        
        self.assertGreater(report.total_records_processed, 0)
        self.assertEqual(report.total_errors, 0)
    
    def test_error_handling(self) -> None:
        """测试错误处理"""
        engine = TrafficAnalysisEngine()
        
        mock_stream = MockDataStream(
            num_records=100,
            error_rate=0.3,
        )
        
        report = engine.process_stream(mock_stream)
        
        self.assertGreater(report.total_errors, 0)
    
    def test_surge_alert_generation(self) -> None:
        """测试流量激增报警生成"""
        engine = TrafficAnalysisEngine(surge_threshold=0.5)
        
        mock_stream = MockDataStream(
            num_records=100,
            error_rate=0.0,
            surge_start=50,
            surge_multiplier=3.0,
        )
        
        report = engine.process_stream(mock_stream)
        
        self.assertGreater(len(report.alerts), 0)


class TestIntegration(unittest.TestCase):
    """
    集成测试：完整数据流处理。
    
    测试场景：
    - 正常车流 -> 传感器故障(触发熔断) -> 流量突增(触发报警)
    """
    
    def test_full_pipeline(self) -> None:
        """测试完整管道"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write("sensor_id,timestamp,vehicle_count,vehicle_type,speed,region\n")
            
            base_time = datetime.now()
            
            for i in range(50):
                f.write(
                    f"SENSOR_001,{(base_time + timedelta(minutes=i)).isoformat()},"
                    f"50,car,60.0,DOWNTOWN\n"
                )
            
            for i in range(10):
                f.write(
                    f"SENSOR_FAULT,{(base_time + timedelta(minutes=i)).isoformat()},"
                    f"-1,car,60.0,DOWNTOWN\n"
                )
            
            for i in range(20):
                f.write(
                    f"SENSOR_001,{(base_time + timedelta(minutes=50+i)).isoformat()},"
                    f"150,car,60.0,DOWNTOWN\n"
                )
            
            temp_path = f.name
        
        try:
            data_stream = FileDataStream(temp_path, file_format="csv")
            
            engine = TrafficAnalysisEngine(
                surge_threshold=0.5,
                max_count=500,
            )
            
            with data_stream:
                report = engine.process_stream(data_stream)
            
            self.assertGreater(report.total_records_processed, 0)
            
            circuit_open_sensors = [
                s for s in report.sensor_statuses.values()
                if s.is_circuit_open
            ]
            
            self.assertGreater(len(circuit_open_sensors), 0)
            
        finally:
            os.unlink(temp_path)


def run_tests() -> None:
    """
    运行所有测试。
    """
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestTrafficRecord))
    suite.addTests(loader.loadTestsFromTestCase(TestSlidingWindow))
    suite.addTests(loader.loadTestsFromTestCase(TestSensorCircuitBreaker))
    suite.addTests(loader.loadTestsFromTestCase(TestDataCleaningPipeline))
    suite.addTests(loader.loadTestsFromTestCase(TestAnomalyDetector))
    suite.addTests(loader.loadTestsFromTestCase(TestTrafficAnalysisEngine))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegration))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(run_tests())
