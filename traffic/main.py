"""
程序入口：组装各模块，协调数据流。

数据流处理流程：
Stream -> Clean -> Aggregate -> Report

解决场景四：模块解耦测试
- 使用 Optional 类型处理可能为空的清洗结果
- 增加空值防御性编程
"""
import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Any

from ingestion import DataStream, FileDataStream
from cleaner import (
    DataCleaningPipeline,
    RemoveDuplicates,
    FixTimestamps,
    FilterOutliers,
)
from analytics import TrafficAggregator
from models import TrafficRecord, TrafficReport, Alert, SensorStatus

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("traffic_analysis.log", encoding="utf-8"),
    ],
)

logger = logging.getLogger(__name__)


class TrafficAnalysisEngine:
    """
    交通流量分析引擎。
    
    协调数据流：Stream -> Clean -> Aggregate -> Report
    """
    
    def __init__(
        self,
        window_size: int = 1000,
        time_window_minutes: int = 15,
        surge_threshold: float = 0.5,
        min_count: int = 0,
        max_count: int = 500,
    ):
        """
        初始化分析引擎。
        
        Args:
            window_size: 滑动窗口大小
            time_window_minutes: 时间窗口大小（分钟）
            surge_threshold: 激增阈值
            min_count: 最小车流量阈值
            max_count: 最大车流量阈值
        """
        self.pipeline = DataCleaningPipeline()
        self.pipeline.add_step(RemoveDuplicates())
        self.pipeline.add_step(FixTimestamps())
        self.pipeline.add_step(FilterOutliers(
            min_count=min_count,
            max_count=max_count,
            use_dynamic_threshold=True,
        ))
        
        self.aggregator = TrafficAggregator(
            window_size=window_size,
            time_window_minutes=time_window_minutes,
            surge_threshold=surge_threshold,
        )
        
        self._start_time: Optional[datetime] = None
        self._end_time: Optional[datetime] = None
        self._processed_count = 0
        self._error_count = 0
    
    def process_stream(self, data_stream: DataStream) -> TrafficReport:
        """
        处理数据流。
        
        Args:
            data_stream: 数据流对象
            
        Returns:
            TrafficReport: 分析报告
        """
        self._start_time = datetime.now()
        logger.info("开始处理数据流...")
        
        alerts: list[Alert] = []
        sensor_statuses: dict[str, SensorStatus] = {}
        
        for result in data_stream.stream():
            cleaned = self.pipeline.process(result)
            
            if not cleaned.is_valid:
                self._error_count += 1
                if cleaned.error_message:
                    logger.debug(f"数据清洗失败: {cleaned.error_message}")
                continue
            
            if cleaned.record is None:
                continue
            
            record = cleaned.record
            
            self._update_sensor_status(
                sensor_statuses, record.sensor_id, success=True
            )
            
            aggregation, alert = self.aggregator.process(record)
            
            if alert:
                alerts.append(alert)
                self._log_alert(alert)
            
            self._processed_count += 1
            
            if self._processed_count % 1000 == 0:
                logger.info(f"已处理 {self._processed_count} 条记录")
        
        self._end_time = datetime.now()
        
        self._collect_circuit_breaker_statuses(sensor_statuses)
        
        report = self._generate_report(alerts, sensor_statuses)
        
        logger.info(
            f"数据处理完成: 总计 {self._processed_count} 条, "
            f"错误 {self._error_count} 条"
        )
        
        return report
    
    def _update_sensor_status(
        self,
        statuses: dict[str, SensorStatus],
        sensor_id: str,
        success: bool,
    ) -> None:
        """
        更新传感器状态。
        """
        if sensor_id not in statuses:
            statuses[sensor_id] = SensorStatus(sensor_id=sensor_id)
        
        status = statuses[sensor_id]
        status.total_records += 1
        status.last_seen = datetime.now()
        
        if not success:
            status.error_count += 1
    
    def _collect_circuit_breaker_statuses(
        self, statuses: dict[str, SensorStatus]
    ) -> None:
        """
        收集熔断器状态。
        """
        filter_outliers = self.pipeline.get_filter_outliers()
        if not filter_outliers:
            return
        
        cb_stats = filter_outliers.get_all_circuit_breaker_stats()
        
        for sensor_id, stats in cb_stats.items():
            if sensor_id not in statuses:
                statuses[sensor_id] = SensorStatus(sensor_id=sensor_id)
            
            status = statuses[sensor_id]
            status.is_circuit_open = stats.get("is_circuit_open", False)
            status.error_count = stats.get("error_count", 0)
            status.total_records = stats.get("total_count", 0)
            
            if status.is_circuit_open:
                logger.warning(f"传感器 [{sensor_id}] 已熔断")
    
    def _log_alert(self, alert: Alert) -> None:
        """
        记录报警日志。
        """
        level_map = {
            1: "WARNING",
            2: "ERROR",
        }
        level = level_map.get(alert.anomaly_level.value, "WARNING")
        
        log_msg = (
            f"[{level}] 传感器 [{alert.sensor_id}] "
            f"检测到 {alert.anomaly_type.value}: {alert.message}"
        )
        
        if alert.suggestion:
            log_msg += f" | 建议: {alert.suggestion}"
        
        if alert.anomaly_level.value == 2:
            logger.error(log_msg)
        else:
            logger.warning(log_msg)
    
    def _generate_report(
        self,
        alerts: list[Alert],
        sensor_statuses: dict[str, SensorStatus],
    ) -> TrafficReport:
        """
        生成分析报告。
        """
        aggregations = self.aggregator.get_time_aggregations()
        
        active_sensors = sum(1 for s in sensor_statuses.values() if s.is_active)
        circuit_open_sensors = sum(
            1 for s in sensor_statuses.values() if s.is_circuit_open
        )
        
        return TrafficReport(
            generated_at=datetime.now(),
            time_range=(
                self._start_time or datetime.now(),
                self._end_time or datetime.now(),
            ),
            total_records_processed=self._processed_count,
            total_errors=self._error_count,
            sensors_active=active_sensors,
            sensors_circuit_open=circuit_open_sensors,
            aggregations=aggregations,
            alerts=alerts,
            sensor_statuses=sensor_statuses,
        )


def save_report(report: TrafficReport, output_path: str) -> None:
    """
    保存报告到文件。
    
    Args:
        report: 分析报告
        output_path: 输出文件路径
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)
    
    logger.info(f"报告已保存: {output_file}")


def print_summary(report: TrafficReport) -> None:
    """
    打印摘要报告。
    """
    print("\n" + "=" * 70)
    print("                      交通流量分析报告摘要")
    print("=" * 70)
    
    print(f"\n📊 基本统计:")
    print(f"   - 处理记录数: {report.total_records_processed:,}")
    print(f"   - 错误记录数: {report.total_errors:,}")
    print(f"   - 活跃传感器: {report.sensors_active}")
    print(f"   - 熔断传感器: {report.sensors_circuit_open}")
    
    print(f"\n📈 时间窗口聚合:")
    for agg in report.aggregations[:5]:
        print(f"   - {agg.window_start.strftime('%H:%M')} ~ {agg.window_end.strftime('%H:%M')}: "
              f"车流 {agg.total_count}, 均速 {agg.avg_speed:.1f} km/h")
    
    if len(report.aggregations) > 5:
        print(f"   ... 共 {len(report.aggregations)} 个时间窗口")
    
    if report.alerts:
        print(f"\n⚠️  报警信息 ({len(report.alerts)} 条):")
        for alert in report.alerts[:5]:
            level_str = "L1" if alert.anomaly_level.value == 1 else "L2"
            print(f"   [{level_str}] {alert.sensor_id}: {alert.message}")
        
        if len(report.alerts) > 5:
            print(f"   ... 共 {len(report.alerts)} 条报警")
    
    circuit_open_sensors = [
        s for s in report.sensor_statuses.values() if s.is_circuit_open
    ]
    if circuit_open_sensors:
        print(f"\n🔌 已熔断传感器:")
        for sensor in circuit_open_sensors:
            print(f"   - {sensor.sensor_id}: 错误率 {sensor.error_rate:.1%}")
    
    print("\n" + "=" * 70)


def parse_args() -> argparse.Namespace:
    """
    解析命令行参数。
    """
    parser = argparse.ArgumentParser(
        description="交通流量实时分析引擎",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py data.csv --format csv --output report.json
  python main.py data.jsonl --format jsonl --surge-threshold 0.3
  python main.py data.csv --window-size 2000 --time-window 30
        """,
    )
    
    parser.add_argument(
        "input",
        type=str,
        help="输入数据文件路径",
    )
    
    parser.add_argument(
        "--format",
        type=str,
        choices=["csv", "jsonl"],
        default="csv",
        help="输入文件格式 (默认: csv)",
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default="output/report.json",
        help="输出报告路径 (默认: output/report.json)",
    )
    
    parser.add_argument(
        "--window-size",
        type=int,
        default=1000,
        help="滑动窗口大小 (默认: 1000)",
    )
    
    parser.add_argument(
        "--time-window",
        type=int,
        default=15,
        help="时间窗口大小（分钟） (默认: 15)",
    )
    
    parser.add_argument(
        "--surge-threshold",
        type=float,
        default=0.5,
        help="流量激增阈值 (默认: 0.5, 即50%%)",
    )
    
    parser.add_argument(
        "--min-count",
        type=int,
        default=0,
        help="最小车流量阈值 (默认: 0)",
    )
    
    parser.add_argument(
        "--max-count",
        type=int,
        default=500,
        help="最大车流量阈值 (默认: 500)",
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="显示详细日志",
    )
    
    return parser.parse_args()


def main() -> int:
    """
    主函数。
    """
    args = parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info(f"启动交通流量分析引擎")
    logger.info(f"输入文件: {args.input}")
    logger.info(f"文件格式: {args.format}")
    logger.info(f"滑动窗口: {args.window_size}")
    logger.info(f"时间窗口: {args.time_window} 分钟")
    logger.info(f"激增阈值: {args.surge_threshold:.0%}")
    
    try:
        data_stream = FileDataStream(
            file_path=args.input,
            file_format=args.format,
        )
        
        engine = TrafficAnalysisEngine(
            window_size=args.window_size,
            time_window_minutes=args.time_window,
            surge_threshold=args.surge_threshold,
            min_count=args.min_count,
            max_count=args.max_count,
        )
        
        with data_stream:
            report = engine.process_stream(data_stream)
        
        save_report(report, args.output)
        
        print_summary(report)
        
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"文件不存在: {e}")
        return 1
    except Exception as e:
        logger.exception(f"处理失败: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
