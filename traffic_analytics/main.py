"""
交通流量分析系统主入口 (main.py)

程序入口，协调各模块完成数据流处理:
Stream -> Clean -> Aggregate -> Report

支持命令行参数配置，灵活调整处理逻辑。
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('traffic_analytics.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

from models import TrafficRecord, AggregationResult, Alert
from ingestion import create_data_stream, DataStream
from cleaner import create_default_pipeline, DataCleaningPipeline
from analytics import AnalyticsEngine


class ReportGenerator:
    """报告生成器"""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def save_aggregations(
        self,
        aggregations: List[AggregationResult],
        filename: str = "aggregations.json"
    ) -> Path:
        """保存聚合结果"""
        output_path = self.output_dir / filename
        data = [agg.to_dict() for agg in aggregations]
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"聚合结果已保存: {output_path}")
        return output_path
    
    def save_alerts(
        self,
        alerts: List[Alert],
        filename: str = "alerts.json"
    ) -> Path:
        """保存报警信息"""
        output_path = self.output_dir / filename
        data = [alert.to_dict() for alert in alerts]
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"报警信息已保存: {output_path}")
        return output_path
    
    def generate_text_report(
        self,
        aggregations: List[AggregationResult],
        alerts: List[Alert],
        pipeline_stats: Dict[str, Any],
        filename: str = "report.txt"
    ) -> Path:
        """生成文本报告"""
        output_path = self.output_dir / filename
        
        lines = []
        lines.append("=" * 60)
        lines.append("交通流量分析报告")
        lines.append(f"生成时间: {datetime.now().isoformat()}")
        lines.append("=" * 60)
        lines.append("")
        
        # 处理统计
        lines.append("【处理统计】")
        lines.append(f"  总处理记录: {pipeline_stats.get('processed_count', 0)}")
        lines.append(f"  有效记录: {pipeline_stats.get('cleaned_count', 0)}")
        lines.append(f"  无效记录: {pipeline_stats.get('invalid_count', 0)}")
        lines.append(f"  熔断跳过: {pipeline_stats.get('tripped_count', 0)}")
        lines.append("")
        
        # 传感器状态
        cb_stats = pipeline_stats.get('circuit_breaker', {})
        lines.append("【传感器状态】")
        lines.append(f"  总传感器数: {cb_stats.get('total_sensors', 0)}")
        lines.append(f"  熔断传感器: {cb_stats.get('tripped_sensors', 0)}")
        lines.append("")
        
        # 聚合结果摘要
        lines.append("【聚合结果摘要】")
        if aggregations:
            total_vehicles = sum(agg.total_count for agg in aggregations)
            lines.append(f"  聚合组数: {len(aggregations)}")
            lines.append(f"  总车辆数: {total_vehicles}")
            
            # 按传感器显示Top流量
            by_sensor = [agg for agg in aggregations if agg.sensor_id]
            if by_sensor:
                lines.append("  传感器流量Top5:")
                sorted_by_count = sorted(by_sensor, key=lambda x: x.total_count, reverse=True)[:5]
                for i, agg in enumerate(sorted_by_count, 1):
                    lines.append(f"    {i}. {agg.sensor_id}: {agg.total_count} 辆")
        else:
            lines.append("  无聚合数据")
        lines.append("")
        
        # 报警信息
        lines.append("【报警信息】")
        if alerts:
            lines.append(f"  报警总数: {len(alerts)}")
            
            # 按类型分组
            by_type: Dict[str, List[Alert]] = {}
            for alert in alerts:
                by_type.setdefault(alert.alert_type, []).append(alert)
            
            for alert_type, type_alerts in by_type.items():
                lines.append(f"  {alert_type}: {len(type_alerts)} 次")
                for alert in type_alerts[:3]:  # 每类显示前3条
                    lines.append(f"    - [{alert.severity}] {alert.message}")
                    if alert.suggested_action:
                        lines.append(f"      建议: {alert.suggested_action}")
        else:
            lines.append("  无报警")
        lines.append("")
        
        lines.append("=" * 60)
        lines.append("报告生成完成")
        lines.append("=" * 60)
        
        # 写入文件
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        
        logger.info(f"文本报告已保存: {output_path}")
        return output_path


class TrafficAnalyticsApp:
    """
    交通流量分析应用
    
    协调数据流、清洗、分析和报告的完整流程。
    """
    
    def __init__(
        self,
        input_source: str,
        stream_type: str = "auto",
        window_minutes: int = 15,
        surge_threshold: float = 0.5,
        error_threshold: float = 0.2,
        output_dir: str = "output",
        batch_size: int = 1000
    ):
        """
        初始化应用
        
        Args:
            input_source: 输入数据源
            stream_type: 流类型 (file/mock/auto)
            window_minutes: 聚合窗口大小(分钟)
            surge_threshold: 突增检测阈值
            error_threshold: 熔断器错误率阈值
            output_dir: 输出目录
            batch_size: 批处理大小
        """
        self.input_source = input_source
        self.stream_type = stream_type
        self.window_minutes = window_minutes
        self.batch_size = batch_size
        
        # 初始化组件
        logger.info("初始化数据清洗管道...")
        self.pipeline = create_default_pipeline(error_threshold=error_threshold)
        
        logger.info("初始化分析引擎...")
        self.engine = AnalyticsEngine(
            window_minutes=window_minutes,
            surge_threshold=surge_threshold
        )
        
        logger.info("初始化报告生成器...")
        self.reporter = ReportGenerator(output_dir=output_dir)
        
        # 统计
        self._processed_count = 0
        self._all_alerts: List[Alert] = []
    
    def run(self) -> Dict[str, Any]:
        """
        运行分析流程
        
        Returns:
            运行结果统计
        """
        logger.info("=" * 60)
        logger.info("交通流量分析系统启动")
        logger.info(f"输入源: {self.input_source}")
        logger.info(f"聚合窗口: {self.window_minutes} 分钟")
        logger.info("=" * 60)
        
        # 创建数据流
        stream = create_data_stream(
            self.input_source,
            stream_type=self.stream_type
        )
        
        # 使用上下文管理器确保资源释放
        with stream:
            # 处理数据流
            self._process_stream(stream)
        
        # 生成最终聚合
        logger.info("生成最终聚合结果...")
        aggregations = self.engine.aggregate(dimension="sensor")
        
        # 收集所有报警
        alerts = self.engine.get_alerts()
        
        # 生成报告
        logger.info("生成报告...")
        self._generate_reports(aggregations, alerts)
        
        # 打印熔断状态
        self._print_circuit_breaker_status()
        
        # 返回统计
        return {
            "processed_count": self._processed_count,
            "aggregation_count": len(aggregations),
            "alert_count": len(alerts),
            "pipeline_stats": self.pipeline.stats
        }
    
    def _process_stream(self, stream: DataStream) -> None:
        """
        处理数据流
        
        Args:
            stream: 数据流
        """
        logger.info("开始处理数据流...")
        
        # 获取清洗后的记录流
        cleaned_stream = self.pipeline.process_stream(stream.records())
        
        batch_count = 0
        for record in cleaned_stream:
            self._processed_count += 1
            batch_count += 1
            
            # 异常检测和分析
            alerts = self.engine.process(record)
            self._all_alerts.extend(alerts)
            
            # 实时打印报警
            for alert in alerts:
                logger.warning(f"[ALERT] {alert.alert_type}: {alert.message}")
            
            # 批处理进度
            if batch_count >= self.batch_size:
                logger.info(f"已处理 {self._processed_count} 条记录...")
                batch_count = 0
        
        logger.info(f"数据流处理完成，共处理 {self._processed_count} 条记录")
    
    def _generate_reports(
        self,
        aggregations: List[AggregationResult],
        alerts: List[Alert]
    ) -> None:
        """生成报告"""
        # 保存聚合结果
        self.reporter.save_aggregations(aggregations)
        
        # 保存报警
        self.reporter.save_alerts(alerts)
        
        # 生成文本报告
        self.reporter.generate_text_report(
            aggregations,
            alerts,
            self.pipeline.stats
        )
    
    def _print_circuit_breaker_status(self) -> None:
        """打印熔断器状态"""
        status_list = self.pipeline.circuit_breaker.get_all_sensor_status()
        tripped = [s for s in status_list if s.is_tripped]
        
        if tripped:
            logger.warning("=" * 60)
            logger.warning("以下传感器已熔断:")
            for status in tripped:
                logger.warning(f"  - {status.sensor_id}: 错误率 {status.error_rate:.2%}")
            logger.warning("=" * 60)
        else:
            logger.info("所有传感器正常运行，无熔断")


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="交通流量分析系统 - 准实时数据处理引擎"
    )
    
    parser.add_argument(
        "input",
        help="输入数据源 (文件路径或使用 'mock' 生成模拟数据)"
    )
    
    parser.add_argument(
        "--stream-type",
        choices=["auto", "file", "mock"],
        default="auto",
        help="数据流类型 (默认: auto)"
    )
    
    parser.add_argument(
        "--window-minutes",
        type=int,
        default=15,
        help="聚合窗口大小(分钟) (默认: 15)"
    )
    
    parser.add_argument(
        "--surge-threshold",
        type=float,
        default=0.5,
        help="流量突增检测阈值 (默认: 0.5 = 50%%)"
    )
    
    parser.add_argument(
        "--error-threshold",
        type=float,
        default=0.2,
        help="熔断器错误率阈值 (默认: 0.2 = 20%%)"
    )
    
    parser.add_argument(
        "--output-dir",
        default="output",
        help="输出目录 (默认: output)"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="批处理大小 (默认: 1000)"
    )
    
    parser.add_argument(
        "--mock-records",
        type=int,
        default=10000,
        help="模拟数据记录数 (默认: 10000)"
    )
    
    parser.add_argument(
        "--mock-sensors",
        type=int,
        default=10,
        help="模拟数据传感器数 (默认: 10)"
    )
    
    parser.add_argument(
        "--mock-anomaly-ratio",
        type=float,
        default=0.05,
        help="模拟数据异常比例 (默认: 0.05)"
    )
    
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_args()
    
    # 处理 mock 模式
    stream_type = args.stream_type
    if args.input.lower() == "mock":
        stream_type = "mock"
    
    # 创建应用
    app = TrafficAnalyticsApp(
        input_source=args.input,
        stream_type=stream_type,
        window_minutes=args.window_minutes,
        surge_threshold=args.surge_threshold,
        error_threshold=args.error_threshold,
        output_dir=args.output_dir,
        batch_size=args.batch_size
    )
    
    # 运行
    try:
        result = app.run()
        logger.info("=" * 60)
        logger.info("分析完成!")
        logger.info(f"处理记录: {result['processed_count']}")
        logger.info(f"聚合结果: {result['aggregation_count']}")
        logger.info(f"报警数量: {result['alert_count']}")
        logger.info("=" * 60)
        return 0
    except Exception as e:
        logger.exception("分析过程中发生错误")
        return 1


if __name__ == "__main__":
    sys.exit(main())
