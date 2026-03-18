# -*- coding: utf-8 -*-
"""
配置管理模块

集中管理：
- 数据库路径
- 部门代码映射
- 工号生成规则
- 审计日志配置
"""
from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class DatabaseConfig:
    """数据库配置"""
    path: str = "data/employees.db"
    timeout: int = 30


@dataclass(frozen=True)
class DepartmentConfig:
    """部门代码映射配置"""
    # 部门代码映射表
    mapping: Dict[str, str] = None
    
    def __post_init__(self):
        # 由于frozen=True，需要在__post_init__中设置默认值
        object.__setattr__(
            self, 'mapping',
            {
                "技术部": "001",
                "产品部": "002",
                "运营部": "003",
                "市场部": "004",
                "人事部": "005",
                "财务部": "006",
                "行政部": "007",
                "销售部": "008",
            }
        )
    
    def get_code(self, department_name: str) -> str:
        """获取部门代码"""
        return self.mapping.get(department_name, "999")  # 未知部门默认999
    
    def get_all_departments(self) -> Dict[str, str]:
        """获取所有部门映射"""
        return self.mapping.copy()


@dataclass(frozen=True)
class EmployeeIdConfig:
    """工号生成配置"""
    prefix: str = "EMP"
    year_digits: int = 2  # 年份后两位
    dept_digits: int = 3  # 部门代码位数
    seq_digits: int = 4   # 流水号位数


@dataclass(frozen=True)
class AuditConfig:
    """审计日志配置"""
    enabled: bool = True
    log_path: str = "data/audit.log"
    max_size_mb: int = 100
    backup_count: int = 5


class Config:
    """全局配置容器"""
    
    def __init__(self):
        self.database = DatabaseConfig()
        self.department = DepartmentConfig()
        self.employee_id = EmployeeIdConfig()
        self.audit = AuditConfig()


# 全局配置实例
config = Config()
