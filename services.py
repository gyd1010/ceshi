# -*- coding: utf-8 -*-
"""
业务逻辑层 (Service Layer)

职责：
- 实现核心业务规则
- 工号生成算法协调
- 批量导入的事务控制
- 审计日志记录

场景一实现：
使用 repository.transaction() 上下文管理器，
捕获异常后触发回滚。
"""
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

from config import config
from models import Employee, BatchImportResult
from repository import EmployeeRepository

logger = logging.getLogger(__name__)


class AuditLogger:
    """审计日志记录器"""
    
    def __init__(self, log_path: Optional[str] = None):
        self.log_path = log_path or config.audit.log_path
        self._ensure_log_file()
    
    def _ensure_log_file(self):
        """确保日志文件存在"""
        import os
        log_dir = os.path.dirname(self.log_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
    
    def log(self, action: str, operator: str, 
            target_id: Optional[str] = None,
            details: Optional[Dict] = None):
        """
        记录审计日志
        
        Args:
            action: 操作类型（ADD/UPDATE/DELETE/BATCH_IMPORT）
            operator: 操作人
            target_id: 目标对象ID
            details: 详细数据
        """
        if not config.audit.enabled:
            return
        
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "action": action,
            "operator": operator,
            "target_id": target_id,
            "details": details or {}
        }
        
        # 追加写入日志文件
        with open(self.log_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
        
        logger.debug(f"Audit log: {action} by {operator}")


class EmployeeService:
    """
    员工服务类
    
    封装所有员工相关的业务逻辑
    """
    
    def __init__(self, repository: Optional[EmployeeRepository] = None):
        self.repository = repository or EmployeeRepository()
        self.audit_logger = AuditLogger()
    
    def create_employee(self, employee_data: Dict[str, Any], 
                       operator: str = "system") -> Employee:
        """
        创建单个员工
        
        Args:
            employee_data: 员工数据字典
            operator: 操作人
        
        Returns:
            创建成功的Employee对象（包含生成的工号）
        
        Raises:
            ValueError: 数据验证失败
        """
        try:
            # 1. 数据验证（Pydantic自动完成）
            employee = Employee(**employee_data)
            
            # 2. 保存到数据库
            employee_id = self.repository.add_employee(employee)
            
            # 3. 更新对象中的工号
            employee.employee_id = employee_id
            
            # 4. 记录审计日志
            self.audit_logger.log(
                action="ADD",
                operator=operator,
                target_id=employee_id,
                details={"name": employee.name, "department": employee.department}
            )
            
            logger.info(f"Employee created: {employee_id}")
            return employee
            
        except Exception as e:
            logger.error(f"Failed to create employee: {e}")
            raise
    
    def batch_import(self, employees_data: List[Dict[str, Any]], 
                    operator: str = "system") -> BatchImportResult:
        """
        批量导入员工
        
        关键要求：如果批次中任何一个员工数据验证失败，
        整个批次必须回滚，不写入任何数据。
        
        场景一实现：
        使用 repository.transaction() 上下文管理器包裹所有操作，
        一旦发生异常，自动回滚所有已执行的操作。
        
        Args:
            employees_data: 员工数据列表
            operator: 操作人
        
        Returns:
            BatchImportResult: 导入结果详情
        """
        result = BatchImportResult(success=False)
        
        if not employees_data:
            result.errors.append({"index": -1, "error": "Empty batch"})
            return result
        
        # 第一阶段：预验证所有数据（不写入数据库）
        validated_employees = []
        for i, data in enumerate(employees_data):
            try:
                employee = Employee(**data)
                validated_employees.append((i, employee))
            except Exception as e:
                result.add_error(i, str(e))
        
        # 如果有验证失败的，直接返回（无需回滚，因为还没开始写入）
        if result.failed_count > 0:
            logger.warning(f"Batch validation failed: {result.failed_count} errors")
            return result
        
        # 第二阶段：事务性写入
        try:
            with self.repository.transaction():
                for index, employee in validated_employees:
                    try:
                        # 生成工号并保存
                        employee_id = self.repository.add_employee(
                            employee, 
                            conn=self.repository._get_connection()
                        )
                        result.add_success(employee_id)
                        
                    except Exception as e:
                        # 任何写入错误都触发回滚
                        result.add_error(index, str(e))
                        raise  # 抛出异常触发事务回滚
            
            # 事务成功提交后记录审计日志
            if result.imported_count > 0:
                self.audit_logger.log(
                    action="BATCH_IMPORT",
                    operator=operator,
                    details={
                        "count": result.imported_count,
                        "employee_ids": result.employee_ids
                    }
                )
            
            result.success = True
            logger.info(f"Batch import successful: {result.imported_count} employees")
            
        except Exception as e:
            # 事务回滚后的处理
            logger.error(f"Batch import failed and rolled back: {e}")
            result.success = False
            if not result.errors:
                result.add_error(-1, f"Transaction failed: {str(e)}")
        
        return result
    
    def get_employee(self, employee_id: str) -> Optional[Employee]:
        """查询单个员工"""
        return self.repository.get_by_id(employee_id)
    
    def list_employees(self, department: Optional[str] = None, 
                      limit: int = 100) -> List[Employee]:
        """列出员工"""
        return self.repository.get_all(department, limit)
    
    def update_employee_status(self, employee_id: str, status: str,
                               operator: str = "system") -> bool:
        """更新员工状态"""
        success = self.repository.update_status(employee_id, status)
        
        if success:
            self.audit_logger.log(
                action="UPDATE_STATUS",
                operator=operator,
                target_id=employee_id,
                details={"new_status": status}
            )
        
        return success
    
    def delete_employee(self, employee_id: str, 
                       operator: str = "system") -> bool:
        """删除员工"""
        # 建议改为软删除（更新状态为"离职"）
        success = self.repository.delete_employee(employee_id)
        
        if success:
            self.audit_logger.log(
                action="DELETE",
                operator=operator,
                target_id=employee_id
            )
        
        return success
    
    def get_department_stats(self) -> Dict[str, int]:
        """获取各部门人数统计"""
        employees = self.repository.get_all(limit=10000)
        stats = {}
        for emp in employees:
            dept = emp.department
            stats[dept] = stats.get(dept, 0) + 1
        return stats
    
    def close(self):
        """关闭资源"""
        self.repository.close()
