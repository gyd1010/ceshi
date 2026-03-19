"""
业务逻辑层：处理核心业务规则，包括工号生成、部门编码映射、批量导入协调。
"""
import logging
import threading
from datetime import datetime
from typing import List, Optional, Tuple

from config import AUDIT_LOG_PATH, DEPARTMENT_CODES
from models import (
    Employee,
    EmployeeCreateResult,
    BatchImportResult,
)
from repository import EmployeeRepository, AuditLogRepository


logger = logging.getLogger("audit")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(AUDIT_LOG_PATH, encoding="utf-8")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)


class EmployeeNumberGenerator:
    """
    工号生成器，确保在并发环境下的唯一性。
    
    工号规则：EMP + 入职年份后两位 + 部门代码(3位) + 流水号(4位)
    例如：2026年入职的技术部（代码001）第5位员工 -> EMP260010005
    """
    _lock = threading.Lock()

    @classmethod
    def generate(
        cls,
        department: str,
        hire_date: str,
        repository: EmployeeRepository,
    ) -> str:
        """
        生成唯一工号。
        使用线程锁确保并发安全。
        """
        with cls._lock:
            department_code = DEPARTMENT_CODES.get(department)
            if not department_code:
                raise ValueError(f"无效的部门: {department}")

            year_suffix = hire_date[2:4]
            max_seq = repository.get_max_sequence_by_department_and_year(
                department_code, year_suffix
            )
            new_seq = max_seq + 1
            employee_no = f"EMP{year_suffix}{department_code}{new_seq:04d}"
            return employee_no


class EmployeeService:
    """
    员工业务服务，协调Repository和业务规则。
    """

    def __init__(
        self,
        employee_repo: Optional[EmployeeRepository] = None,
        audit_repo: Optional[AuditLogRepository] = None,
    ):
        self.employee_repo = employee_repo or EmployeeRepository()
        self.audit_repo = audit_repo or AuditLogRepository()

    def create_employee(
        self,
        name: str,
        department: str,
        email: str,
        phone: str,
        id_card: str,
        hire_date: str,
        operator: str = "system",
    ) -> EmployeeCreateResult:
        """
        创建单个员工。
        
        流程：
        1. 数据验证（Pydantic模型）
        2. 唯一性检查
        3. 生成工号
        4. 持久化
        5. 记录审计日志
        """
        try:
            employee = Employee(
                name=name,
                department=department,
                email=email,
                phone=phone,
                id_card=id_card,
                hire_date=hire_date,
            )

            conflicts = self.employee_repo.check_unique_fields(
                email=employee.email, phone=employee.phone, id_card=employee.id_card
            )
            if conflicts:
                return EmployeeCreateResult(
                    success=False, error="; ".join(conflicts)
                )

            employee.employee_no = EmployeeNumberGenerator.generate(
                employee.department, employee.hire_date, self.employee_repo
            )

            self.employee_repo.add_employee(employee)

            self._log_audit(operator, "CREATE", employee.employee_no, f"创建员工: {name}")

            return EmployeeCreateResult(
                success=True, employee_no=employee.employee_no
            )

        except ValueError as e:
            return EmployeeCreateResult(success=False, error=str(e))
        except Exception as e:
            return EmployeeCreateResult(success=False, error=f"系统错误: {e}")

    def batch_import(
        self,
        employees_data: List[dict],
        operator: str = "system",
    ) -> BatchImportResult:
        """
        批量导入员工，支持事务回滚。
        
        关键特性：
        - 如果批次中任何一个员工数据验证失败，整个批次回滚
        - 返回详细的错误报告
        
        Args:
            employees_data: 员工数据字典列表
            operator: 操作人
            
        Returns:
            BatchImportResult: 包含成功/失败详情的结果对象
        """
        result = BatchImportResult(
            success=False,
            total_count=len(employees_data),
            success_count=0,
            failed_count=0,
            employee_numbers=[],
            errors=[],
        )

        validated_employees: List[Employee] = []
        all_conflicts: List[str] = []

        for idx, data in enumerate(employees_data):
            try:
                employee = Employee(**data)
                conflicts = self.employee_repo.check_unique_fields(
                    email=employee.email, phone=employee.phone, id_card=employee.id_card
                )
                if conflicts:
                    all_conflicts.extend([f"第{idx + 1}条: {c}" for c in conflicts])
                    continue
                validated_employees.append(employee)
            except ValueError as e:
                result.errors.append(
                    {"index": idx + 1, "data": data, "error": str(e)}
                )

        if result.errors or all_conflicts:
            result.failed_count = len(result.errors) + len(
                set(c.split(":")[0] for c in all_conflicts)
            )
            for conflict in all_conflicts:
                result.errors.append({"error": conflict})
            return result

        try:
            for emp in validated_employees:
                emp.employee_no = EmployeeNumberGenerator.generate(
                    emp.department, emp.hire_date, self.employee_repo
                )

            self.employee_repo.batch_insert(validated_employees)

            result.success = True
            result.success_count = len(validated_employees)
            result.employee_numbers = [emp.employee_no for emp in validated_employees]

            self._log_audit(
                operator,
                "BATCH_IMPORT",
                None,
                f"批量导入 {result.success_count} 名员工: {', '.join(result.employee_numbers)}",
            )

        except Exception as e:
            result.errors.append({"error": f"批量插入失败，已回滚: {e}"})
            self._log_audit(
                operator,
                "BATCH_IMPORT_FAILED",
                None,
                f"批量导入失败: {e}",
            )

        return result

    def get_employee(self, employee_no: str) -> Optional[Employee]:
        """
        根据工号查询员工。
        """
        return self.employee_repo.get_by_employee_no(employee_no)

    def get_all_employees(self) -> List[Employee]:
        """
        获取所有员工列表。
        """
        return self.employee_repo.get_all()

    def delete_employee(
        self, employee_no: str, operator: str = "system"
    ) -> Tuple[bool, str]:
        """
        删除员工。
        """
        employee = self.employee_repo.get_by_employee_no(employee_no)
        if not employee:
            return False, f"员工不存在: {employee_no}"

        success = self.employee_repo.delete_by_id(
            self._get_employee_id(employee_no)
        )
        if success:
            self._log_audit(
                operator, "DELETE", employee_no, f"删除员工: {employee.name}"
            )
            return True, f"员工 {employee_no} 已删除"
        return False, "删除失败"

    def _get_employee_id(self, employee_no: str) -> Optional[int]:
        conn = self.employee_repo.db.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id FROM employees WHERE employee_no = ?", (employee_no,)
        )
        row = cursor.fetchone()
        return row["id"] if row else None

    def _log_audit(
        self,
        operator: str,
        action: str,
        target_id: Optional[str],
        details: Optional[str],
    ) -> None:
        """
        记录审计日志（同时写入数据库和日志文件）。
        """
        self.audit_repo.log(operator, action, target_id, details)
        logger.info(f"[{operator}] {action} - {target_id or 'N/A'} - {details or 'N/A'}")


class DepartmentService:
    """
    部门服务，提供部门相关的查询功能。
    """

    @staticmethod
    def get_all_departments() -> dict:
        """
        获取所有部门及其代码。
        """
        return DEPARTMENT_CODES.copy()

    @staticmethod
    def get_department_name(code: str) -> Optional[str]:
        """
        根据部门代码获取部门名称。
        """
        from config import DEPARTMENT_NAMES

        return DEPARTMENT_NAMES.get(code)
