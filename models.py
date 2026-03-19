"""
领域模型层：定义Employee数据类，使用Pydantic进行严格的数据验证。
"""
import re
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, field_validator, model_validator

from config import (
    DEPARTMENT_CODES,
    EMAIL_PATTERN,
    PHONE_PATTERN,
    ID_CARD_PATTERN,
)


class Employee(BaseModel):
    """
    员工数据模型，包含严格的数据验证逻辑。
    
    Attributes:
        name: 员工姓名
        department: 部门名称
        email: 邮箱地址
        phone: 手机号码
        id_card: 身份证号码
        hire_date: 入职日期 (YYYY-MM-DD格式)
        employee_no: 工号 (系统生成)
        department_code: 部门代码 (系统生成)
    """
    name: str
    department: str
    email: str
    phone: str
    id_card: str
    hire_date: str
    employee_no: Optional[str] = None
    department_code: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("姓名不能为空")
        if len(v.strip()) < 2:
            raise ValueError("姓名长度不能少于2个字符")
        return v.strip()

    @field_validator("department")
    @classmethod
    def validate_department(cls, v: str) -> str:
        if v not in DEPARTMENT_CODES:
            valid_depts = ", ".join(DEPARTMENT_CODES.keys())
            raise ValueError(f"无效的部门名称，有效部门: {valid_depts}")
        return v

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        if not re.match(EMAIL_PATTERN, v):
            raise ValueError(f"无效的邮箱格式: {v}")
        return v.lower()

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v: str) -> str:
        if not re.match(PHONE_PATTERN, v):
            raise ValueError(f"无效的手机号码格式: {v}")
        return v

    @field_validator("id_card")
    @classmethod
    def validate_id_card(cls, v: str) -> str:
        v_upper = v.upper()
        if not re.match(ID_CARD_PATTERN, v_upper):
            raise ValueError(f"无效的身份证号码格式: {v}")
        cls._validate_id_card_checksum(v_upper)
        return v_upper

    @staticmethod
    def _validate_id_card_checksum(id_card: str) -> None:
        """
        验证身份证校验码。
        """
        weights = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
        check_codes = "10X98765432"
        total = sum(int(id_card[i]) * weights[i] for i in range(17))
        if check_codes[total % 11] != id_card[17]:
            raise ValueError(f"身份证校验码错误: {id_card}")

    @field_validator("hire_date")
    @classmethod
    def validate_hire_date(cls, v: str) -> str:
        try:
            hire_dt = datetime.strptime(v, "%Y-%m-%d")
            if hire_dt > datetime.now():
                raise ValueError("入职日期不能晚于当前日期")
            return v
        except ValueError as e:
            raise ValueError(f"无效的日期格式，请使用YYYY-MM-DD格式: {v}") from e

    @model_validator(mode="after")
    def set_department_code(self) -> "Employee":
        self.department_code = DEPARTMENT_CODES.get(self.department)
        return self

    def to_dict(self) -> dict:
        return self.model_dump()


class EmployeeCreateResult(BaseModel):
    """
    员工创建结果模型。
    """
    success: bool
    employee_no: Optional[str] = None
    error: Optional[str] = None


class BatchImportResult(BaseModel):
    """
    批量导入结果模型。
    """
    success: bool
    total_count: int
    success_count: int
    failed_count: int
    employee_numbers: list[str] = []
    errors: list[dict] = []


class AuditLogEntry(BaseModel):
    """
    审计日志条目模型。
    """
    timestamp: str
    operator: str
    action: str
    target_id: Optional[str] = None
    details: Optional[str] = None
