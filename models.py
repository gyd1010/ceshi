# -*- coding: utf-8 -*-
"""
领域模型模块 - 员工数据模型

使用 Pydantic 进行严格的数据验证：
- 邮箱格式验证
- 手机号格式验证
- 身份证号验证（18位，含校验位）
- 入职日期验证
"""
import re
from datetime import date, datetime
from typing import Optional, Dict, Any
from enum import Enum

from pydantic import BaseModel, Field, validator, root_validator


class Gender(str, Enum):
    """性别枚举"""
    MALE = "男"
    FEMALE = "女"
    OTHER = "其他"


class EmployeeStatus(str, Enum):
    """员工状态枚举"""
    ACTIVE = "在职"
    RESIGNED = "离职"
    PROBATION = "试用期"
    SUSPENDED = "停薪留职"


class Employee(BaseModel):
    """
    员工领域模型
    
    字段说明：
    - employee_id: 系统自动生成的工号（EMP + 年份 + 部门代码 + 流水号）
    - name: 姓名（2-20个字符）
    - email: 邮箱（必须符合邮箱格式）
    - phone: 手机号（11位，1开头）
    - id_card: 身份证号（18位，含校验）
    - department: 部门名称
    - position: 职位
    - hire_date: 入职日期
    - gender: 性别
    - status: 员工状态
    - created_at: 创建时间（自动填充）
    """
    
    # 系统字段
    employee_id: Optional[str] = Field(None, description="工号，系统自动生成")
    created_at: Optional[datetime] = Field(None, description="创建时间")
    
    # 基本信息
    name: str = Field(..., min_length=2, max_length=20, description="姓名")
    gender: Gender = Field(default=Gender.MALE, description="性别")
    
    # 联系方式
    email: str = Field(..., description="邮箱地址")
    phone: str = Field(..., description="手机号码")
    
    # 身份认证
    id_card: str = Field(..., description="身份证号码")
    
    # 工作信息
    department: str = Field(..., min_length=2, max_length=50, description="部门")
    position: str = Field(..., min_length=2, max_length=50, description="职位")
    hire_date: date = Field(..., description="入职日期")
    status: EmployeeStatus = Field(default=EmployeeStatus.ACTIVE, description="状态")
    
    # 扩展信息（可选）
    address: Optional[str] = Field(None, max_length=200, description="住址")
    emergency_contact: Optional[str] = Field(None, max_length=20, description="紧急联系人电话")
    
    @validator('email')
    def validate_email(cls, v: str) -> str:
        """验证邮箱格式"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(pattern, v):
            raise ValueError(f"邮箱格式不正确: {v}")
        return v.lower().strip()
    
    @validator('phone')
    def validate_phone(cls, v: str) -> str:
        """验证手机号格式（中国大陆）"""
        # 去除空格和横线
        v = v.replace(" ", "").replace("-", "").strip()
        
        # 必须是11位数字，1开头
        pattern = r'^1[3-9]\d{9}$'
        if not re.match(pattern, v):
            raise ValueError(f"手机号格式不正确: {v}，必须是11位数字且1开头")
        return v
    
    @validator('id_card')
    def validate_id_card(cls, v: str) -> str:
        """
        验证身份证号码（18位）
        
        校验规则：
        1. 必须是18位
        2. 前17位必须是数字
        3. 最后一位可以是数字或X
        4. 校验位计算（加权求和模11）
        """
        v = v.upper().strip()
        
        # 基本格式检查
        if len(v) != 18:
            raise ValueError(f"身份证号必须是18位: {v}")
        
        pattern = r'^\d{17}[\dX]$'
        if not re.match(pattern, v):
            raise ValueError(f"身份证号格式不正确: {v}")
        
        # 校验位验证
        weights = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
        check_codes = ['1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2']
        
        # 计算校验和
        sum_value = sum(int(v[i]) * weights[i] for i in range(17))
        calculated_check = check_codes[sum_value % 11]
        
        if v[17] != calculated_check:
            raise ValueError(f"身份证号校验位错误: {v}")
        
        return v
    
    @validator('hire_date')
    def validate_hire_date(cls, v: date) -> date:
        """验证入职日期"""
        if v > date.today():
            raise ValueError(f"入职日期不能是未来日期: {v}")
        
        # 入职日期不能早于1950年（合理范围）
        if v.year < 1950:
            raise ValueError(f"入职日期不合理: {v}")
        
        return v
    
    @validator('name')
    def validate_name(cls, v: str) -> str:
        """验证姓名"""
        v = v.strip()
        if not v:
            raise ValueError("姓名不能为空")
        
        # 检查是否包含数字或特殊字符
        if re.search(r'[0-9!@#$%^&*()_+=\[\]{}|;:\'",.<>?/\\]', v):
            raise ValueError(f"姓名包含非法字符: {v}")
        
        return v
    
    @root_validator
    def set_defaults(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """设置默认值"""
        if values.get('created_at') is None:
            values['created_at'] = datetime.now()
        return values
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（用于序列化）"""
        return {
            "employee_id": self.employee_id,
            "name": self.name,
            "gender": self.gender.value,
            "email": self.email,
            "phone": self.phone,
            "id_card": self.id_card,
            "department": self.department,
            "position": self.position,
            "hire_date": self.hire_date.isoformat(),
            "status": self.status.value,
            "address": self.address,
            "emergency_contact": self.emergency_contact,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Employee":
        """从字典创建实例"""
        # 处理日期字段
        if isinstance(data.get('hire_date'), str):
            data['hire_date'] = date.fromisoformat(data['hire_date'])
        if isinstance(data.get('created_at'), str):
            data['created_at'] = datetime.fromisoformat(data['created_at'])
        
        # 处理枚举
        if 'gender' in data and isinstance(data['gender'], str):
            data['gender'] = Gender(data['gender'])
        if 'status' in data and isinstance(data['status'], str):
            data['status'] = EmployeeStatus(data['status'])
        
        return cls(**data)
    
    class Config:
        """Pydantic 配置"""
        validate_assignment = True
        extra = "forbid"
        json_encoders = {
            date: lambda v: v.isoformat(),
            datetime: lambda v: v.isoformat()
        }


class BatchImportResult(BaseModel):
    """批量导入结果"""
    success: bool
    imported_count: int = 0
    failed_count: int = 0
    errors: list = []
    employee_ids: list = []
    
    def add_error(self, index: int, error_message: str):
        """添加错误记录"""
        self.failed_count += 1
        self.errors.append({
            "index": index,
            "error": error_message
        })
    
    def add_success(self, employee_id: str):
        """添加成功记录"""
        self.imported_count += 1
        self.employee_ids.append(employee_id)
