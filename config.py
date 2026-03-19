"""
配置模块：管理数据库路径、部门代码映射、验证规则等全局配置。
"""
from pathlib import Path
from typing import Dict

BASE_DIR: Path = Path(__file__).parent.resolve()

DATABASE_PATH: str = str(BASE_DIR / "employees.db")

AUDIT_LOG_PATH: str = str(BASE_DIR / "audit.log")

DEPARTMENT_CODES: Dict[str, str] = {
    "技术部": "001",
    "产品部": "002",
    "运营部": "003",
    "市场部": "004",
    "人事部": "005",
    "财务部": "006",
}

DEPARTMENT_NAMES: Dict[str, str] = {v: k for k, v in DEPARTMENT_CODES.items()}

EMAIL_PATTERN: str = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

PHONE_PATTERN: str = r"^1[3-9]\d{9}$"

ID_CARD_PATTERN: str = r"^[1-9]\d{5}(19|20)\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\d{3}[\dXx]$"

SQL_CREATE_EMPLOYEES_TABLE: str = """
CREATE TABLE IF NOT EXISTS employees (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_no TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    department TEXT NOT NULL,
    department_code TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    phone TEXT UNIQUE NOT NULL,
    id_card TEXT UNIQUE NOT NULL,
    hire_date TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_employee_no ON employees(employee_no);
CREATE INDEX IF NOT EXISTS idx_department_code ON employees(department_code);
CREATE INDEX IF NOT EXISTS idx_hire_date ON employees(hire_date);
"""

SQL_CREATE_AUDIT_LOG_TABLE: str = """
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    operator TEXT NOT NULL,
    action TEXT NOT NULL,
    target_id TEXT,
    details TEXT
);
"""
