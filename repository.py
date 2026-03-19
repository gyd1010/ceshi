"""
数据访问层：封装SQLite操作，实现CRUD、事务管理、批量插入。
"""
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Optional, List, Generator

from config import DATABASE_PATH, SQL_CREATE_EMPLOYEES_TABLE, SQL_CREATE_AUDIT_LOG_TABLE
from models import Employee, AuditLogEntry


class DatabaseConnection:
    """
    数据库连接管理器，支持连接池和事务管理。
    """
    _instance: Optional["DatabaseConnection"] = None
    _connection: Optional[sqlite3.Connection] = None

    def __new__(cls) -> "DatabaseConnection":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            self._connection = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
            self._connection.row_factory = sqlite3.Row
            self._execute_schema()
        return self._connection

    def _execute_schema(self) -> None:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.executescript(SQL_CREATE_EMPLOYEES_TABLE)
        cursor.executescript(SQL_CREATE_AUDIT_LOG_TABLE)
        conn.commit()

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None


class EmployeeRepository:
    """
    员工数据仓库，封装所有员工相关的数据库操作。
    """

    def __init__(self, db: Optional[DatabaseConnection] = None):
        self.db = db or DatabaseConnection()

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Cursor, None, None]:
        """
        事务上下文管理器，支持自动提交和回滚。
        
        使用示例:
            with repo.transaction() as cursor:
                cursor.execute("INSERT INTO ...")
                cursor.execute("UPDATE ...")
        """
        conn = self.db.get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e

    def add_employee(self, employee: Employee) -> int:
        """
        添加单个员工，返回插入的记录ID。
        """
        now = datetime.now().isoformat()
        employee.created_at = now
        employee.updated_at = now

        with self.transaction() as cursor:
            cursor.execute(
                """
                INSERT INTO employees 
                (employee_no, name, department, department_code, email, phone, id_card, hire_date, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    employee.employee_no,
                    employee.name,
                    employee.department,
                    employee.department_code,
                    employee.email,
                    employee.phone,
                    employee.id_card,
                    employee.hire_date,
                    employee.created_at,
                    employee.updated_at,
                ),
            )
            return cursor.lastrowid

    def get_by_id(self, employee_id: int) -> Optional[Employee]:
        """
        根据ID查询员工。
        """
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM employees WHERE id = ?", (employee_id,))
        row = cursor.fetchone()
        if row:
            return self._row_to_employee(row)
        return None

    def get_by_employee_no(self, employee_no: str) -> Optional[Employee]:
        """
        根据工号查询员工。
        """
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM employees WHERE employee_no = ?", (employee_no,))
        row = cursor.fetchone()
        if row:
            return self._row_to_employee(row)
        return None

    def get_all(self) -> List[Employee]:
        """
        获取所有员工列表。
        """
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM employees ORDER BY created_at DESC")
        rows = cursor.fetchall()
        return [self._row_to_employee(row) for row in rows]

    def get_max_sequence_by_department_and_year(
        self, department_code: str, year_suffix: str
    ) -> int:
        """
        获取指定部门和年份的最大流水号。
        用于工号生成的原子递增。
        """
        conn = self.db.get_connection()
        cursor = conn.cursor()
        pattern = f"EMP{year_suffix}{department_code}%"
        cursor.execute(
            """
            SELECT employee_no FROM employees 
            WHERE employee_no LIKE ? 
            ORDER BY employee_no DESC LIMIT 1
            """,
            (pattern,),
        )
        row = cursor.fetchone()
        if row:
            last_no = row["employee_no"]
            return int(last_no[-4:])
        return 0

    def batch_insert(self, employees: List[Employee]) -> List[int]:
        """
        批量插入员工，使用事务保证原子性。
        如果任何一条插入失败，整个批次回滚。
        返回插入的记录ID列表。
        """
        now = datetime.now().isoformat()
        ids: List[int] = []

        with self.transaction() as cursor:
            for emp in employees:
                emp.created_at = now
                emp.updated_at = now
                cursor.execute(
                    """
                    INSERT INTO employees 
                    (employee_no, name, department, department_code, email, phone, id_card, hire_date, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        emp.employee_no,
                        emp.name,
                        emp.department,
                        emp.department_code,
                        emp.email,
                        emp.phone,
                        emp.id_card,
                        emp.hire_date,
                        emp.created_at,
                        emp.updated_at,
                    ),
                )
                ids.append(cursor.lastrowid)
        return ids

    def delete_by_id(self, employee_id: int) -> bool:
        """
        根据ID删除员工。
        """
        with self.transaction() as cursor:
            cursor.execute("DELETE FROM employees WHERE id = ?", (employee_id,))
            return cursor.rowcount > 0

    def update_employee(self, employee_id: int, updates: dict) -> bool:
        """
        更新员工信息。
        """
        updates["updated_at"] = datetime.now().isoformat()
        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values()) + [employee_id]

        with self.transaction() as cursor:
            cursor.execute(
                f"UPDATE employees SET {set_clause} WHERE id = ?",
                values,
            )
            return cursor.rowcount > 0

    def check_unique_fields(
        self, email: Optional[str] = None, phone: Optional[str] = None, id_card: Optional[str] = None
    ) -> List[str]:
        """
        检查字段唯一性，返回冲突的字段列表。
        """
        conflicts: List[str] = []
        conn = self.db.get_connection()
        cursor = conn.cursor()

        if email:
            cursor.execute("SELECT 1 FROM employees WHERE email = ?", (email,))
            if cursor.fetchone():
                conflicts.append(f"邮箱 {email} 已存在")

        if phone:
            cursor.execute("SELECT 1 FROM employees WHERE phone = ?", (phone,))
            if cursor.fetchone():
                conflicts.append(f"手机号 {phone} 已存在")

        if id_card:
            cursor.execute("SELECT 1 FROM employees WHERE id_card = ?", (id_card,))
            if cursor.fetchone():
                conflicts.append(f"身份证号 {id_card} 已存在")

        return conflicts

    def _row_to_employee(self, row: sqlite3.Row) -> Employee:
        return Employee(
            name=row["name"],
            department=row["department"],
            email=row["email"],
            phone=row["phone"],
            id_card=row["id_card"],
            hire_date=row["hire_date"],
            employee_no=row["employee_no"],
            department_code=row["department_code"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


class AuditLogRepository:
    """
    审计日志仓库。
    """

    def __init__(self, db: Optional[DatabaseConnection] = None):
        self.db = db or DatabaseConnection()

    def log(
        self,
        operator: str,
        action: str,
        target_id: Optional[str] = None,
        details: Optional[str] = None,
    ) -> int:
        """
        记录审计日志。
        """
        timestamp = datetime.now().isoformat()
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO audit_log (timestamp, operator, action, target_id, details)
            VALUES (?, ?, ?, ?, ?)
            """,
            (timestamp, operator, action, target_id, details),
        )
        conn.commit()
        return cursor.lastrowid

    def get_all(self, limit: int = 100) -> List[AuditLogEntry]:
        """
        获取审计日志列表。
        """
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM audit_log ORDER BY timestamp DESC LIMIT ?",
            (limit,),
        )
        rows = cursor.fetchall()
        return [
            AuditLogEntry(
                timestamp=row["timestamp"],
                operator=row["operator"],
                action=row["action"],
                target_id=row["target_id"],
                details=row["details"],
            )
            for row in rows
        ]
