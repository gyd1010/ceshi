# -*- coding: utf-8 -*-
"""
数据访问层 (Repository Layer)

职责：
- 封装所有SQLite数据库操作
- 提供参数化查询防止SQL注入
- 实现事务上下文管理器支持回滚
- 工号生成时的并发控制（通过数据库锁）

场景一解决方案：
使用 contextlib.contextmanager 实现显式事务上下文，
在 services 层捕获异常触发回滚。
"""
import sqlite3
import logging
import threading
from datetime import datetime
from typing import List, Optional, Dict, Any, Generator
from contextlib import contextmanager
from dataclasses import asdict

from config import config
from models import Employee

logger = logging.getLogger(__name__)


class EmployeeRepository:
    """
    员工数据仓库
    
    提供员工数据的CRUD操作和批量导入功能
    """
    
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or config.database.path
        self._local = threading.local()
        self._init_database()
    
    def _init_database(self):
        """初始化数据库表结构"""
        # 确保目录存在
        import os
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 员工主表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS employees (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    employee_id TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    gender TEXT,
                    email TEXT UNIQUE NOT NULL,
                    phone TEXT UNIQUE NOT NULL,
                    id_card TEXT UNIQUE NOT NULL,
                    department TEXT NOT NULL,
                    position TEXT NOT NULL,
                    hire_date TEXT NOT NULL,
                    status TEXT DEFAULT '在职',
                    address TEXT,
                    emergency_contact TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 工号序列表（用于原子性生成流水号）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS employee_sequences (
                    year_dept TEXT PRIMARY KEY,
                    last_sequence INTEGER DEFAULT 0
                )
            """)
            
            # 创建索引
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_employee_id 
                ON employees(employee_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_department 
                ON employees(department)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_hire_date 
                ON employees(hire_date)
            """)
            
            conn.commit()
            logger.info("Database initialized successfully")
    
    def _get_connection(self) -> sqlite3.Connection:
        """获取数据库连接（线程本地存储）"""
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            self._local.connection = sqlite3.connect(
                self.db_path,
                timeout=config.database.timeout,
                check_same_thread=False
            )
            self._local.connection.row_factory = sqlite3.Row
        return self._local.connection
    
    def close(self):
        """关闭数据库连接"""
        if hasattr(self._local, 'connection') and self._local.connection:
            self._local.connection.close()
            self._local.connection = None
    
    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        """
        事务上下文管理器
        
        使用示例：
            with repo.transaction() as conn:
                repo.add_employee(emp1, conn)
                repo.add_employee(emp2, conn)
                # 如果发生异常，自动回滚
        
        场景一解决方案：
        通过此上下文管理器实现显式事务控制，
        在 services 层捕获异常后触发回滚。
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # 开启事务
            cursor.execute("BEGIN")
            logger.debug("Transaction started")
            
            yield conn
            
            # 提交事务
            conn.commit()
            logger.debug("Transaction committed")
            
        except Exception as e:
            # 回滚事务
            conn.rollback()
            logger.error(f"Transaction rolled back due to: {e}")
            raise
    
    def generate_employee_id(self, hire_date: datetime, department: str, 
                            conn: Optional[sqlite3.Connection] = None) -> str:
        """
        生成唯一工号
        
        格式：EMP + 年份后两位 + 部门代码(3位) + 流水号(4位)
        例如：EMP260010005
        
        场景二解决方案：
        使用数据库层面的原子操作获取流水号：
        1. INSERT OR REPLACE 自动处理并发
        2. RETURNING 获取新值（SQLite 3.35+）
        3. 低版本使用 SELECT + UPDATE 配合事务
        
        Args:
            hire_date: 入职日期
            department: 部门名称
            conn: 数据库连接（可选，用于事务内调用）
        
        Returns:
            生成的工号
        """
        from config import config as cfg
        
        # 提取年份后两位
        year_suffix = str(hire_date.year)[-2:]
        
        # 获取部门代码
        dept_code = cfg.department.get_code(department)
        
        # 构建年份-部门键
        year_dept_key = f"{year_suffix}{dept_code}"
        
        use_local_conn = conn is None
        if use_local_conn:
            conn = self._get_connection()
        
        cursor = conn.cursor()
        
        try:
            # 原子性获取并递增流水号
            # 使用 INSERT OR REPLACE + 子查询实现原子操作
            cursor.execute("""
                INSERT INTO employee_sequences (year_dept, last_sequence)
                VALUES (?, 1)
                ON CONFLICT(year_dept) DO UPDATE SET
                    last_sequence = last_sequence + 1
                RETURNING last_sequence
            """, (year_dept_key,))
            
            result = cursor.fetchone()
            if result:
                sequence = result[0]
            else:
                # 兼容旧版本SQLite（不支持RETURNING）
                cursor.execute("""
                    SELECT last_sequence FROM employee_sequences
                    WHERE year_dept = ?
                """, (year_dept_key,))
                sequence = cursor.fetchone()[0]
            
            # 构建工号
            employee_id = f"{cfg.employee_id.prefix}{year_suffix}{dept_code}{sequence:04d}"
            
            if use_local_conn:
                conn.commit()
            
            logger.debug(f"Generated employee_id: {employee_id}")
            return employee_id
            
        except Exception as e:
            if use_local_conn:
                conn.rollback()
            logger.error(f"Failed to generate employee_id: {e}")
            raise
    
    def add_employee(self, employee: Employee, 
                     conn: Optional[sqlite3.Connection] = None) -> str:
        """
        添加单个员工
        
        Args:
            employee: 员工对象
            conn: 数据库连接（可选，用于事务内调用）
        
        Returns:
            生成的工号
        """
        use_local_conn = conn is None
        if use_local_conn:
            conn = self._get_connection()
        
        cursor = conn.cursor()
        
        try:
            # 生成工号
            employee_id = self.generate_employee_id(
                employee.hire_date, 
                employee.department,
                conn
            )
            
            # 插入员工数据
            cursor.execute("""
                INSERT INTO employees 
                (employee_id, name, gender, email, phone, id_card,
                 department, position, hire_date, status, address, emergency_contact)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                employee_id,
                employee.name,
                employee.gender.value,
                employee.email,
                employee.phone,
                employee.id_card,
                employee.department,
                employee.position,
                employee.hire_date.isoformat(),
                employee.status.value,
                employee.address,
                employee.emergency_contact
            ))
            
            if use_local_conn:
                conn.commit()
            
            logger.info(f"Employee added: {employee_id} - {employee.name}")
            return employee_id
            
        except sqlite3.IntegrityError as e:
            if use_local_conn:
                conn.rollback()
            # 解析具体冲突字段
            error_msg = str(e).lower()
            if 'email' in error_msg:
                raise ValueError(f"邮箱已存在: {employee.email}")
            elif 'phone' in error_msg:
                raise ValueError(f"手机号已存在: {employee.phone}")
            elif 'id_card' in error_msg:
                raise ValueError(f"身份证号已存在: {employee.id_card}")
            elif 'employee_id' in error_msg:
                raise ValueError(f"工号冲突: {employee.employee_id}")
            else:
                raise ValueError(f"数据完整性错误: {e}")
        
        except Exception as e:
            if use_local_conn:
                conn.rollback()
            logger.error(f"Failed to add employee: {e}")
            raise
    
    def get_by_id(self, employee_id: str) -> Optional[Employee]:
        """根据工号查询员工"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM employees WHERE employee_id = ?
            """, (employee_id,))
            
            row = cursor.fetchone()
            if row:
                return self._row_to_employee(row)
            return None
    
    def get_by_email(self, email: str) -> Optional[Employee]:
        """根据邮箱查询员工"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM employees WHERE email = ?
            """, (email,))
            
            row = cursor.fetchone()
            if row:
                return self._row_to_employee(row)
            return None
    
    def get_all(self, department: Optional[str] = None, 
                limit: int = 100) -> List[Employee]:
        """查询所有员工（支持按部门过滤）"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            if department:
                cursor.execute("""
                    SELECT * FROM employees 
                    WHERE department = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                """, (department, limit))
            else:
                cursor.execute("""
                    SELECT * FROM employees 
                    ORDER BY created_at DESC
                    LIMIT ?
                """, (limit,))
            
            rows = cursor.fetchall()
            return [self._row_to_employee(row) for row in rows]
    
    def update_status(self, employee_id: str, status: str) -> bool:
        """更新员工状态"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE employees 
                SET status = ?
                WHERE employee_id = ?
            """, (status, employee_id))
            conn.commit()
            return cursor.rowcount > 0
    
    def delete_employee(self, employee_id: str) -> bool:
        """删除员工（软删除建议改为更新状态）"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM employees WHERE employee_id = ?
            """, (employee_id,))
            conn.commit()
            return cursor.rowcount > 0
    
    def _row_to_employee(self, row: sqlite3.Row) -> Employee:
        """将数据库行转换为Employee对象"""
        return Employee(
            employee_id=row['employee_id'],
            name=row['name'],
            gender=row['gender'],
            email=row['email'],
            phone=row['phone'],
            id_card=row['id_card'],
            department=row['department'],
            position=row['position'],
            hire_date=datetime.fromisoformat(row['hire_date']).date(),
            status=row['status'],
            address=row['address'],
            emergency_contact=row['emergency_contact'],
            created_at=datetime.fromisoformat(row['created_at'])
        )
