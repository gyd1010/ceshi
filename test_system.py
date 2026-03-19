"""
测试套件：覆盖核心逻辑的单元测试，重点测试事务回滚和工号生成唯一性。
"""
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from config import DATABASE_PATH, DEPARTMENT_CODES
from models import Employee, EmployeeCreateResult, BatchImportResult
from repository import EmployeeRepository, AuditLogRepository, DatabaseConnection
from services import EmployeeService, EmployeeNumberGenerator


class TestEmployeeModel(unittest.TestCase):
    """
    测试Employee模型的验证逻辑。
    """

    def test_valid_employee(self) -> None:
        """测试有效的员工数据"""
        emp = Employee(
            name="张三",
            department="技术部",
            email="zhangsan@example.com",
            phone="13800138000",
            id_card="110101199001011234",
            hire_date="2024-01-15",
        )
        self.assertEqual(emp.name, "张三")
        self.assertEqual(emp.department_code, "001")

    def test_invalid_email(self) -> None:
        """测试无效邮箱格式"""
        with self.assertRaises(ValueError) as context:
            Employee(
                name="张三",
                department="技术部",
                email="invalid-email",
                phone="13800138000",
                id_card="110101199001011234",
                hire_date="2024-01-15",
            )
        self.assertIn("无效的邮箱格式", str(context.exception))

    def test_invalid_phone(self) -> None:
        """测试无效手机号格式"""
        with self.assertRaises(ValueError) as context:
            Employee(
                name="张三",
                department="技术部",
                email="test@example.com",
                phone="12345678901",
                id_card="110101199001011234",
                hire_date="2024-01-15",
            )
        self.assertIn("无效的手机号码格式", str(context.exception))

    def test_invalid_id_card(self) -> None:
        """测试无效身份证格式"""
        with self.assertRaises(ValueError) as context:
            Employee(
                name="张三",
                department="技术部",
                email="test@example.com",
                phone="13800138000",
                id_card="123456789012345678",
                hire_date="2024-01-15",
            )
        self.assertIn("无效的身份证号码格式", str(context.exception))

    def test_invalid_department(self) -> None:
        """测试无效部门"""
        with self.assertRaises(ValueError) as context:
            Employee(
                name="张三",
                department="不存在的部门",
                email="test@example.com",
                phone="13800138000",
                id_card="110101199001011234",
                hire_date="2024-01-15",
            )
        self.assertIn("无效的部门名称", str(context.exception))

    def test_future_hire_date(self) -> None:
        """测试入职日期不能晚于当前日期"""
        future_date = "2099-01-01"
        with self.assertRaises(ValueError) as context:
            Employee(
                name="张三",
                department="技术部",
                email="test@example.com",
                phone="13800138000",
                id_card="110101199001011234",
                hire_date=future_date,
            )
        self.assertIn("入职日期不能晚于当前日期", str(context.exception))

    def test_empty_name(self) -> None:
        """测试空姓名"""
        with self.assertRaises(ValueError) as context:
            Employee(
                name="",
                department="技术部",
                email="test@example.com",
                phone="13800138000",
                id_card="110101199001011234",
                hire_date="2024-01-15",
            )
        self.assertIn("姓名不能为空", str(context.exception))


class TestEmployeeNumberGenerator(unittest.TestCase):
    """
    测试工号生成器的唯一性和正确性。
    """

    def test_generate_employee_no(self) -> None:
        """测试工号生成格式"""
        emp_no = EmployeeNumberGenerator.generate(
            department="技术部",
            hire_date="2026-01-15",
            repository=EmployeeRepository(),
        )
        self.assertTrue(emp_no.startswith("EMP26"))
        self.assertTrue(emp_no[5:8] == "001")
        self.assertEqual(len(emp_no), 13)

    def test_employee_no_format(self) -> None:
        """测试工号格式正确性"""
        emp_no = "EMP260010001"
        self.assertEqual(emp_no[:3], "EMP")
        self.assertEqual(emp_no[3:5], "26")
        self.assertEqual(emp_no[5:8], "001")
        self.assertEqual(int(emp_no[8:]), 1)


class TestEmployeeRepository(unittest.TestCase):
    """
    测试数据访问层。
    """

    @classmethod
    def setUpClass(cls) -> None:
        """测试前准备：使用临时数据库"""
        cls.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        cls.original_db_path = DATABASE_PATH

    @classmethod
    def tearDownClass(cls) -> None:
        """测试后清理"""
        cls.temp_db.close()
        os.unlink(cls.temp_db.name)

    def setUp(self) -> None:
        """每个测试前重置数据库"""
        with patch("repository.DATABASE_PATH", self.temp_db.name):
            with patch("config.DATABASE_PATH", self.temp_db.name):
                self.db = DatabaseConnection()
                self.repo = EmployeeRepository(self.db)

    def tearDown(self) -> None:
        """每个测试后关闭连接"""
        self.db.close()

    def test_add_and_get_employee(self) -> None:
        """测试添加和查询员工"""
        emp = Employee(
            name="测试员工",
            department="技术部",
            email="test@example.com",
            phone="13800138001",
            id_card="110101199001011234",
            hire_date="2024-01-15",
            employee_no="EMP240010001",
        )
        emp_id = self.repo.add_employee(emp)
        self.assertIsInstance(emp_id, int)

        retrieved = self.repo.get_by_id(emp_id)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.name, "测试员工")

    def test_transaction_rollback(self) -> None:
        """测试事务回滚机制"""
        emp1 = Employee(
            name="员工1",
            department="技术部",
            email="emp1@example.com",
            phone="13800138001",
            id_card="110101199001011234",
            hire_date="2024-01-15",
            employee_no="EMP240010001",
        )
        emp2 = Employee(
            name="员工2",
            department="技术部",
            email="emp2@example.com",
            phone="13800138002",
            id_card="110101199002022345",
            hire_date="2024-01-15",
            employee_no="EMP240010002",
        )

        self.repo.add_employee(emp1)

        with self.assertRaises(Exception):
            with self.repo.transaction() as cursor:
                cursor.execute(
                    "INSERT INTO employees (employee_no, name) VALUES (?, ?)",
                    ("EMP240010003", "员工3"),
                )
                raise Exception("模拟错误触发回滚")

        count_before = len(self.repo.get_all())

        self.repo.batch_insert([emp2])
        count_after = len(self.repo.get_all())

        self.assertEqual(count_after, count_before + 1)


class TestEmployeeService(unittest.TestCase):
    """
    测试业务逻辑层。
    """

    @classmethod
    def setUpClass(cls) -> None:
        """测试前准备"""
        cls.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)

    @classmethod
    def tearDownClass(cls) -> None:
        """测试后清理"""
        cls.temp_db.close()
        os.unlink(cls.temp_db.name)

    def setUp(self) -> None:
        """每个测试前重置"""
        with patch("repository.DATABASE_PATH", self.temp_db.name):
            with patch("config.DATABASE_PATH", self.temp_db.name):
                self.db = DatabaseConnection()
                self.emp_repo = EmployeeRepository(self.db)
                self.audit_repo = AuditLogRepository(self.db)
                self.service = EmployeeService(self.emp_repo, self.audit_repo)

    def tearDown(self) -> None:
        self.db.close()

    def test_create_employee_success(self) -> None:
        """测试成功创建员工"""
        result = self.service.create_employee(
            name="张三",
            department="技术部",
            email="zhangsan@example.com",
            phone="13800138001",
            id_card="110101199001011234",
            hire_date="2024-01-15",
            operator="test_admin",
        )
        self.assertTrue(result.success)
        self.assertIsNotNone(result.employee_no)
        self.assertIsNone(result.error)

    def test_create_employee_duplicate_email(self) -> None:
        """测试重复邮箱"""
        self.service.create_employee(
            name="张三",
            department="技术部",
            email="duplicate@example.com",
            phone="13800138001",
            id_card="110101199001011234",
            hire_date="2024-01-15",
        )

        result = self.service.create_employee(
            name="李四",
            department="产品部",
            email="duplicate@example.com",
            phone="13800138002",
            id_card="110101199002022345",
            hire_date="2024-01-15",
        )
        self.assertFalse(result.success)
        self.assertIn("已存在", result.error)

    def test_batch_import_success(self) -> None:
        """测试成功的批量导入"""
        employees_data = [
            {
                "name": "员工A",
                "department": "技术部",
                "email": "emp_a@example.com",
                "phone": "13800138001",
                "id_card": "110101199001011234",
                "hire_date": "2024-01-15",
            },
            {
                "name": "员工B",
                "department": "产品部",
                "email": "emp_b@example.com",
                "phone": "13800138002",
                "id_card": "110101199002022345",
                "hire_date": "2024-01-15",
            },
        ]

        result = self.service.batch_import(employees_data, operator="test_admin")

        self.assertTrue(result.success)
        self.assertEqual(result.success_count, 2)
        self.assertEqual(result.failed_count, 0)
        self.assertEqual(len(result.employee_numbers), 2)

    def test_batch_import_rollback_on_validation_error(self) -> None:
        """
        测试批量导入时数据验证失败导致整体回滚。
        这是核心测试用例：部分数据失败 -> 整体回滚 -> 数据库无任何新增。
        """
        employees_data = [
            {
                "name": "有效员工",
                "department": "技术部",
                "email": "valid@example.com",
                "phone": "13800138001",
                "id_card": "110101199001011234",
                "hire_date": "2024-01-15",
            },
            {
                "name": "无效员工",
                "department": "技术部",
                "email": "invalid-email",
                "phone": "13800138002",
                "id_card": "110101199002022345",
                "hire_date": "2024-01-15",
            },
            {
                "name": "另一个有效员工",
                "department": "产品部",
                "email": "valid2@example.com",
                "phone": "13800138003",
                "id_card": "110101199003033456",
                "hire_date": "2024-01-15",
            },
        ]

        count_before = len(self.service.get_all_employees())

        result = self.service.batch_import(employees_data, operator="test_admin")

        self.assertFalse(result.success)
        self.assertGreater(len(result.errors), 0)

        count_after = len(self.service.get_all_employees())
        self.assertEqual(count_before, count_after, "验证失败后数据库应该没有任何新增记录")

        error_messages = [str(e) for e in result.errors]
        self.assertTrue(
            any("邮箱" in msg or "invalid" in msg.lower() for msg in error_messages)
        )

    def test_batch_import_rollback_on_duplicate(self) -> None:
        """
        测试批量导入时唯一性冲突导致整体回滚。
        """
        self.service.create_employee(
            name="已存在员工",
            department="技术部",
            email="existing@example.com",
            phone="13800138999",
            id_card="110101199999999999",
            hire_date="2024-01-15",
        )

        employees_data = [
            {
                "name": "新员工A",
                "department": "技术部",
                "email": "new_a@example.com",
                "phone": "13800138001",
                "id_card": "110101199001011234",
                "hire_date": "2024-01-15",
            },
            {
                "name": "冲突员工",
                "department": "产品部",
                "email": "existing@example.com",
                "phone": "13800138002",
                "id_card": "110101199002022345",
                "hire_date": "2024-01-15",
            },
        ]

        count_before = len(self.service.get_all_employees())

        result = self.service.batch_import(employees_data, operator="test_admin")

        self.assertFalse(result.success)

        count_after = len(self.service.get_all_employees())
        self.assertEqual(count_before, count_after, "唯一性冲突后应该回滚所有记录")


class TestAuditLog(unittest.TestCase):
    """
    测试审计日志功能。
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp_db.close()
        os.unlink(cls.temp_db.name)

    def setUp(self) -> None:
        with patch("repository.DATABASE_PATH", self.temp_db.name):
            with patch("config.DATABASE_PATH", self.temp_db.name):
                self.db = DatabaseConnection()
                self.audit_repo = AuditLogRepository(self.db)

    def tearDown(self) -> None:
        self.db.close()

    def test_audit_log_creation(self) -> None:
        """测试审计日志记录"""
        log_id = self.audit_repo.log(
            operator="admin",
            action="CREATE",
            target_id="EMP240010001",
            details="创建员工: 张三",
        )
        self.assertIsInstance(log_id, int)

        logs = self.audit_repo.get_all(limit=10)
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0].operator, "admin")
        self.assertEqual(logs[0].action, "CREATE")


def run_tests() -> None:
    """
    运行所有测试。
    """
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    suite.addTests(loader.loadTestsFromTestCase(TestEmployeeModel))
    suite.addTests(loader.loadTestsFromTestCase(TestEmployeeNumberGenerator))
    suite.addTests(loader.loadTestsFromTestCase(TestEmployeeRepository))
    suite.addTests(loader.loadTestsFromTestCase(TestEmployeeService))
    suite.addTests(loader.loadTestsFromTestCase(TestAuditLog))

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)


if __name__ == "__main__":
    run_tests()
