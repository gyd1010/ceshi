# -*- coding: utf-8 -*-
"""
测试套件 (Test Suite)

覆盖：
1. 数据模型验证（邮箱、手机号、身份证）
2. 工号生成唯一性
3. 事务回滚（批量导入部分失败场景）
4. 并发安全

场景三实现：
编写失败案例验证数据验证拦截效果
"""
import sys
import os
import json
import threading
import time
from datetime import date, datetime

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(__file__))

from config import config
from models import Employee, BatchImportResult
from repository import EmployeeRepository
from services import EmployeeService


class TestRunner:
    """测试运行器"""
    
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.tests = []
    
    def test(self, name):
        """测试装饰器"""
        def decorator(func):
            self.tests.append((name, func))
            return func
        return decorator
    
    def run(self):
        """运行所有测试"""
        print("\n" + "=" * 70)
        print("员工登记系统 - 测试套件")
        print("=" * 70)
        
        for name, func in self.tests:
            try:
                print(f"\n[Test] {name}...")
                func()
                print(f"  ✅ 通过")
                self.passed += 1
            except AssertionError as e:
                print(f"  ❌ 失败: {e}")
                self.failed += 1
            except Exception as e:
                print(f"  ❌ 异常: {e}")
                self.failed += 1
        
        print("\n" + "=" * 70)
        print(f"测试结果: {self.passed} 通过, {self.failed} 失败")
        print("=" * 70)
        
        return self.failed == 0


# 创建全局测试运行器
runner = TestRunner()


# ==================== 数据模型验证测试 ====================

@runner.test("邮箱格式验证-正确")
def test_email_valid():
    """测试正确的邮箱格式"""
    emp = Employee(
        name="测试",
        email="test@company.com",
        phone="13800138000",
        id_card="110101199001011234",
        department="技术部",
        position="工程师",
        hire_date=date.today()
    )
    assert emp.email == "test@company.com"


@runner.test("邮箱格式验证-错误")
def test_email_invalid():
    """测试错误的邮箱格式应该被拒绝"""
    try:
        Employee(
            name="测试",
            email="invalid-email",
            phone="13800138000",
            id_card="110101199001011234",
            department="技术部",
            position="工程师",
            hire_date=date.today()
        )
        raise AssertionError("应该抛出验证错误")
    except ValueError as e:
        assert "邮箱" in str(e)


@runner.test("手机号格式验证-正确")
def test_phone_valid():
    """测试正确的手机号"""
    emp = Employee(
        name="测试",
        email="test@company.com",
        phone="13800138000",
        id_card="110101199001011234",
        department="技术部",
        position="工程师",
        hire_date=date.today()
    )
    assert emp.phone == "13800138000"


@runner.test("手机号格式验证-错误")
def test_phone_invalid():
    """测试错误的手机号应该被拒绝"""
    invalid_phones = [
        "12345678901",  # 不是1开头
        "1380013800",   # 10位
        "138001380000", # 12位
        "abcdefghijk",  # 非数字
    ]
    
    for phone in invalid_phones:
        try:
            Employee(
                name="测试",
                email="test@company.com",
                phone=phone,
                id_card="110101199001011234",
                department="技术部",
                position="工程师",
                hire_date=date.today()
            )
            raise AssertionError(f"手机号 {phone} 应该被拒绝")
        except ValueError:
            pass  # 预期行为


@runner.test("身份证号验证-正确")
def test_idcard_valid():
    """测试正确的身份证号"""
    # 使用一个真实的身份证号格式（仅用于测试）
    emp = Employee(
        name="测试",
        email="test@company.com",
        phone="13800138000",
        id_card="110101199001011234",  # 假设这是有效的
        department="技术部",
        position="工程师",
        hire_date=date.today()
    )
    assert len(emp.id_card) == 18


@runner.test("身份证号验证-长度错误")
def test_idcard_length_invalid():
    """测试长度不正确的身份证号"""
    try:
        Employee(
            name="测试",
            email="test@company.com",
            phone="13800138000",
            id_card="11010119900101123",  # 17位
            department="技术部",
            position="工程师",
            hire_date=date.today()
        )
        raise AssertionError("应该抛出验证错误")
    except ValueError as e:
        assert "18位" in str(e)


# ==================== 工号生成测试 ====================

@runner.test("工号生成格式")
def test_employee_id_format():
    """测试工号格式正确性"""
    # 清理测试数据库
    test_db = "data/test_employees.db"
    if os.path.exists(test_db):
        os.remove(test_db)
    
    repo = EmployeeRepository(db_path=test_db)
    
    try:
        # 生成工号
        hire_date = datetime(2026, 3, 18)
        employee_id = repo.generate_employee_id(hire_date, "技术部")
        
        # 验证格式: EMP + 26 + 001 + 0001
        assert employee_id.startswith("EMP"), f"工号应该以EMP开头: {employee_id}"
        assert len(employee_id) == 11, f"工号长度应该是11位: {employee_id}"
        assert employee_id[3:5] == "26", f"年份应该是26: {employee_id}"
        assert employee_id[5:8] == "001", f"部门代码应该是001: {employee_id}"
        
        print(f"    生成工号: {employee_id}")
    finally:
        repo.close()
        if os.path.exists(test_db):
            os.remove(test_db)


@runner.test("工号生成唯一性")
def test_employee_id_uniqueness():
    """测试工号生成的唯一性"""
    test_db = "data/test_unique.db"
    if os.path.exists(test_db):
        os.remove(test_db)
    
    repo = EmployeeRepository(db_path=test_db)
    
    try:
        hire_date = datetime(2026, 3, 18)
        ids = set()
        
        # 生成10个工号
        for _ in range(10):
            emp_id = repo.generate_employee_id(hire_date, "技术部")
            ids.add(emp_id)
        
        # 验证唯一性
        assert len(ids) == 10, f"生成的工号应该有10个唯一值，实际: {len(ids)}"
        
        print(f"    生成 {len(ids)} 个唯一工号")
    finally:
        repo.close()
        if os.path.exists(test_db):
            os.remove(test_db)


# ==================== 事务回滚测试 ====================

@runner.test("批量导入事务回滚")
def test_batch_import_rollback():
    """
    测试批量导入的事务回滚
    
    场景：批次中部分数据失败，整体回滚
    """
    test_db = "data/test_rollback.db"
    if os.path.exists(test_db):
        os.remove(test_db)
    
    service = EmployeeService(repository=EmployeeRepository(db_path=test_db))
    
    try:
        # 准备数据：前2条有效，第3条无效（邮箱格式错误）
        batch_data = [
            {
                "name": "张三",
                "email": "zhangsan@test.com",
                "phone": "13800138001",
                "id_card": "110101199001011234",
                "department": "技术部",
                "position": "工程师",
                "hire_date": "2026-03-18"
            },
            {
                "name": "李四",
                "email": "lisi@test.com",
                "phone": "13800138002",
                "id_card": "110101199002021235",
                "department": "产品部",
                "position": "经理",
                "hire_date": "2026-03-18"
            },
            {
                "name": "王五",
                "email": "invalid-email-format",  # 无效邮箱
                "phone": "13800138003",
                "id_card": "110101199003031236",
                "department": "运营部",
                "position": "专员",
                "hire_date": "2026-03-18"
            }
        ]
        
        # 执行批量导入
        result = service.batch_import(batch_data)
        
        # 验证：应该失败并回滚
        assert not result.success, "导入应该失败"
        assert result.failed_count > 0, "应该有失败的记录"
        assert result.imported_count == 0, "成功导入数应该为0（已回滚）"
        
        # 验证数据库中确实没有数据
        all_employees = service.list_employees()
        assert len(all_employees) == 0, f"数据库应该为空，实际有 {len(all_employees)} 条记录"
        
        print(f"    导入失败，已回滚: {result.failed_count} 条错误")
        print(f"    错误详情: {result.errors[0]['error']}")
        
    finally:
        service.close()
        if os.path.exists(test_db):
            os.remove(test_db)


@runner.test("批量导入全部成功")
def test_batch_import_success():
    """测试批量导入全部成功的情况"""
    test_db = "data/test_success.db"
    if os.path.exists(test_db):
        os.remove(test_db)
    
    service = EmployeeService(repository=EmployeeRepository(db_path=test_db))
    
    try:
        # 准备有效数据
        batch_data = [
            {
                "name": "张三",
                "email": "zhangsan@test.com",
                "phone": "13800138001",
                "id_card": "110101199001011234",
                "department": "技术部",
                "position": "工程师",
                "hire_date": "2026-03-18"
            },
            {
                "name": "李四",
                "email": "lisi@test.com",
                "phone": "13800138002",
                "id_card": "110101199002021235",
                "department": "产品部",
                "position": "经理",
                "hire_date": "2026-03-18"
            }
        ]
        
        result = service.batch_import(batch_data)
        
        # 验证成功
        assert result.success, f"导入应该成功: {result.errors}"
        assert result.imported_count == 2, f"应该导入2条，实际: {result.imported_count}"
        assert len(result.employee_ids) == 2, "应该生成2个工号"
        
        # 验证数据库中有数据
        all_employees = service.list_employees()
        assert len(all_employees) == 2, f"数据库应该有2条记录，实际: {len(all_employees)}"
        
        print(f"    导入成功: {result.imported_count} 条")
        print(f"    工号: {result.employee_ids}")
        
    finally:
        service.close()
        if os.path.exists(test_db):
            os.remove(test_db)


# ==================== 并发测试 ====================

@runner.test("工号生成并发安全")
def test_concurrent_id_generation():
    """测试并发环境下工号生成的安全性"""
    test_db = "data/test_concurrent.db"
    if os.path.exists(test_db):
        os.remove(test_db)
    
    repo = EmployeeRepository(db_path=test_db)
    generated_ids = []
    errors = []
    
    def worker():
        try:
            hire_date = datetime(2026, 3, 18)
            emp_id = repo.generate_employee_id(hire_date, "技术部")
            generated_ids.append(emp_id)
        except Exception as e:
            errors.append(str(e))
    
    try:
        # 启动5个线程同时生成工号
        threads = []
        for _ in range(5):
            t = threading.Thread(target=worker)
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # 验证
        if errors:
            print(f"    警告: 出现 {len(errors)} 个错误")
        
        # 检查唯一性
        unique_ids = set(generated_ids)
        assert len(unique_ids) == len(generated_ids), \
            f"工号不唯一！生成{len(generated_ids)}个，唯一{len(unique_ids)}个"
        
        print(f"    并发生成 {len(generated_ids)} 个唯一工号")
        
    finally:
        repo.close()
        if os.path.exists(test_db):
            os.remove(test_db)


# ==================== 运行测试 ====================

if __name__ == "__main__":
    success = runner.run()
    sys.exit(0 if success else 1)
