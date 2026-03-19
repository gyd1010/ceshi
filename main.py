"""
程序入口：提供简单的CLI菜单，组装各模块完成用户交互。
"""
import json
from typing import List

from config import DEPARTMENT_CODES
from models import Employee, BatchImportResult
from services import EmployeeService, DepartmentService
from repository import DatabaseConnection


def print_header(title: str) -> None:
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def print_employee(emp: Employee) -> None:
    print(f"\n工号: {emp.employee_no}")
    print(f"姓名: {emp.name}")
    print(f"部门: {emp.department} ({emp.department_code})")
    print(f"邮箱: {emp.email}")
    print(f"手机: {emp.phone}")
    print(f"身份证: {emp.id_card}")
    print(f"入职日期: {emp.hire_date}")


def menu_add_employee(service: EmployeeService) -> None:
    print_header("添加员工")

    print("\n可选部门:")
    for dept, code in DEPARTMENT_CODES.items():
        print(f"  {dept}: {code}")

    try:
        name = input("\n请输入姓名: ").strip()
        department = input("请输入部门: ").strip()
        email = input("请输入邮箱: ").strip()
        phone = input("请输入手机号: ").strip()
        id_card = input("请输入身份证号: ").strip()
        hire_date = input("请输入入职日期(YYYY-MM-DD): ").strip()
        operator = input("请输入操作人(默认: admin): ").strip() or "admin"

        result = service.create_employee(
            name=name,
            department=department,
            email=email,
            phone=phone,
            id_card=id_card,
            hire_date=hire_date,
            operator=operator,
        )

        if result.success:
            print(f"\n✅ 员工创建成功！工号: {result.employee_no}")
        else:
            print(f"\n❌ 创建失败: {result.error}")

    except KeyboardInterrupt:
        print("\n操作已取消")


def menu_batch_import(service: EmployeeService) -> None:
    print_header("批量导入员工")

    print("\n请输入员工JSON数据（输入'demo'使用示例数据）:")
    json_input = input().strip()

    if json_input.lower() == "demo":
        employees_data = [
            {
                "name": "张三",
                "department": "技术部",
                "email": "zhangsan@example.com",
                "phone": "13800138001",
                "id_card": "110101199001011234",
                "hire_date": "2026-01-15",
            },
            {
                "name": "李四",
                "department": "产品部",
                "email": "lisi@example.com",
                "phone": "13800138002",
                "id_card": "110101199002022345",
                "hire_date": "2026-01-15",
            },
            {
                "name": "王五",
                "department": "运营部",
                "email": "invalid-email",
                "phone": "13800138003",
                "id_card": "110101199003033456",
                "hire_date": "2026-01-15",
            },
        ]
        print("\n使用示例数据:")
        print(json.dumps(employees_data, ensure_ascii=False, indent=2))
    else:
        try:
            employees_data = json.loads(json_input)
        except json.JSONDecodeError:
            print("❌ JSON格式错误")
            return

    operator = input("\n请输入操作人(默认: admin): ").strip() or "admin"

    result = service.batch_import(employees_data, operator)

    print("\n" + "-" * 40)
    print(f"总数: {result.total_count}")
    print(f"成功: {result.success_count}")
    print(f"失败: {result.failed_count}")

    if result.success:
        print("\n✅ 批量导入成功！")
        print("生成的工号:")
        for emp_no in result.employee_numbers:
            print(f"  - {emp_no}")

    if result.errors:
        print("\n❌ 错误详情:")
        for err in result.errors:
            print(f"  - {err}")


def menu_list_employees(service: EmployeeService) -> None:
    print_header("员工列表")

    employees = service.get_all_employees()

    if not employees:
        print("\n暂无员工数据")
        return

    print(f"\n共 {len(employees)} 名员工:\n")
    for emp in employees:
        print(f"  [{emp.employee_no}] {emp.name} - {emp.department}")


def menu_view_employee(service: EmployeeService) -> None:
    print_header("查看员工详情")

    employee_no = input("\n请输入工号: ").strip()

    employee = service.get_employee(employee_no)

    if employee:
        print_employee(employee)
    else:
        print(f"\n❌ 未找到工号为 {employee_no} 的员工")


def menu_delete_employee(service: EmployeeService) -> None:
    print_header("删除员工")

    employee_no = input("\n请输入要删除的工号: ").strip()
    operator = input("请输入操作人(默认: admin): ").strip() or "admin"

    confirm = input(f"\n确认删除工号 {employee_no} 的员工？(y/n): ").strip().lower()
    if confirm == "y":
        success, message = service.delete_employee(employee_no, operator)
        if success:
            print(f"\n✅ {message}")
        else:
            print(f"\n❌ {message}")
    else:
        print("已取消")


def menu_departments() -> None:
    print_header("部门列表")

    print("\n部门代码映射表:\n")
    for dept, code in DEPARTMENT_CODES.items():
        print(f"  {dept}: {code}")


def main() -> None:
    """
    主程序入口。
    """
    service = EmployeeService()

    while True:
        print_header("员工登记系统")
        print("\n  1. 添加员工")
        print("  2. 批量导入")
        print("  3. 查看员工列表")
        print("  4. 查看员工详情")
        print("  5. 删除员工")
        print("  6. 查看部门列表")
        print("  0. 退出")

        choice = input("\n请选择操作: ").strip()

        if choice == "1":
            menu_add_employee(service)
        elif choice == "2":
            menu_batch_import(service)
        elif choice == "3":
            menu_list_employees(service)
        elif choice == "4":
            menu_view_employee(service)
        elif choice == "5":
            menu_delete_employee(service)
        elif choice == "6":
            menu_departments()
        elif choice == "0":
            print("\n感谢使用，再见！")
            DatabaseConnection().close()
            break
        else:
            print("\n无效的选择，请重新输入")

        input("\n按回车键继续...")


if __name__ == "__main__":
    main()
