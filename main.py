# -*- coding: utf-8 -*-
"""
程序入口 (Presentation Layer)

职责：
- CLI菜单交互
- 组装各模块
- 提供简单的命令行接口
"""
import sys
import json
import argparse
from datetime import date
from typing import Optional

from config import config
from models import Employee
from services import EmployeeService


def print_header():
    """打印程序标题"""
    print("\n" + "=" * 60)
    print("员工登记系统 v2.0")
    print("=" * 60)


def print_menu():
    """打印主菜单"""
    print("\n请选择操作：")
    print("1. 录入新员工")
    print("2. 批量导入（JSON文件）")
    print("3. 查询员工")
    print("4. 列出所有员工")
    print("5. 部门统计")
    print("0. 退出")
    print("-" * 60)


def input_employee_data() -> dict:
    """交互式输入员工数据"""
    print("\n请输入员工信息：")
    
    data = {}
    data['name'] = input("姓名: ").strip()
    data['email'] = input("邮箱: ").strip()
    data['phone'] = input("手机号: ").strip()
    data['id_card'] = input("身份证号: ").strip()
    data['department'] = input("部门: ").strip()
    data['position'] = input("职位: ").strip()
    
    # 入职日期，默认今天
    hire_date_str = input("入职日期 (YYYY-MM-DD，默认今天): ").strip()
    if hire_date_str:
        data['hire_date'] = hire_date_str
    else:
        data['hire_date'] = date.today().isoformat()
    
    # 可选字段
    gender = input("性别 (男/女/其他，默认男): ").strip()
    if gender:
        data['gender'] = gender
    
    address = input("住址 (可选): ").strip()
    if address:
        data['address'] = address
    
    return data


def cmd_add_employee(service: EmployeeService):
    """添加员工命令"""
    try:
        data = input_employee_data()
        employee = service.create_employee(data, operator="admin")
        
        print("\n✅ 员工录入成功！")
        print(f"   工号: {employee.employee_id}")
        print(f"   姓名: {employee.name}")
        print(f"   部门: {employee.department}")
        print(f"   邮箱: {employee.email}")
        
    except Exception as e:
        print(f"\n❌ 录入失败: {e}")


def cmd_batch_import(service: EmployeeService, file_path: Optional[str] = None):
    """批量导入命令"""
    if not file_path:
        file_path = input("请输入JSON文件路径: ").strip()
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            employees_data = json.load(f)
        
        if not isinstance(employees_data, list):
            print("❌ 文件格式错误：必须是JSON数组")
            return
        
        print(f"\n开始导入 {len(employees_data)} 条记录...")
        
        result = service.batch_import(employees_data, operator="admin")
        
        print("\n" + "=" * 60)
        print("导入结果:")
        print("=" * 60)
        print(f"成功: {result.imported_count} 条")
        print(f"失败: {result.failed_count} 条")
        
        if result.success:
            print("\n✅ 批量导入成功！")
            print(f"生成的工号: {', '.join(result.employee_ids[:5])}")
            if len(result.employee_ids) > 5:
                print(f"... 等共 {len(result.employee_ids)} 个")
        else:
            print("\n❌ 批量导入失败，已回滚")
            if result.errors:
                print("\n错误详情:")
                for error in result.errors[:5]:
                    print(f"  第{error['index']+1}条: {error['error']}")
        
    except FileNotFoundError:
        print(f"❌ 文件不存在: {file_path}")
    except json.JSONDecodeError as e:
        print(f"❌ JSON格式错误: {e}")
    except Exception as e:
        print(f"❌ 导入失败: {e}")


def cmd_query_employee(service: EmployeeService):
    """查询员工命令"""
    employee_id = input("请输入工号: ").strip()
    
    employee = service.get_employee(employee_id)
    
    if employee:
        print("\n" + "=" * 60)
        print("员工信息")
        print("=" * 60)
        print(f"工号: {employee.employee_id}")
        print(f"姓名: {employee.name}")
        print(f"性别: {employee.gender.value}")
        print(f"部门: {employee.department}")
        print(f"职位: {employee.position}")
        print(f"邮箱: {employee.email}")
        print(f"手机: {employee.phone}")
        print(f"身份证: {employee.id_card[:6]}********{employee.id_card[-4:]}")
        print(f"入职日期: {employee.hire_date}")
        print(f"状态: {employee.status.value}")
        print(f"创建时间: {employee.created_at}")
    else:
        print(f"\n❌ 未找到工号为 {employee_id} 的员工")


def cmd_list_employees(service: EmployeeService):
    """列出员工命令"""
    department = input("请输入部门名称（留空查询全部）: ").strip()
    
    employees = service.list_employees(
        department=department if department else None,
        limit=50
    )
    
    if employees:
        print("\n" + "=" * 60)
        print(f"员工列表（共 {len(employees)} 人）")
        print("=" * 60)
        print(f"{'工号':<15} {'姓名':<10} {'部门':<10} {'职位':<10} {'状态':<8}")
        print("-" * 60)
        for emp in employees:
            print(f"{emp.employee_id:<15} {emp.name:<10} {emp.department:<10} "
                  f"{emp.position:<10} {emp.status.value:<8}")
    else:
        print("\n暂无员工记录")


def cmd_department_stats(service: EmployeeService):
    """部门统计命令"""
    stats = service.get_department_stats()
    
    if stats:
        print("\n" + "=" * 60)
        print("各部门人数统计")
        print("=" * 60)
        print(f"{'部门':<20} {'人数':<10}")
        print("-" * 60)
        for dept, count in sorted(stats.items(), key=lambda x: -x[1]):
            print(f"{dept:<20} {count:<10}")
        print("-" * 60)
        print(f"{'总计':<20} {sum(stats.values()):<10}")
    else:
        print("\n暂无数据")


def create_sample_json():
    """创建示例JSON文件"""
    sample_data = [
        {
            "name": "张三",
            "gender": "男",
            "email": "zhangsan@company.com",
            "phone": "13800138001",
            "id_card": "110101199001011234",
            "department": "技术部",
            "position": "高级工程师",
            "hire_date": "2026-03-18"
        },
        {
            "name": "李四",
            "gender": "女",
            "email": "lisi@company.com",
            "phone": "13800138002",
            "id_card": "110101199002021235",
            "department": "产品部",
            "position": "产品经理",
            "hire_date": "2026-03-18"
        },
        {
            "name": "王五",
            "gender": "男",
            "email": "wangwu@company.com",
            "phone": "13800138003",
            "id_card": "110101199003031236",
            "department": "技术部",
            "position": "前端工程师",
            "hire_date": "2026-03-18"
        }
    ]
    
    import os
    os.makedirs("data", exist_ok=True)
    
    with open("data/sample_employees.json", "w", encoding="utf-8") as f:
        json.dump(sample_data, f, ensure_ascii=False, indent=2)
    
    print("✅ 已创建示例文件: data/sample_employees.json")


def interactive_mode(service: EmployeeService):
    """交互模式"""
    while True:
        print_menu()
        choice = input("请输入选项: ").strip()
        
        if choice == "1":
            cmd_add_employee(service)
        elif choice == "2":
            cmd_batch_import(service)
        elif choice == "3":
            cmd_query_employee(service)
        elif choice == "4":
            cmd_list_employees(service)
        elif choice == "5":
            cmd_department_stats(service)
        elif choice == "0":
            print("\n感谢使用，再见！")
            break
        else:
            print("\n❌ 无效选项，请重新输入")


def main():
    """主入口"""
    parser = argparse.ArgumentParser(description="员工登记系统")
    parser.add_argument("--import", dest="import_file", help="批量导入JSON文件")
    parser.add_argument("--create-sample", action="store_true", help="创建示例JSON文件")
    parser.add_argument("--interactive", "-i", action="store_true", help="交互模式")
    
    args = parser.parse_args()
    
    print_header()
    
    # 初始化服务
    service = EmployeeService()
    
    try:
        if args.create_sample:
            create_sample_json()
        elif args.import_file:
            cmd_batch_import(service, args.import_file)
        elif args.interactive or len(sys.argv) == 1:
            interactive_mode(service)
        else:
            parser.print_help()
    finally:
        service.close()


if __name__ == "__main__":
    main()
