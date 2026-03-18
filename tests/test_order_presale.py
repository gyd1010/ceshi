# -*- coding: utf-8 -*-
"""
预售订单完整测试模块
包含单元测试+集成测试+并发测试
覆盖正常流程、异常流程、并发场景
"""
import asyncio
import pytest
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from models.order import Order, OrderItem, OrderStatus, OrderType, PresaleStatus
from models.inventory import Inventory, InventoryLock, InventoryLog, InventoryLockStatus, InventoryLogType
from models.base import get_db_session, init_db, close_db
from services.order_service import OrderService
from services.inventory_service import InventoryService
from utils.exceptions import (
    OrderNotFoundException,
    OrderStatusException,
    InventoryInsufficientException,
    PresaleTimeException,
    PresaleStatusException,
    PaymentAmountException,
    IdempotencyException
)
from utils.loggers import LogContext


# ==================== Fixtures ====================

@pytest.fixture(scope="session")
def event_loop():
    """创建事件循环"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session", autouse=True)
async def setup_database():
    """设置测试数据库"""
    await init_db()
    yield
    await close_db()


@pytest.fixture
async def db_session():
    """数据库会话"""
    async with get_db_session() as session:
        yield session


@pytest.fixture
async def order_service():
    """订单服务"""
    service = OrderService()
    yield service
    await service.close()


@pytest.fixture
async def inventory_service():
    """库存服务"""
    service = InventoryService()
    yield service
    await service.close()


@pytest.fixture
async def test_user_id():
    """测试用户ID"""
    return f"test_user_{uuid.uuid4().hex[:8]}"


@pytest.fixture
async def test_sku():
    """创建测试SKU"""
    async with get_db_session() as session:
        sku_id = f"test_sku_{uuid.uuid4().hex[:8]}"
        inventory = Inventory(
            sku_id=sku_id,
            spu_id="test_spu_001",
            available_stock=1000,
            locked_stock=0,
            sold_stock=0,
            presale_stock=500,
            presale_locked=0,
            presale_sold=0,
            safety_stock=10
        )
        session.add(inventory)
        await session.flush()
        return sku_id


@pytest.fixture
async def test_presale_sku():
    """创建测试预售SKU"""
    async with get_db_session() as session:
        sku_id = f"test_presale_sku_{uuid.uuid4().hex[:8]}"
        inventory = Inventory(
            sku_id=sku_id,
            spu_id="test_spu_002",
            available_stock=0,
            locked_stock=0,
            sold_stock=0,
            presale_stock=1000,
            presale_locked=0,
            presale_sold=0,
            is_presale=True,
            safety_stock=0
        )
        session.add(inventory)
        await session.flush()
        return sku_id


@pytest.fixture
def shipping_address():
    """测试收货地址"""
    return {
        "name": "测试用户",
        "phone": "13800138000",
        "province": "广东省",
        "city": "深圳市",
        "district": "南山区",
        "address": "科技园南路88号",
        "zip_code": "518000"
    }


@pytest.fixture
def presale_config():
    """测试预售配置"""
    now = datetime.utcnow()
    return {
        "deposit_amount": Decimal("100.00"),
        "final_payment_amount": Decimal("900.00"),
        "presale_start_time": now - timedelta(days=1),
        "presale_end_time": now + timedelta(days=7),
        "final_payment_start_time": now + timedelta(days=8),
        "final_payment_end_time": now + timedelta(days=14)
    }


# ==================== 单元测试 ====================

class TestOrderModel:
    """订单模型单元测试"""
    
    @pytest.mark.asyncio
    async def test_create_normal_order(self, db_session, test_user_id, test_sku, shipping_address):
        """测试创建普通订单"""
        with LogContext(trace_id=f"test_{uuid.uuid4().hex[:8]}", user_id=test_user_id):
            order = Order(
                order_no=f"N{datetime.utcnow().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:6].upper()}",
                user_id=test_user_id,
                order_type=OrderType.NORMAL,
                status=OrderStatus.PENDING_PAYMENT,
                total_amount=Decimal("999.00"),
                pay_amount=Decimal("999.00"),
                expire_at=datetime.utcnow() + timedelta(minutes=30)
            )
            db_session.add(order)
            await db_session.flush()
            
            assert order.id is not None
            assert order.order_no.startswith("N")
            assert order.status == OrderStatus.PENDING_PAYMENT
    
    @pytest.mark.asyncio
    async def test_create_presale_order(self, db_session, test_user_id, test_presale_sku, shipping_address, presale_config):
        """测试创建预售订单"""
        with LogContext(trace_id=f"test_{uuid.uuid4().hex[:8]}", user_id=test_user_id):
            order = Order(
                order_no=f"P{datetime.utcnow().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:6].upper()}",
                user_id=test_user_id,
                order_type=OrderType.PRESALE,
                status=OrderStatus.PRESALE_DEPOSIT_PENDING,
                presale_status=PresaleStatus.DEPOSIT_PHASE,
                total_amount=presale_config["deposit_amount"] + presale_config["final_payment_amount"],
                deposit_amount=presale_config["deposit_amount"],
                final_payment_amount=presale_config["final_payment_amount"],
                pay_amount=presale_config["deposit_amount"],
                presale_start_time=presale_config["presale_start_time"],
                presale_end_time=presale_config["presale_end_time"],
                final_payment_start_time=presale_config["final_payment_start_time"],
                final_payment_end_time=presale_config["final_payment_end_time"],
                expire_at=datetime.utcnow() + timedelta(hours=24)
            )
            db_session.add(order)
            await db_session.flush()
            
            assert order.id is not None
            assert order.order_no.startswith("P")
            assert order.is_presale is True
            assert order.status == OrderStatus.PRESALE_DEPOSIT_PENDING
    
    @pytest.mark.asyncio
    async def test_order_status_transition(self, db_session, test_user_id):
        """测试订单状态流转"""
        order = Order(
            order_no=f"N{datetime.utcnow().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:6].upper()}",
            user_id=test_user_id,
            order_type=OrderType.NORMAL,
            status=OrderStatus.PENDING_PAYMENT,
            total_amount=Decimal("100.00"),
            pay_amount=Decimal("100.00")
        )
        db_session.add(order)
        await db_session.flush()
        
        # 测试正常状态流转
        assert order.transition_to(OrderStatus.PAID) is True
        assert order.status == OrderStatus.PAID
        
        assert order.transition_to(OrderStatus.PROCESSING) is True
        assert order.status == OrderStatus.PROCESSING
        
        # 测试非法状态流转
        assert order.transition_to(OrderStatus.PENDING_PAYMENT) is False


class TestInventoryModel:
    """库存模型单元测试"""
    
    @pytest.mark.asyncio
    async def test_inventory_lock(self, db_session, test_sku):
        """测试库存锁定"""
        result = await db_session.execute(
            select(Inventory).where(Inventory.sku_id == test_sku)
        )
        inventory = result.scalar_one()
        
        initial_available = inventory.available_stock
        
        # 锁定库存
        success = inventory.lock_stock(10, is_presale=False)
        assert success is True
        assert inventory.available_stock == initial_available - 10
        assert inventory.locked_stock == 10
        
        # 解锁库存
        success = inventory.unlock_stock(10, is_presale=False)
        assert success is True
        assert inventory.available_stock == initial_available
        assert inventory.locked_stock == 0
    
    @pytest.mark.asyncio
    async def test_presale_inventory_lock(self, db_session, test_presale_sku):
        """测试预售库存锁定"""
        result = await db_session.execute(
            select(Inventory).where(Inventory.sku_id == test_presale_sku)
        )
        inventory = result.scalar_one()
        
        # 锁定预售库存
        success = inventory.lock_stock(50, is_presale=True)
        assert success is True
        assert inventory.presale_locked == 50
        
        # 扣减预售库存
        success = inventory.deduct_stock(50, is_presale=True)
        assert success is True
        assert inventory.presale_locked == 0
        assert inventory.presale_sold == 50
    
    @pytest.mark.asyncio
    async def test_insufficient_inventory(self, db_session, test_sku):
        """测试库存不足"""
        result = await db_session.execute(
            select(Inventory).where(Inventory.sku_id == test_sku)
        )
        inventory = result.scalar_one()
        
        # 尝试锁定超过可用库存
        success = inventory.lock_stock(999999, is_presale=False)
        assert success is False


# ==================== 集成测试 ====================

class TestOrderServiceIntegration:
    """订单服务集成测试"""
    
    @pytest.mark.asyncio
    async def test_create_and_pay_normal_order(
        self, order_service, test_user_id, test_sku, shipping_address
    ):
        """测试创建并支付普通订单完整流程"""
        with LogContext(trace_id=f"test_{uuid.uuid4().hex[:8]}", user_id=test_user_id):
            # 创建订单
            items = [{
                "sku_id": test_sku,
                "spu_id": "test_spu_001",
                "sku_name": "测试商品",
                "quantity": 2,
                "price": Decimal("199.99")
            }]
            
            order = await order_service.create_order(
                user_id=test_user_id,
                items=items,
                shipping_address=shipping_address,
                order_type=OrderType.NORMAL
            )
            
            assert order is not None
            assert order.status == OrderStatus.PENDING_PAYMENT
            
            # 支付订单
            paid_order = await order_service.pay_order(
                order_id=order.id,
                payment_amount=order.pay_amount,
                payment_method="wechat",
                payment_no=f"WX{uuid.uuid4().hex[:16].upper()}"
            )
            
            assert paid_order.status == OrderStatus.PAID
    
    @pytest.mark.asyncio
    async def test_create_presale_order_full_flow(
        self, order_service, test_user_id, test_presale_sku, 
        shipping_address, presale_config
    ):
        """测试预售订单完整流程"""
        with LogContext(trace_id=f"test_{uuid.uuid4().hex[:8]}", user_id=test_user_id):
            # 创建预售订单
            items = [{
                "sku_id": test_presale_sku,
                "spu_id": "test_spu_002",
                "sku_name": "预售商品",
                "quantity": 1,
                "original_price": Decimal("1200.00"),
                "sale_price": Decimal("1000.00")
            }]
            
            order = await order_service.create_order(
                user_id=test_user_id,
                items=items,
                shipping_address=shipping_address,
                order_type=OrderType.PRESALE,
                presale_config=presale_config
            )
            
            assert order is not None
            assert order.order_type == OrderType.PRESALE
            assert order.status == OrderStatus.PRESALE_DEPOSIT_PENDING
            
            # 支付定金
            deposit_paid = await order_service.pay_order(
                order_id=order.id,
                payment_amount=order.deposit_amount,
                payment_method="alipay",
                payment_no=f"AL{uuid.uuid4().hex[:16].upper()}",
                payment_type="deposit"
            )
            
            assert deposit_paid.status == OrderStatus.PRESALE_DEPOSIT_PAID
            
            # 模拟时间推进到尾款支付期
            # 这里我们需要直接修改订单状态来测试尾款支付
            async with get_db_session() as session:
                result = await session.execute(
                    select(Order).where(Order.id == order.id)
                )
                order_in_db = result.scalar_one()
                order_in_db.status = OrderStatus.PRESALE_DEPOSIT_PAID
                await session.flush()
            
            # 支付尾款
            final_paid = await order_service.pay_order(
                order_id=order.id,
                payment_amount=order.final_payment_amount,
                payment_method="alipay",
                payment_no=f"AL{uuid.uuid4().hex[:16].upper()}",
                payment_type="final"
            )
            
            assert final_paid.status == OrderStatus.PRESALE_FINAL_PAYMENT_PAID
    
    @pytest.mark.asyncio
    async def test_cancel_order(
        self, order_service, test_user_id, test_sku, shipping_address
    ):
        """测试取消订单"""
        with LogContext(trace_id=f"test_{uuid.uuid4().hex[:8]}", user_id=test_user_id):
            # 创建订单
            items = [{
                "sku_id": test_sku,
                "spu_id": "test_spu_001",
                "sku_name": "测试商品",
                "quantity": 1,
                "price": Decimal("99.99")
            }]
            
            order = await order_service.create_order(
                user_id=test_user_id,
                items=items,
                shipping_address=shipping_address,
                order_type=OrderType.NORMAL
            )
            
            # 取消订单
            cancelled = await order_service.cancel_order(
                order_id=order.id,
                reason="用户主动取消"
            )
            
            assert cancelled.status == OrderStatus.CANCELLED
            assert cancelled.cancel_reason == "用户主动取消"


class TestInventoryServiceIntegration:
    """库存服务集成测试"""
    
    @pytest.mark.asyncio
    async def test_lock_and_deduct_inventory(
        self, inventory_service, test_sku
    ):
        """测试锁定并扣减库存"""
        async with get_db_session() as session:
            order_id = f"test_order_{uuid.uuid4().hex[:8]}"
            
            # 锁定库存
            lock = await inventory_service.lock_stock(
                session,
                order_id=order_id,
                sku_id=test_sku,
                quantity=5,
                is_presale=False,
                expire_minutes=30
            )
            
            assert lock is not None
            assert lock.quantity == 5
            assert lock.status == InventoryLockStatus.LOCKED
            
            # 扣减库存
            success = await inventory_service.deduct_stock(
                session,
                order_id=order_id,
                sku_id=test_sku,
                quantity=5,
                is_presale=False
            )
            
            assert success is True
            
            # 验证库存状态
            status = await inventory_service.get_inventory_status(session, test_sku)
            assert status["sold_stock"] >= 5
    
    @pytest.mark.asyncio
    async def test_insufficient_inventory_exception(
        self, inventory_service, test_sku
    ):
        """测试库存不足异常"""
        async with get_db_session() as session:
            order_id = f"test_order_{uuid.uuid4().hex[:8]}"
            
            with pytest.raises(InventoryInsufficientException):
                await inventory_service.lock_stock(
                    session,
                    order_id=order_id,
                    sku_id=test_sku,
                    quantity=999999,  # 超过可用库存
                    is_presale=False
                )


# ==================== 异常流程测试 ====================

class TestExceptionFlows:
    """异常流程测试"""
    
    @pytest.mark.asyncio
    async def test_pay_nonexistent_order(self, order_service):
        """测试支付不存在的订单"""
        with pytest.raises(OrderNotFoundException):
            await order_service.pay_order(
                order_id="nonexistent_order_id",
                payment_amount=Decimal("100.00"),
                payment_method="wechat",
                payment_no="WX123456"
            )
    
    @pytest.mark.asyncio
    async def test_pay_wrong_amount(self, order_service, test_user_id, test_sku, shipping_address):
        """测试支付错误金额"""
        with LogContext(trace_id=f"test_{uuid.uuid4().hex[:8]}", user_id=test_user_id):
            items = [{
                "sku_id": test_sku,
                "spu_id": "test_spu_001",
                "sku_name": "测试商品",
                "quantity": 1,
                "price": Decimal("100.00")
            }]
            
            order = await order_service.create_order(
                user_id=test_user_id,
                items=items,
                shipping_address=shipping_address,
                order_type=OrderType.NORMAL
            )
            
            with pytest.raises(PaymentAmountException):
                await order_service.pay_order(
                    order_id=order.id,
                    payment_amount=Decimal("99.99"),  # 错误金额
                    payment_method="wechat",
                    payment_no="WX123456"
                )
    
    @pytest.mark.asyncio
    async def test_cancel_paid_order(self, order_service, test_user_id, test_sku, shipping_address):
        """测试取消已支付订单（应该失败）"""
        with LogContext(trace_id=f"test_{uuid.uuid4().hex[:8]}", user_id=test_user_id):
            items = [{
                "sku_id": test_sku,
                "spu_id": "test_spu_001",
                "sku_name": "测试商品",
                "quantity": 1,
                "price": Decimal("100.00")
            }]
            
            order = await order_service.create_order(
                user_id=test_user_id,
                items=items,
                shipping_address=shipping_address,
                order_type=OrderType.NORMAL
            )
            
            # 支付订单
            await order_service.pay_order(
                order_id=order.id,
                payment_amount=order.pay_amount,
                payment_method="wechat",
                payment_no=f"WX{uuid.uuid4().hex[:16].upper()}"
            )
            
            # 尝试取消已支付订单
            with pytest.raises(OrderStatusException):
                await order_service.cancel_order(
                    order_id=order.id,
                    reason="测试取消"
                )


# ==================== 并发测试 ====================

class TestConcurrency:
    """并发测试"""
    
    @pytest.mark.asyncio
    async def test_concurrent_inventory_lock(
        self, inventory_service, test_sku
    ):
        """测试并发库存锁定（防止超卖）"""
        async with get_db_session() as session:
            # 获取初始库存
            result = await session.execute(
                select(Inventory).where(Inventory.sku_id == test_sku)
            )
            inventory = result.scalar_one()
            initial_available = inventory.available_stock
        
        # 并发创建多个订单
        async def create_order_task(task_id: int):
            try:
                async with get_db_session() as session:
                    order_id = f"concurrent_order_{task_id}_{uuid.uuid4().hex[:8]}"
                    lock = await inventory_service.lock_stock(
                        session,
                        order_id=order_id,
                        sku_id=test_sku,
                        quantity=10,
                        is_presale=False,
                        expire_minutes=30
                    )
                    return True, lock
            except InventoryInsufficientException:
                return False, None
            except Exception as e:
                return False, str(e)
        
        # 启动100个并发任务
        tasks = [create_order_task(i) for i in range(100)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 统计成功数量
        success_count = sum(1 for r in results if isinstance(r, tuple) and r[0])
        
        # 验证没有超卖
        async with get_db_session() as session:
            status = await inventory_service.get_inventory_status(session, test_sku)
            locked = status["locked_stock"]
            available = status["available_stock"]
            
            # 锁定的库存不能超过初始库存
            assert locked <= initial_available
            # 可用库存不能为负
            assert available >= 0
            # 总库存守恒
            assert available + locked + status["sold_stock"] <= initial_available + status["sold_stock"]
    
    @pytest.mark.asyncio
    async def test_concurrent_presale_orders(
        self, order_service, test_user_id, test_presale_sku, 
        shipping_address, presale_config
    ):
        """测试并发预售订单创建"""
        async with get_db_session() as session:
            # 获取预售库存
            result = await session.execute(
                select(Inventory).where(Inventory.sku_id == test_presale_sku)
            )
            inventory = result.scalar_one()
            initial_presale = inventory.presale_stock
        
        async def create_presale_task(task_id: int):
            try:
                with LogContext(trace_id=f"concurrent_{task_id}", user_id=test_user_id):
                    items = [{
                        "sku_id": test_presale_sku,
                        "spu_id": "test_spu_002",
                        "sku_name": "预售商品",
                        "quantity": 1,
                        "original_price": Decimal("1200.00"),
                        "sale_price": Decimal("1000.00")
                    }]
                    
                    order = await order_service.create_order(
                        user_id=test_user_id,
                        items=items,
                        shipping_address=shipping_address,
                        order_type=OrderType.PRESALE,
                        presale_config=presale_config,
                        idempotency_key=f"concurrent_{task_id}_{uuid.uuid4().hex[:8]}"
                    )
                    return True, order
            except InventoryInsufficientException:
                return False, "insufficient"
            except Exception as e:
                return False, str(e)
        
        # 启动200个并发任务
        tasks = [create_presale_task(i) for i in range(200)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 统计成功数量
        success_count = sum(1 for r in results if isinstance(r, tuple) and r[0])
        
        # 验证没有超卖
        async with get_db_session() as session:
            result = await session.execute(
                select(Inventory).where(Inventory.sku_id == test_presale_sku)
            )
            inventory = result.scalar_one()
            
            # 预售锁定不能超过总预售库存
            assert inventory.presale_locked <= initial_presale
            # 已售+锁定不能超过总库存
            assert inventory.presale_sold + inventory.presale_locked <= initial_presale


# ==================== 边界场景测试 ====================

class TestEdgeCases:
    """边界场景测试"""
    
    @pytest.mark.asyncio
    async def test_cancel_and_reorder(
        self, order_service, test_user_id, test_sku, shipping_address
    ):
        """测试取消后重新下单"""
        with LogContext(trace_id=f"test_{uuid.uuid4().hex[:8]}", user_id=test_user_id):
            items = [{
                "sku_id": test_sku,
                "spu_id": "test_spu_001",
                "sku_name": "测试商品",
                "quantity": 5,
                "price": Decimal("99.99")
            }]
            
            # 第一次下单
            order1 = await order_service.create_order(
                user_id=test_user_id,
                items=items,
                shipping_address=shipping_address,
                order_type=OrderType.NORMAL
            )
            
            # 取消订单
            await order_service.cancel_order(
                order_id=order1.id,
                reason="测试取消后重新下单"
            )
            
            # 重新下单
            order2 = await order_service.create_order(
                user_id=test_user_id,
                items=items,
                shipping_address=shipping_address,
                order_type=OrderType.NORMAL
            )
            
            assert order2.id != order1.id
            assert order2.status == OrderStatus.PENDING_PAYMENT
    
    @pytest.mark.asyncio
    async def test_idempotency(
        self, order_service, test_user_id, test_sku, shipping_address
    ):
        """测试幂等性"""
        with LogContext(trace_id=f"test_{uuid.uuid4().hex[:8]}", user_id=test_user_id):
            items = [{
                "sku_id": test_sku,
                "spu_id": "test_spu_001",
                "sku_name": "测试商品",
                "quantity": 1,
                "price": Decimal("99.99")
            }]
            
            idempotency_key = f"test_idempotent_{uuid.uuid4().hex[:8]}"
            
            # 第一次创建
            order1 = await order_service.create_order(
                user_id=test_user_id,
                items=items,
                shipping_address=shipping_address,
                order_type=OrderType.NORMAL,
                idempotency_key=idempotency_key
            )
            
            # 第二次使用相同幂等键创建
            with pytest.raises(IdempotencyException):
                await order_service.create_order(
                    user_id=test_user_id,
                    items=items,
                    shipping_address=shipping_address,
                    order_type=OrderType.NORMAL,
                    idempotency_key=idempotency_key
                )
    
    @pytest.mark.asyncio
    async def test_presale_time_validation(
        self, order_service, test_user_id, test_presale_sku, shipping_address
    ):
        """测试预售时间验证"""
        with LogContext(trace_id=f"test_{uuid.uuid4().hex[:8]}", user_id=test_user_id):
            # 预售时间已过期的配置
            expired_config = {
                "deposit_amount": Decimal("100.00"),
                "final_payment_amount": Decimal("900.00"),
                "presale_start_time": datetime.utcnow() - timedelta(days=10),
                "presale_end_time": datetime.utcnow() - timedelta(days=5),  # 已过期
                "final_payment_start_time": datetime.utcnow() - timedelta(days=4),
                "final_payment_end_time": datetime.utcnow() - timedelta(days=1)
            }
            
            items = [{
                "sku_id": test_presale_sku,
                "spu_id": "test_spu_002",
                "sku_name": "预售商品",
                "quantity": 1,
                "original_price": Decimal("1200.00"),
                "sale_price": Decimal("1000.00")
            }]
            
            with pytest.raises(PresaleTimeException):
                await order_service.create_order(
                    user_id=test_user_id,
                    items=items,
                    shipping_address=shipping_address,
                    order_type=OrderType.PRESALE,
                    presale_config=expired_config
                )


# ==================== 主函数（用于直接运行测试） ====================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
