# -*- coding: utf-8 -*-
"""
订单服务模块
彻底解耦巨型方法，拆分为4个子服务
实现预售订单状态流转核心逻辑
"""
import asyncio
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, List, Dict, Any, Tuple

from sqlalchemy import select, update, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from models.order import (
    Order, OrderItem, OrderStatus, OrderType, PresaleStatus
)
from models.inventory import InventoryLock, InventoryLockStatus
from models.base import get_db_session
from services.inventory_service import InventoryService
from utils.exceptions import (
    OrderNotFoundException,
    OrderStatusException,
    PresaleException,
    PresaleTimeException,
    PresaleStatusException,
    PaymentAmountException,
    PaymentTimeoutException,
    InventoryInsufficientException,
    IdempotencyException
)
from utils.loggers import get_logger, LogContext, log_execution_time
from config.settings import settings

logger = get_logger(__name__)


class OrderCreationService:
    """订单创建服务 - 子服务1"""
    
    def __init__(self, inventory_service: InventoryService):
        self.inventory_service = inventory_service
    
    def _generate_order_no(self, order_type: OrderType) -> str:
        """生成订单号"""
        prefix = {
            OrderType.NORMAL: "N",
            OrderType.PRESALE: "P",
            OrderType.FLASH_SALE: "F",
            OrderType.GROUP_BUY: "G",
        }.get(order_type, "O")
        
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        random_suffix = uuid.uuid4().hex[:6].upper()
        return f"{prefix}{timestamp}{random_suffix}"
    
    @log_execution_time()
    async def create_normal_order(
        self,
        session: AsyncSession,
        user_id: str,
        items: List[Dict[str, Any]],
        shipping_address: Dict[str, str],
        idempotency_key: Optional[str] = None,
        **kwargs
    ) -> Order:
        """创建普通订单"""
        trace_id = LogContext().context.get("trace_id", "")
        
        # 幂等检查
        if idempotency_key:
            existing = await session.execute(
                select(Order).where(Order.idempotency_key == idempotency_key)
            )
            if existing.scalar_one_or_none():
                raise IdempotencyException(idempotency_key, "", trace_id)
        
        # 计算金额
        total_amount = Decimal('0')
        order_items = []
        
        for item_data in items:
            sku_id = item_data["sku_id"]
            quantity = item_data["quantity"]
            price = Decimal(str(item_data["price"]))
            subtotal = price * quantity
            total_amount += subtotal
            
            order_items.append(OrderItem(
                sku_id=sku_id,
                spu_id=item_data.get("spu_id", ""),
                sku_name=item_data.get("sku_name", ""),
                sku_image=item_data.get("sku_image"),
                original_price=price,
                sale_price=price,
                quantity=quantity,
                subtotal=subtotal,
                is_presale_item=False
            ))
        
        # 创建订单
        order = Order(
            order_no=self._generate_order_no(OrderType.NORMAL),
            user_id=user_id,
            order_type=OrderType.NORMAL,
            status=OrderStatus.PENDING_PAYMENT,
            total_amount=total_amount,
            pay_amount=total_amount,
            idempotency_key=idempotency_key,
            expire_at=datetime.utcnow() + timedelta(minutes=settings.ORDER_EXPIRE_MINUTES),
            extra_data={
                "shipping_address": shipping_address,
                "source": kwargs.get("source", "app")
            }
        )
        
        session.add(order)
        await session.flush()
        
        # 关联订单项
        for item in order_items:
            item.order_id = order.id
            session.add(item)
        
        # 锁定库存
        for item in order_items:
            try:
                await self.inventory_service.lock_stock(
                    session,
                    order.id,
                    item.sku_id,
                    item.quantity,
                    is_presale=False,
                    idempotency_key=f"{idempotency_key}:{item.sku_id}" if idempotency_key else None,
                    expire_minutes=settings.ORDER_EXPIRE_MINUTES
                )
            except InventoryInsufficientException as e:
                # 回滚已锁定库存
                logger.error(f"Lock stock failed for {item.sku_id}, rolling back...")
                raise
        
        logger.info(f"Normal order created: {order.order_no}, user={user_id}")
        return order
    
    @log_execution_time()
    async def create_presale_order(
        self,
        session: AsyncSession,
        user_id: str,
        items: List[Dict[str, Any]],
        shipping_address: Dict[str, str],
        presale_config: Dict[str, Any],
        idempotency_key: Optional[str] = None,
        **kwargs
    ) -> Order:
        """
        创建预售订单
        
        presale_config: {
            "deposit_amount": 100.00,
            "final_payment_amount": 900.00,
            "presale_start_time": "2024-01-01T00:00:00",
            "presale_end_time": "2024-01-07T23:59:59",
            "final_payment_start_time": "2024-01-08T00:00:00",
            "final_payment_end_time": "2024-01-14T23:59:59"
        }
        """
        trace_id = LogContext().context.get("trace_id", "")
        
        # 检查预售时间
        now = datetime.utcnow()
        presale_start = presale_config.get("presale_start_time")
        presale_end = presale_config.get("presale_end_time")
        
        if presale_start and now < presale_start:
            raise PresaleTimeException(
                items[0]["sku_id"], now.isoformat(), 
                presale_start.isoformat(), presale_end.isoformat(),
                trace_id
            )
        if presale_end and now > presale_end:
            raise PresaleTimeException(
                items[0]["sku_id"], now.isoformat(),
                presale_start.isoformat(), presale_end.isoformat(),
                trace_id
            )
        
        # 幂等检查
        if idempotency_key:
            existing = await session.execute(
                select(Order).where(Order.idempotency_key == idempotency_key)
            )
            if existing.scalar_one_or_none():
                raise IdempotencyException(idempotency_key, "", trace_id)
        
        # 计算金额
        deposit_amount = Decimal(str(presale_config.get("deposit_amount", 0)))
        final_payment_amount = Decimal(str(presale_config.get("final_payment_amount", 0)))
        total_amount = deposit_amount + final_payment_amount
        
        order_items = []
        for item_data in items:
            sku_id = item_data["sku_id"]
            quantity = item_data["quantity"]
            
            order_items.append(OrderItem(
                sku_id=sku_id,
                spu_id=item_data.get("spu_id", ""),
                sku_name=item_data.get("sku_name", ""),
                sku_image=item_data.get("sku_image"),
                original_price=Decimal(str(item_data.get("original_price", 0))),
                sale_price=Decimal(str(item_data.get("sale_price", 0))),
                quantity=quantity,
                subtotal=Decimal(str(item_data.get("sale_price", 0))) * quantity,
                is_presale_item=True,
                presale_deposit=deposit_amount,
                presale_final_payment=final_payment_amount
            ))
        
        # 创建预售订单
        order = Order(
            order_no=self._generate_order_no(OrderType.PRESALE),
            user_id=user_id,
            order_type=OrderType.PRESALE,
            status=OrderStatus.PRESALE_DEPOSIT_PENDING,
            presale_status=PresaleStatus.DEPOSIT_PHASE,
            total_amount=total_amount,
            deposit_amount=deposit_amount,
            final_payment_amount=final_payment_amount,
            pay_amount=deposit_amount,  # 首次支付定金
            presale_start_time=presale_config.get("presale_start_time"),
            presale_end_time=presale_config.get("presale_end_time"),
            final_payment_start_time=presale_config.get("final_payment_start_time"),
            final_payment_end_time=presale_config.get("final_payment_end_time"),
            idempotency_key=idempotency_key,
            expire_at=datetime.utcnow() + timedelta(hours=settings.PRESALE_DEPOSIT_EXPIRE_HOURS),
            extra_data={
                "shipping_address": shipping_address,
                "source": kwargs.get("source", "app"),
                "presale_phase": "deposit"
            }
        )
        
        session.add(order)
        await session.flush()
        
        # 关联订单项
        for item in order_items:
            item.order_id = order.id
            session.add(item)
        
        # 锁定预售库存
        for item in order_items:
            try:
                await self.inventory_service.lock_stock(
                    session,
                    order.id,
                    item.sku_id,
                    item.quantity,
                    is_presale=True,
                    idempotency_key=f"{idempotency_key}:{item.sku_id}" if idempotency_key else None,
                    expire_minutes=settings.PRESALE_DEPOSIT_EXPIRE_HOURS * 60
                )
            except InventoryInsufficientException as e:
                logger.error(f"Lock presale stock failed for {item.sku_id}")
                raise
        
        logger.info(f"Presale order created: {order.order_no}, user={user_id}, deposit={deposit_amount}")
        return order


class PaymentService:
    """支付服务 - 子服务2"""
    
    def __init__(self, inventory_service: InventoryService):
        self.inventory_service = inventory_service
    
    @log_execution_time()
    async def pay_normal_order(
        self,
        session: AsyncSession,
        order_id: str,
        payment_amount: Decimal,
        payment_method: str,
        payment_no: str
    ) -> Order:
        """支付普通订单"""
        trace_id = LogContext().context.get("trace_id", "")
        
        # 获取订单
        result = await session.execute(
            select(Order).where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()
        
        if not order:
            raise OrderNotFoundException(order_id, trace_id)
        
        if order.order_type != OrderType.NORMAL:
            raise OrderStatusException(
                order_id, order.status.value, OrderStatus.PENDING_PAYMENT.value, trace_id
            )
        
        if order.status != OrderStatus.PENDING_PAYMENT:
            raise OrderStatusException(
                order_id, order.status.value, OrderStatus.PENDING_PAYMENT.value, trace_id
            )
        
        # 检查过期
        if order.expire_at and datetime.utcnow() > order.expire_at:
            raise PaymentTimeoutException(order_id, settings.ORDER_EXPIRE_MINUTES, trace_id)
        
        # 检查金额
        if payment_amount != order.pay_amount:
            raise PaymentAmountException(order_id, order.pay_amount, payment_amount, trace_id)
        
        # 更新订单状态
        order.status = OrderStatus.PAID
        order.paid_at = datetime.utcnow()
        order.payment_method = payment_method
        order.payment_no = payment_no
        order.version += 1
        
        await session.flush()
        
        logger.info(f"Normal order paid: {order.order_no}, amount={payment_amount}")
        return order
    
    @log_execution_time()
    async def pay_presale_deposit(
        self,
        session: AsyncSession,
        order_id: str,
        payment_amount: Decimal,
        payment_method: str,
        payment_no: str
    ) -> Order:
        """支付预售定金"""
        trace_id = LogContext().context.get("trace_id", "")
        
        result = await session.execute(
            select(Order).where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()
        
        if not order:
            raise OrderNotFoundException(order_id, trace_id)
        
        if order.order_type != OrderType.PRESALE:
            raise PresaleStatusException(
                order_id, order.status.value, OrderStatus.PRESALE_DEPOSIT_PENDING.value, trace_id
            )
        
        if order.status != OrderStatus.PRESALE_DEPOSIT_PENDING:
            raise PresaleStatusException(
                order_id, order.status.value, OrderStatus.PRESALE_DEPOSIT_PENDING.value, trace_id
            )
        
        # 检查金额
        if payment_amount != order.deposit_amount:
            raise PaymentAmountException(order_id, order.deposit_amount, payment_amount, trace_id)
        
        # 状态流转
        order.status = OrderStatus.PRESALE_DEPOSIT_PAID
        order.presale_status = PresaleStatus.FINAL_PAYMENT_PHASE
        order.deposit_paid_at = datetime.utcnow()
        order.payment_method = payment_method
        order.payment_no = payment_no
        order.pay_amount = order.final_payment_amount  # 更新待付金额为尾款
        order.expire_at = order.final_payment_end_time  # 更新过期时间为尾款截止时间
        order.version += 1
        
        await session.flush()
        
        logger.info(f"Presale deposit paid: {order.order_no}, amount={payment_amount}")
        return order
    
    @log_execution_time()
    async def pay_presale_final(
        self,
        session: AsyncSession,
        order_id: str,
        payment_amount: Decimal,
        payment_method: str,
        payment_no: str
    ) -> Order:
        """支付预售尾款"""
        trace_id = LogContext().context.get("trace_id", "")
        
        result = await session.execute(
            select(Order).where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()
        
        if not order:
            raise OrderNotFoundException(order_id, trace_id)
        
        if order.order_type != OrderType.PRESALE:
            raise PresaleStatusException(
                order_id, order.status.value, OrderStatus.PRESALE_FINAL_PAYMENT_PENDING.value, trace_id
            )
        
        if order.status != OrderStatus.PRESALE_DEPOSIT_PAID:
            raise PresaleStatusException(
                order_id, order.status.value, OrderStatus.PRESALE_FINAL_PAYMENT_PENDING.value, trace_id
            )
        
        # 检查尾款支付时间
        now = datetime.utcnow()
        if order.final_payment_start_time and now < order.final_payment_start_time:
            raise PresaleTimeException(
                order.items[0].sku_id if order.items else "",
                now.isoformat(),
                order.final_payment_start_time.isoformat(),
                order.final_payment_end_time.isoformat() if order.final_payment_end_time else "",
                trace_id
            )
        
        if order.final_payment_end_time and now > order.final_payment_end_time:
            raise PaymentTimeoutException(order_id, settings.PRESALE_FINAL_PAY_EXPIRE_HOURS, trace_id)
        
        # 检查金额
        if payment_amount != order.final_payment_amount:
            raise PaymentAmountException(order_id, order.final_payment_amount, payment_amount, trace_id)
        
        # 状态流转
        order.status = OrderStatus.PRESALE_FINAL_PAYMENT_PAID
        order.presale_status = PresaleStatus.COMPLETED
        order.final_payment_paid_at = datetime.utcnow()
        order.paid_at = datetime.utcnow()
        order.payment_method = payment_method
        order.payment_no = f"{order.payment_no},{payment_no}"  # 追加支付流水
        order.version += 1
        
        await session.flush()
        
        logger.info(f"Presale final payment paid: {order.order_no}, amount={payment_amount}")
        return order


class LogisticsService:
    """物流服务 - 子服务3"""
    
    @log_execution_time()
    async def dispatch_order(
        self,
        session: AsyncSession,
        order_id: str,
        warehouse_id: Optional[str] = None
    ) -> Order:
        """订单发货调度"""
        trace_id = LogContext().context.get("trace_id", "")
        
        result = await session.execute(
            select(Order).where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()
        
        if not order:
            raise OrderNotFoundException(order_id, trace_id)
        
        # 检查是否可以发货
        valid_statuses = [OrderStatus.PAID, OrderStatus.PRESALE_FINAL_PAYMENT_PAID]
        if order.status not in valid_statuses:
            raise OrderStatusException(
                order_id, order.status.value, "PAID or PRESALE_FINAL_PAYMENT_PAID", trace_id
            )
        
        # 生成物流单号
        shipping_no = f"SF{datetime.utcnow().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:6].upper()}"
        
        # 状态流转
        if order.order_type == OrderType.PRESALE:
            # 预售订单支付尾款后流转到处理中
            order.status = OrderStatus.PROCESSING
        else:
            order.status = OrderStatus.PROCESSING
        
        order.shipping_no = shipping_no
        order.version += 1
        
        await session.flush()
        
        logger.info(f"Order dispatched: {order.order_no}, shipping_no={shipping_no}")
        return order
    
    @log_execution_time()
    async def ship_order(
        self,
        session: AsyncSession,
        order_id: str,
        shipping_company: str = "SF"
    ) -> Order:
        """订单发货"""
        trace_id = LogContext().context.get("trace_id", "")
        
        result = await session.execute(
            select(Order).where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()
        
        if not order:
            raise OrderNotFoundException(order_id, trace_id)
        
        if order.status != OrderStatus.PROCESSING:
            raise OrderStatusException(
                order_id, order.status.value, OrderStatus.PROCESSING.value, trace_id
            )
        
        order.status = OrderStatus.SHIPPED
        order.shipped_at = datetime.utcnow()
        order.version += 1
        
        await session.flush()
        
        logger.info(f"Order shipped: {order.order_no}, company={shipping_company}")
        return order
    
    @log_execution_time()
    async def deliver_order(
        self,
        session: AsyncSession,
        order_id: str
    ) -> Order:
        """订单送达"""
        trace_id = LogContext().context.get("trace_id", "")
        
        result = await session.execute(
            select(Order).where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()
        
        if not order:
            raise OrderNotFoundException(order_id, trace_id)
        
        if order.status != OrderStatus.SHIPPED:
            raise OrderStatusException(
                order_id, order.status.value, OrderStatus.SHIPPED.value, trace_id
            )
        
        order.status = OrderStatus.DELIVERED
        order.delivered_at = datetime.utcnow()
        order.version += 1
        
        await session.flush()
        
        logger.info(f"Order delivered: {order.order_no}")
        return order
    
    @log_execution_time()
    async def complete_order(
        self,
        session: AsyncSession,
        order_id: str
    ) -> Order:
        """订单完成"""
        trace_id = LogContext().context.get("trace_id", "")
        
        result = await session.execute(
            select(Order).where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()
        
        if not order:
            raise OrderNotFoundException(order_id, trace_id)
        
        if order.status != OrderStatus.DELIVERED:
            raise OrderStatusException(
                order_id, order.status.value, OrderStatus.DELIVERED.value, trace_id
            )
        
        order.status = OrderStatus.COMPLETED
        order.version += 1
        
        await session.flush()
        
        logger.info(f"Order completed: {order.order_no}")
        return order


class OrderService:
    """
    订单服务主类 - 子服务4（协调者）
    整合所有子服务，提供统一接口
    """
    
    def __init__(self):
        self.inventory_service = InventoryService()
        self.creation_service = OrderCreationService(self.inventory_service)
        self.payment_service = PaymentService(self.inventory_service)
        self.logistics_service = LogisticsService()
    
    async def close(self):
        """关闭资源"""
        await self.inventory_service.close()
    
    # ==================== 创建订单 ====================
    
    async def create_order(
        self,
        user_id: str,
        items: List[Dict[str, Any]],
        shipping_address: Dict[str, str],
        order_type: OrderType = OrderType.NORMAL,
        presale_config: Optional[Dict[str, Any]] = None,
        idempotency_key: Optional[str] = None,
        **kwargs
    ) -> Order:
        """创建订单（统一入口）"""
        async with get_db_session() as session:
            if order_type == OrderType.PRESALE:
                if not presale_config:
                    raise PresaleException("预售订单必须提供presale_config")
                return await self.creation_service.create_presale_order(
                    session, user_id, items, shipping_address, 
                    presale_config, idempotency_key, **kwargs
                )
            else:
                return await self.creation_service.create_normal_order(
                    session, user_id, items, shipping_address,
                    idempotency_key, **kwargs
                )
    
    # ==================== 支付订单 ====================
    
    async def pay_order(
        self,
        order_id: str,
        payment_amount: Decimal,
        payment_method: str,
        payment_no: str,
        payment_type: str = "full"  # full, deposit, final
    ) -> Order:
        """支付订单（统一入口）"""
        async with get_db_session() as session:
            # 获取订单类型
            result = await session.execute(
                select(Order).where(Order.id == order_id)
            )
            order = result.scalar_one_or_none()
            
            if not order:
                raise OrderNotFoundException(order_id)
            
            if order.order_type == OrderType.PRESALE:
                if payment_type == "deposit":
                    return await self.payment_service.pay_presale_deposit(
                        session, order_id, payment_amount, payment_method, payment_no
                    )
                elif payment_type == "final":
                    return await self.payment_service.pay_presale_final(
                        session, order_id, payment_amount, payment_method, payment_no
                    )
                else:
                    raise PresaleException(f"预售订单不支持支付类型: {payment_type}")
            else:
                return await self.payment_service.pay_normal_order(
                    session, order_id, payment_amount, payment_method, payment_no
                )
    
    # ==================== 取消订单 ====================
    
    async def cancel_order(
        self,
        order_id: str,
        reason: str,
        operator_id: Optional[str] = None
    ) -> Order:
        """取消订单"""
        trace_id = LogContext().context.get("trace_id", "")
        
        async with get_db_session() as session:
            result = await session.execute(
                select(Order).where(Order.id == order_id)
            )
            order = result.scalar_one_or_none()
            
            if not order:
                raise OrderNotFoundException(order_id, trace_id)
            
            if not order.can_cancel():
                raise OrderStatusException(
                    order_id, order.status.value, "CANCELLABLE", trace_id
                )
            
            # 释放库存锁定
            lock_result = await session.execute(
                select(InventoryLock).where(
                    and_(
                        InventoryLock.order_id == order_id,
                        InventoryLock.status == InventoryLockStatus.LOCKED
                    )
                )
            )
            locks = lock_result.scalars().all()
            
            for lock in locks:
                await self.inventory_service.unlock_stock(session, lock.id, operator_id)
            
            # 更新订单状态
            order.status = OrderStatus.CANCELLED
            order.cancelled_at = datetime.utcnow()
            order.cancel_reason = reason
            order.version += 1
            
            await session.flush()
            
            logger.info(f"Order cancelled: {order.order_no}, reason={reason}")
            return order
    
    # ==================== 物流相关 ====================
    
    async def dispatch_order(
        self,
        order_id: str,
        warehouse_id: Optional[str] = None
    ) -> Order:
        """订单发货调度"""
        async with get_db_session() as session:
            return await self.logistics_service.dispatch_order(session, order_id, warehouse_id)
    
    async def ship_order(
        self,
        order_id: str,
        shipping_company: str = "SF"
    ) -> Order:
        """订单发货"""
        async with get_db_session() as session:
            return await self.logistics_service.ship_order(session, order_id, shipping_company)
    
    async def deliver_order(self, order_id: str) -> Order:
        """订单送达"""
        async with get_db_session() as session:
            return await self.logistics_service.deliver_order(session, order_id)
    
    async def complete_order(self, order_id: str) -> Order:
        """订单完成"""
        async with get_db_session() as session:
            return await self.logistics_service.complete_order(session, order_id)
    
    # ==================== 查询 ====================
    
    async def get_order(self, order_id: str) -> Optional[Order]:
        """获取订单详情"""
        async with get_db_session() as session:
            result = await session.execute(
                select(Order).where(Order.id == order_id)
            )
            return result.scalar_one_or_none()
    
    async def get_order_by_no(self, order_no: str) -> Optional[Order]:
        """根据订单号获取订单"""
        async with get_db_session() as session:
            result = await session.execute(
                select(Order).where(Order.order_no == order_no)
            )
            return result.scalar_one_or_none()
    
    async def list_user_orders(
        self,
        user_id: str,
        status: Optional[OrderStatus] = None,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[Order], int]:
        """查询用户订单列表"""
        async with get_db_session() as session:
            query = select(Order).where(
                and_(
                    Order.user_id == user_id,
                    Order.is_deleted == False
                )
            )
            
            if status:
                query = query.where(Order.status == status)
            
            # 总数
            count_result = await session.execute(
                select(Order).where(Order.user_id == user_id)
            )
            total = len(count_result.scalars().all())
            
            # 分页
            query = query.order_by(Order.created_at.desc())
            query = query.offset((page - 1) * page_size).limit(page_size)
            
            result = await session.execute(query)
            orders = result.scalars().all()
            
            return list(orders), total
