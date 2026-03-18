# -*- coding: utf-8 -*-
"""
订单模型模块
重构后的Order模型，支持预售订单、状态机、兼容老数据
"""
import enum
from datetime import datetime
from decimal import Decimal
from typing import Optional, List, TYPE_CHECKING

from sqlalchemy import (
    Column, String, Integer, DateTime, Numeric, ForeignKey, 
    Enum, Text, Boolean, Index, UniqueConstraint
)
from sqlalchemy.orm import relationship, validates
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid

from models.base import Base
from utils.loggers import get_logger

logger = get_logger(__name__)

if TYPE_CHECKING:
    from models.inventory import InventoryLock


class OrderStatus(str, enum.Enum):
    """订单状态枚举"""
    # 普通订单状态
    PENDING_PAYMENT = "pending_payment"      # 待支付
    PAID = "paid"                            # 已支付
    PROCESSING = "processing"                # 处理中
    SHIPPED = "shipped"                      # 已发货
    DELIVERED = "delivered"                  # 已送达
    COMPLETED = "completed"                  # 已完成
    CANCELLED = "cancelled"                  # 已取消
    REFUNDED = "refunded"                    # 已退款
    
    # 预售订单专用状态
    PRESALE_DEPOSIT_PENDING = "presale_deposit_pending"    # 待付定金
    PRESALE_DEPOSIT_PAID = "presale_deposit_paid"          # 已付定金
    PRESALE_FINAL_PAYMENT_PENDING = "presale_final_payment_pending"  # 待付尾款
    PRESALE_FINAL_PAYMENT_PAID = "presale_final_payment_paid"        # 已付尾款


class OrderType(str, enum.Enum):
    """订单类型枚举"""
    NORMAL = "normal"           # 普通订单
    PRESALE = "presale"         # 预售订单
    FLASH_SALE = "flash_sale"   # 秒杀订单
    GROUP_BUY = "group_buy"     # 团购订单


class PresaleStatus(str, enum.Enum):
    """预售订单子状态"""
    NOT_STARTED = "not_started"     # 未开始
    DEPOSIT_PHASE = "deposit_phase" # 定金阶段
    FINAL_PAYMENT_PHASE = "final_payment_phase"  # 尾款阶段
    COMPLETED = "completed"         # 已完成
    CANCELLED = "cancelled"         # 已取消


class Order(Base):
    """
    订单模型（重构版）
    
    兼容老数据的设计：
    1. 所有新字段都有默认值，nullable=True
    2. 使用JSONB存储扩展字段，避免频繁DDL
    3. 保留老字段，新增字段通过property兼容
    """
    __tablename__ = "orders"
    
    # 主键
    id = Column(String(32), primary_key=True, default=lambda: str(uuid.uuid4().hex))
    
    # 订单编号（对外展示）
    order_no = Column(String(64), unique=True, nullable=False, index=True)
    
    # 用户ID
    user_id = Column(String(32), nullable=False, index=True)
    
    # 订单类型（新增，兼容老数据默认为normal）
    order_type = Column(Enum(OrderType), nullable=False, default=OrderType.NORMAL)
    
    # 订单状态
    status = Column(Enum(OrderStatus), nullable=False, default=OrderStatus.PENDING_PAYMENT)
    
    # 金额字段
    total_amount = Column(Numeric(15, 2), nullable=False, default=0)
    discount_amount = Column(Numeric(15, 2), nullable=False, default=0)
    shipping_amount = Column(Numeric(15, 2), nullable=False, default=0)
    pay_amount = Column(Numeric(15, 2), nullable=False, default=0)
    
    # 预售订单专用字段（新增，nullable兼容老数据）
    deposit_amount = Column(Numeric(15, 2), nullable=True)      # 定金金额
    final_payment_amount = Column(Numeric(15, 2), nullable=True)  # 尾款金额
    presale_status = Column(Enum(PresaleStatus), nullable=True)   # 预售子状态
    presale_start_time = Column(DateTime, nullable=True)          # 预售开始时间
    presale_end_time = Column(DateTime, nullable=True)            # 预售结束时间
    final_payment_start_time = Column(DateTime, nullable=True)    # 尾款支付开始时间
    final_payment_end_time = Column(DateTime, nullable=True)      # 尾款支付结束时间
    deposit_paid_at = Column(DateTime, nullable=True)             # 定金支付时间
    final_payment_paid_at = Column(DateTime, nullable=True)       # 尾款支付时间
    
    # 支付相关
    paid_at = Column(DateTime, nullable=True)
    payment_method = Column(String(32), nullable=True)
    payment_no = Column(String(128), nullable=True)
    
    # 物流相关
    shipping_no = Column(String(128), nullable=True)
    shipped_at = Column(DateTime, nullable=True)
    delivered_at = Column(DateTime, nullable=True)
    
    # 取消/退款相关
    cancelled_at = Column(DateTime, nullable=True)
    cancel_reason = Column(Text, nullable=True)
    refunded_at = Column(DateTime, nullable=True)
    refund_amount = Column(Numeric(15, 2), nullable=True)
    
    # 幂等性控制
    idempotency_key = Column(String(64), unique=True, nullable=True, index=True)
    
    # 扩展字段（JSONB，避免频繁DDL）
    extra_data = Column(JSONB, nullable=True, default=dict)
    
    # 时间戳
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    expire_at = Column(DateTime, nullable=True)  # 订单过期时间
    
    # 软删除
    is_deleted = Column(Boolean, nullable=False, default=False)
    deleted_at = Column(DateTime, nullable=True)
    
    # 版本号（乐观锁）
    version = Column(Integer, nullable=False, default=0)
    
    # 关联关系
    items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")
    inventory_locks = relationship("InventoryLock", back_populates="order", cascade="all, delete-orphan")
    
    # 索引
    __table_args__ = (
        Index('idx_order_user_created', 'user_id', 'created_at'),
        Index('idx_order_status_type', 'status', 'order_type'),
        Index('idx_order_presale_time', 'presale_start_time', 'presale_end_time'),
        Index('idx_order_expire', 'expire_at'),
    )
    
    def __repr__(self) -> str:
        return f"<Order(id={self.id}, no={self.order_no}, type={self.order_type}, status={self.status})>"
    
    # ==================== 状态机方法 ====================
    
    def can_pay_deposit(self) -> bool:
        """是否可以支付定金"""
        if self.order_type != OrderType.PRESALE:
            return False
        return self.status == OrderStatus.PRESALE_DEPOSIT_PENDING
    
    def can_pay_final(self) -> bool:
        """是否可以支付尾款"""
        if self.order_type != OrderType.PRESALE:
            return False
        return self.status == OrderStatus.PRESALE_FINAL_PAYMENT_PENDING
    
    def can_cancel(self) -> bool:
        """是否可以取消"""
        cancellable_statuses = [
            OrderStatus.PENDING_PAYMENT,
            OrderStatus.PRESALE_DEPOSIT_PENDING,
            OrderStatus.PRESALE_DEPOSIT_PAID,
            OrderStatus.PRESALE_FINAL_PAYMENT_PENDING,
        ]
        return self.status in cancellable_statuses and not self.is_deleted
    
    def can_refund(self) -> bool:
        """是否可以退款"""
        refundable_statuses = [
            OrderStatus.PAID,
            OrderStatus.PRESALE_DEPOSIT_PAID,
            OrderStatus.PRESALE_FINAL_PAYMENT_PAID,
            OrderStatus.PROCESSING,
        ]
        return self.status in refundable_statuses and not self.is_deleted
    
    def is_paid(self) -> bool:
        """是否已支付完成"""
        if self.order_type == OrderType.PRESALE:
            return self.status == OrderStatus.PRESALE_FINAL_PAYMENT_PAID
        return self.status in [OrderStatus.PAID, OrderStatus.PROCESSING, OrderStatus.SHIPPED, 
                               OrderStatus.DELIVERED, OrderStatus.COMPLETED]
    
    def transition_to(self, new_status: OrderStatus) -> bool:
        """
        状态机转换
        返回是否允许转换
        """
        # 定义允许的状态转换
        transitions = {
            # 普通订单
            OrderStatus.PENDING_PAYMENT: [OrderStatus.PAID, OrderStatus.CANCELLED],
            OrderStatus.PAID: [OrderStatus.PROCESSING, OrderStatus.REFUNDED],
            OrderStatus.PROCESSING: [OrderStatus.SHIPPED, OrderStatus.REFUNDED],
            OrderStatus.SHIPPED: [OrderStatus.DELIVERED],
            OrderStatus.DELIVERED: [OrderStatus.COMPLETED],
            
            # 预售订单
            OrderStatus.PRESALE_DEPOSIT_PENDING: [
                OrderStatus.PRESALE_DEPOSIT_PAID, 
                OrderStatus.CANCELLED
            ],
            OrderStatus.PRESALE_DEPOSIT_PAID: [
                OrderStatus.PRESALE_FINAL_PAYMENT_PENDING,
                OrderStatus.CANCELLED
            ],
            OrderStatus.PRESALE_FINAL_PAYMENT_PENDING: [
                OrderStatus.PRESALE_FINAL_PAYMENT_PAID,
                OrderStatus.CANCELLED
            ],
            OrderStatus.PRESALE_FINAL_PAYMENT_PAID: [
                OrderStatus.PROCESSING,
                OrderStatus.REFUNDED
            ],
        }
        
        allowed = transitions.get(self.status, [])
        if new_status in allowed:
            self.status = new_status
            self.version += 1
            return True
        return False
    
    # ==================== 兼容老数据属性 ====================
    
    @property
    def is_presale(self) -> bool:
        """是否为预售订单（兼容属性）"""
        return self.order_type == OrderType.PRESALE
    
    @property
    def effective_amount(self) -> Decimal:
        """实际应付金额"""
        if self.order_type == OrderType.PRESALE:
            if self.status == OrderStatus.PRESALE_DEPOSIT_PENDING:
                return self.deposit_amount or Decimal('0')
            elif self.status == OrderStatus.PRESALE_FINAL_PAYMENT_PENDING:
                return self.final_payment_amount or Decimal('0')
        return self.pay_amount
    
    @property
    def paid_total(self) -> Decimal:
        """已支付总额"""
        total = Decimal('0')
        if self.deposit_paid_at and self.deposit_amount:
            total += self.deposit_amount
        if self.final_payment_paid_at and self.final_payment_amount:
            total += self.final_payment_amount
        if self.paid_at and self.pay_amount:
            total += self.pay_amount
        return total


class OrderItem(Base):
    """
    订单商品项模型
    """
    __tablename__ = "order_items"
    
    id = Column(String(32), primary_key=True, default=lambda: str(uuid.uuid4().hex))
    order_id = Column(String(32), ForeignKey("orders.id"), nullable=False, index=True)
    
    # 商品信息
    sku_id = Column(String(32), nullable=False, index=True)
    spu_id = Column(String(32), nullable=False)
    sku_name = Column(String(255), nullable=False)
    sku_image = Column(String(512), nullable=True)
    
    # 价格信息
    original_price = Column(Numeric(15, 2), nullable=False)
    sale_price = Column(Numeric(15, 2), nullable=False)
    
    # 数量
    quantity = Column(Integer, nullable=False, default=1)
    
    # 小计
    subtotal = Column(Numeric(15, 2), nullable=False)
    
    # 预售相关（冗余存储，方便查询）
    is_presale_item = Column(Boolean, nullable=False, default=False)
    presale_deposit = Column(Numeric(15, 2), nullable=True)
    presale_final_payment = Column(Numeric(15, 2), nullable=True)
    
    # 扩展字段
    extra_data = Column(JSONB, nullable=True, default=dict)
    
    # 时间戳
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关联
    order = relationship("Order", back_populates="items")
    
    # 索引
    __table_args__ = (
        Index('idx_order_item_sku', 'sku_id', 'created_at'),
    )
    
    def __repr__(self) -> str:
        return f"<OrderItem(id={self.id}, sku={self.sku_id}, qty={self.quantity})>"
    
    @validates('quantity')
    def validate_quantity(self, key: str, quantity: int) -> int:
        """验证数量"""
        if quantity <= 0:
            raise ValueError("Quantity must be greater than 0")
        return quantity
