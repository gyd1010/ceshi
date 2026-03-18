# -*- coding: utf-8 -*-
"""
库存模型模块
支持阶梯库存、预售锁定库存、并发控制
"""
import enum
from datetime import datetime
from typing import Optional, List, TYPE_CHECKING

from sqlalchemy import (
    Column, String, Integer, DateTime, Numeric, ForeignKey, 
    Enum, Text, Boolean, Index, UniqueConstraint, CheckConstraint
)
from sqlalchemy.orm import relationship, validates
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid

from models.base import Base
from utils.loggers import get_logger

logger = get_logger(__name__)

if TYPE_CHECKING:
    from models.order import Order


class InventoryLogType(str, enum.Enum):
    """库存日志类型"""
    LOCK = "lock"               # 锁定
    UNLOCK = "unlock"           # 解锁
    DEDUCT = "deduct"           # 扣减
    ROLLBACK = "rollback"       # 回滚
    ADJUST = "adjust"           # 调整
    PRESALE_LOCK = "presale_lock"    # 预售锁定
    PRESALE_UNLOCK = "presale_unlock"  # 预售解锁


class InventoryLockStatus(str, enum.Enum):
    """库存锁定状态"""
    LOCKED = "locked"           # 已锁定
    DEDUCTED = "deducted"       # 已扣减
    RELEASED = "released"       # 已释放
    EXPIRED = "expired"         # 已过期


class Inventory(Base):
    """
    库存主表
    支持阶梯库存、预售库存分离
    """
    __tablename__ = "inventory"
    
    id = Column(String(32), primary_key=True, default=lambda: str(uuid.uuid4().hex))
    sku_id = Column(String(32), unique=True, nullable=False, index=True)
    spu_id = Column(String(32), nullable=False, index=True)
    
    # 库存类型
    is_presale = Column(Boolean, nullable=False, default=False)
    
    # 实际库存（已上架可售）
    available_stock = Column(Integer, nullable=False, default=0)
    
    # 锁定库存（已下单未支付）
    locked_stock = Column(Integer, nullable=False, default=0)
    
    # 预售专用库存
    presale_stock = Column(Integer, nullable=False, default=0)        # 预售总库存
    presale_locked = Column(Integer, nullable=False, default=0)       # 预售已锁定
    presale_sold = Column(Integer, nullable=False, default=0)         # 预售已售出
    
    # 已售/在途
    sold_stock = Column(Integer, nullable=False, default=0)
    in_transit_stock = Column(Integer, nullable=False, default=0)
    
    # 安全库存
    safety_stock = Column(Integer, nullable=False, default=0)
    
    # 版本号（乐观锁，解决并发超卖）
    version = Column(Integer, nullable=False, default=0)
    
    # 扩展字段
    extra_data = Column(JSONB, nullable=True, default=dict)
    
    # 时间戳
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关联
    tiers = relationship("InventoryTier", back_populates="inventory", cascade="all, delete-orphan")
    logs = relationship("InventoryLog", back_populates="inventory")
    locks = relationship("InventoryLock", back_populates="inventory")
    
    # 约束
    __table_args__ = (
        CheckConstraint('available_stock >= 0', name='chk_available_non_negative'),
        CheckConstraint('locked_stock >= 0', name='chk_locked_non_negative'),
        CheckConstraint('presale_stock >= 0', name='chk_presale_non_negative'),
        CheckConstraint('presale_locked >= 0', name='chk_presale_locked_non_negative'),
        CheckConstraint('presale_sold >= 0', name='chk_presale_sold_non_negative'),
        CheckConstraint('available_stock + locked_stock + sold_stock <= 10000000', name='chk_stock_reasonable'),
    )
    
    def __repr__(self) -> str:
        return f"<Inventory(sku={self.sku_id}, available={self.available_stock}, locked={self.locked_stock})>"
    
    @property
    def total_stock(self) -> int:
        """总库存"""
        return self.available_stock + self.locked_stock + self.sold_stock
    
    @property
    def real_can_sale(self) -> int:
        """真实可售库存（考虑安全库存）"""
        return max(0, self.available_stock - self.safety_stock)
    
    @property
    def presale_available(self) -> int:
        """预售可用库存"""
        return max(0, self.presale_stock - self.presale_locked - self.presale_sold)
    
    def can_lock(self, quantity: int, is_presale: bool = False) -> bool:
        """检查是否可以锁定指定数量"""
        if is_presale:
            return self.presale_available >= quantity
        return self.real_can_sale >= quantity
    
    def lock_stock(self, quantity: int, is_presale: bool = False) -> bool:
        """
        锁定库存（内存操作，需配合数据库乐观锁）
        返回是否成功
        """
        if is_presale:
            if self.presale_available < quantity:
                return False
            self.presale_locked += quantity
        else:
            if self.real_can_sale < quantity:
                return False
            self.available_stock -= quantity
            self.locked_stock += quantity
        self.version += 1
        return True
    
    def unlock_stock(self, quantity: int, is_presale: bool = False) -> bool:
        """解锁库存"""
        if is_presale:
            if self.presale_locked < quantity:
                return False
            self.presale_locked -= quantity
        else:
            if self.locked_stock < quantity:
                return False
            self.locked_stock -= quantity
            self.available_stock += quantity
        self.version += 1
        return True
    
    def deduct_stock(self, quantity: int, is_presale: bool = False) -> bool:
        """扣减库存（从锁定转为已售）"""
        if is_presale:
            if self.presale_locked < quantity:
                return False
            self.presale_locked -= quantity
            self.presale_sold += quantity
        else:
            if self.locked_stock < quantity:
                return False
            self.locked_stock -= quantity
            self.sold_stock += quantity
        self.version += 1
        return True
    
    def rollback_stock(self, quantity: int, is_presale: bool = False) -> bool:
        """回滚库存（从锁定退回可用）"""
        return self.unlock_stock(quantity, is_presale)


class InventoryTier(Base):
    """
    阶梯库存表
    支持不同数量区间的不同价格
    """
    __tablename__ = "inventory_tiers"
    
    id = Column(String(32), primary_key=True, default=lambda: str(uuid.uuid4().hex))
    inventory_id = Column(String(32), ForeignKey("inventory.id"), nullable=False, index=True)
    
    # 阶梯区间
    min_quantity = Column(Integer, nullable=False)  # 最小数量（含）
    max_quantity = Column(Integer, nullable=False)  # 最大数量（含），-1表示无上限
    
    # 阶梯价格
    tier_price = Column(Numeric(15, 2), nullable=False)
    
    # 阶梯库存
    tier_stock = Column(Integer, nullable=False, default=0)
    tier_sold = Column(Integer, nullable=False, default=0)
    
    # 是否启用
    is_active = Column(Boolean, nullable=False, default=True)
    
    # 优先级（数字越小优先级越高）
    priority = Column(Integer, nullable=False, default=0)
    
    # 时间戳
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关联
    inventory = relationship("Inventory", back_populates="tiers")
    
    # 约束
    __table_args__ = (
        UniqueConstraint('inventory_id', 'min_quantity', name='uix_tier_range'),
        CheckConstraint('min_quantity > 0', name='chk_min_positive'),
        CheckConstraint('max_quantity >= -1', name='chk_max_valid'),
        CheckConstraint('min_quantity <= max_quantity OR max_quantity = -1', name='chk_range_valid'),
    )
    
    def __repr__(self) -> str:
        return f"<InventoryTier(inv={self.inventory_id}, range={self.min_quantity}-{self.max_quantity}, price={self.tier_price})>"
    
    @property
    def available(self) -> int:
        """可用数量"""
        return max(0, self.tier_stock - self.tier_sold)
    
    def matches_quantity(self, quantity: int) -> bool:
        """检查数量是否在此阶梯"""
        if quantity < self.min_quantity:
            return False
        if self.max_quantity == -1:
            return True
        return quantity <= self.max_quantity


class InventoryLock(Base):
    """
    库存锁定记录表
    用于追踪每个订单的库存锁定状态，支持幂等和超时释放
    """
    __tablename__ = "inventory_locks"
    
    id = Column(String(32), primary_key=True, default=lambda: str(uuid.uuid4().hex))
    
    # 关联
    order_id = Column(String(32), ForeignKey("orders.id"), nullable=False, index=True)
    inventory_id = Column(String(32), ForeignKey("inventory.id"), nullable=False, index=True)
    
    # 锁定信息
    sku_id = Column(String(32), nullable=False, index=True)
    quantity = Column(Integer, nullable=False)
    is_presale = Column(Boolean, nullable=False, default=False)
    
    # 状态
    status = Column(Enum(InventoryLockStatus), nullable=False, default=InventoryLockStatus.LOCKED)
    
    # 超时时间
    expire_at = Column(DateTime, nullable=False)
    
    # 操作时间
    locked_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    deducted_at = Column(DateTime, nullable=True)
    released_at = Column(DateTime, nullable=True)
    
    # 幂等键
    idempotency_key = Column(String(64), unique=True, nullable=False, index=True)
    
    # 扩展字段
    extra_data = Column(JSONB, nullable=True, default=dict)
    
    # 时间戳
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关联
    order = relationship("Order", back_populates="inventory_locks")
    inventory = relationship("Inventory", back_populates="locks")
    
    # 索引
    __table_args__ = (
        Index('idx_lock_status_expire', 'status', 'expire_at'),
        Index('idx_lock_order_sku', 'order_id', 'sku_id'),
    )
    
    def __repr__(self) -> str:
        return f"<InventoryLock(order={self.order_id}, sku={self.sku_id}, qty={self.quantity}, status={self.status})>"
    
    def is_expired(self) -> bool:
        """是否已过期"""
        return datetime.utcnow() > self.expire_at
    
    def mark_deducted(self):
        """标记为已扣减"""
        self.status = InventoryLockStatus.DEDUCTED
        self.deducted_at = datetime.utcnow()
    
    def mark_released(self):
        """标记为已释放"""
        self.status = InventoryLockStatus.RELEASED
        self.released_at = datetime.utcnow()
    
    def mark_expired(self):
        """标记为已过期"""
        self.status = InventoryLockStatus.EXPIRED
        self.released_at = datetime.utcnow()


class InventoryLog(Base):
    """
    库存操作日志表
    用于审计和追踪
    """
    __tablename__ = "inventory_logs"
    
    id = Column(String(32), primary_key=True, default=lambda: str(uuid.uuid4().hex))
    inventory_id = Column(String(32), ForeignKey("inventory.id"), nullable=False, index=True)
    
    # 操作类型
    log_type = Column(Enum(InventoryLogType), nullable=False)
    
    # 操作数量
    quantity = Column(Integer, nullable=False)
    
    # 操作前数量
    before_available = Column(Integer, nullable=False)
    before_locked = Column(Integer, nullable=False)
    before_sold = Column(Integer, nullable=False)
    
    # 操作后数量
    after_available = Column(Integer, nullable=False)
    after_locked = Column(Integer, nullable=False)
    after_sold = Column(Integer, nullable=False)
    
    # 关联订单
    order_id = Column(String(32), nullable=True, index=True)
    
    # 操作人
    operator_id = Column(String(32), nullable=True)
    operator_type = Column(String(32), nullable=True)  # user/system/auto
    
    # 原因/备注
    reason = Column(Text, nullable=True)
    
    # 扩展字段
    extra_data = Column(JSONB, nullable=True, default=dict)
    
    # 时间戳
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # 关联
    inventory = relationship("Inventory", back_populates="logs")
    
    # 索引
    __table_args__ = (
        Index('idx_log_type_time', 'log_type', 'created_at'),
        Index('idx_log_order', 'order_id', 'created_at'),
    )
    
    def __repr__(self) -> str:
        return f"<InventoryLog(inv={self.inventory_id}, type={self.log_type}, qty={self.quantity})>"
