# -*- coding: utf-8 -*-
"""
库存服务模块
实现库存预锁定、最终扣减、自动回滚机制
处理Redis与DB数据一致性
"""
import asyncio
import json
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, List, Dict, Any, Tuple
from contextlib import asynccontextmanager

from sqlalchemy import select, update, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as redis

from models.inventory import (
    Inventory, InventoryLock, InventoryLog, InventoryTier,
    InventoryLockStatus, InventoryLogType
)
from models.order import Order, OrderType
from models.base import get_db_session
from utils.exceptions import (
    InventoryInsufficientException,
    InventoryLockException,
    RetryException,
    IdempotencyException
)
from utils.loggers import get_logger, LogContext, log_execution_time
from config.settings import settings

logger = get_logger(__name__)


class RedisInventoryClient:
    """Redis库存客户端"""
    
    def __init__(self):
        self._redis: Optional[redis.Redis] = None
        self._lock = asyncio.Lock()
    
    async def get_client(self) -> redis.Redis:
        """获取Redis客户端"""
        if self._redis is None:
            async with self._lock:
                if self._redis is None:
                    self._redis = redis.from_url(
                        settings.REDIS_URL,
                        max_connections=settings.REDIS_POOL_SIZE,
                        socket_timeout=settings.REDIS_SOCKET_TIMEOUT,
                        socket_connect_timeout=settings.REDIS_SOCKET_CONNECT_TIMEOUT,
                        decode_responses=True
                    )
        return self._redis
    
    async def close(self):
        """关闭Redis连接"""
        if self._redis:
            await self._redis.close()
            self._redis = None
    
    def _stock_key(self, sku_id: str) -> str:
        """库存key"""
        return f"inv:stock:{sku_id}"
    
    def _lock_key(self, sku_id: str) -> str:
        """锁定key"""
        return f"inv:lock:{sku_id}"
    
    def _presale_key(self, sku_id: str) -> str:
        """预售库存key"""
        return f"inv:presale:{sku_id}"
    
    async def get_stock(self, sku_id: str) -> Dict[str, int]:
        """获取库存信息"""
        client = await self.get_client()
        stock_key = self._stock_key(sku_id)
        presale_key = self._presale_key(sku_id)
        
        stock_data = await client.hgetall(stock_key)
        presale_data = await client.hgetall(presale_key)
        
        return {
            "available": int(stock_data.get("available", 0)),
            "locked": int(stock_data.get("locked", 0)),
            "sold": int(stock_data.get("sold", 0)),
            "presale_total": int(presale_data.get("total", 0)),
            "presale_locked": int(presale_data.get("locked", 0)),
            "presale_sold": int(presale_data.get("sold", 0)),
        }
    
    async def init_stock(self, sku_id: str, available: int, presale_total: int = 0):
        """初始化库存（从DB加载到Redis）"""
        client = await self.get_client()
        stock_key = self._stock_key(sku_id)
        presale_key = self._presale_key(sku_id)
        
        pipe = client.pipeline()
        pipe.hset(stock_key, mapping={
            "available": available,
            "locked": 0,
            "sold": 0
        })
        pipe.hset(presale_key, mapping={
            "total": presale_total,
            "locked": 0,
            "sold": 0
        })
        await pipe.execute()
    
    async def lock_stock(self, sku_id: str, quantity: int, is_presale: bool = False) -> bool:
        """
        使用Redis Lua脚本原子锁定库存
        返回是否成功
        """
        client = await self.get_client()
        
        if is_presale:
            key = self._presale_key(sku_id)
            field_available = "total"
            field_locked = "locked"
        else:
            key = self._stock_key(sku_id)
            field_available = "available"
            field_locked = "locked"
        
        # Lua脚本：原子检查并锁定
        lua_script = """
        local available = tonumber(redis.call('hget', KEYS[1], ARGV[1]))
        local locked = tonumber(redis.call('hget', KEYS[1], ARGV[2]))
        local quantity = tonumber(ARGV[3])
        
        if available >= quantity then
            redis.call('hincrby', KEYS[1], ARGV[1], -quantity)
            redis.call('hincrby', KEYS[1], ARGV[2], quantity)
            return 1
        else
            return 0
        end
        """
        
        try:
            result = await client.eval(
                lua_script, 1, key, field_available, field_locked, quantity
            )
            return result == 1
        except Exception as e:
            logger.error(f"Redis lock stock failed: {sku_id}, qty={quantity}, error={e}")
            return False
    
    async def unlock_stock(self, sku_id: str, quantity: int, is_presale: bool = False) -> bool:
        """解锁库存"""
        client = await self.get_client()
        
        if is_presale:
            key = self._presale_key(sku_id)
            field_available = "total"
            field_locked = "locked"
        else:
            key = self._stock_key(sku_id)
            field_available = "available"
            field_locked = "locked"
        
        lua_script = """
        local locked = tonumber(redis.call('hget', KEYS[1], ARGV[2]))
        local quantity = tonumber(ARGV[3])
        
        if locked >= quantity then
            redis.call('hincrby', KEYS[1], ARGV[1], quantity)
            redis.call('hincrby', KEYS[1], ARGV[2], -quantity)
            return 1
        else
            return 0
        end
        """
        
        try:
            result = await client.eval(
                lua_script, 1, key, field_available, field_locked, quantity
            )
            return result == 1
        except Exception as e:
            logger.error(f"Redis unlock stock failed: {sku_id}, qty={quantity}, error={e}")
            return False
    
    async def deduct_stock(self, sku_id: str, quantity: int, is_presale: bool = False) -> bool:
        """扣减库存（从锁定转为已售）"""
        client = await self.get_client()
        
        if is_presale:
            key = self._presale_key(sku_id)
            field_locked = "locked"
            field_sold = "sold"
        else:
            key = self._stock_key(sku_id)
            field_locked = "locked"
            field_sold = "sold"
        
        lua_script = """
        local locked = tonumber(redis.call('hget', KEYS[1], ARGV[1]))
        local quantity = tonumber(ARGV[3])
        
        if locked >= quantity then
            redis.call('hincrby', KEYS[1], ARGV[1], -quantity)
            redis.call('hincrby', KEYS[1], ARGV[2], quantity)
            return 1
        else
            return 0
        end
        """
        
        try:
            result = await client.eval(
                lua_script, 1, key, field_locked, field_sold, quantity
            )
            return result == 1
        except Exception as e:
            logger.error(f"Redis deduct stock failed: {sku_id}, qty={quantity}, error={e}")
            return False


class InventoryService:
    """库存服务"""
    
    def __init__(self):
        self.redis_client = RedisInventoryClient()
    
    async def close(self):
        """关闭资源"""
        await self.redis_client.close()
    
    async def _get_db_inventory(self, session: AsyncSession, sku_id: str) -> Optional[Inventory]:
        """获取库存记录"""
        result = await session.execute(
            select(Inventory).where(Inventory.sku_id == sku_id)
        )
        return result.scalar_one_or_none()
    
    async def _sync_to_redis(self, inventory: Inventory):
        """同步库存到Redis"""
        await self.redis_client.init_stock(
            inventory.sku_id,
            inventory.available_stock,
            inventory.presale_stock
        )
    
    async def _sync_from_redis(self, inventory: Inventory) -> Dict[str, int]:
        """从Redis同步库存状态"""
        return await self.redis_client.get_stock(inventory.sku_id)
    
    async def _create_inventory_log(
        self,
        session: AsyncSession,
        inventory_id: str,
        log_type: InventoryLogType,
        quantity: int,
        before: Dict[str, int],
        after: Dict[str, int],
        order_id: Optional[str] = None,
        operator_id: Optional[str] = None,
        reason: Optional[str] = None
    ) -> InventoryLog:
        """创建库存日志"""
        log = InventoryLog(
            inventory_id=inventory_id,
            log_type=log_type,
            quantity=quantity,
            before_available=before.get("available", 0),
            before_locked=before.get("locked", 0),
            before_sold=before.get("sold", 0),
            after_available=after.get("available", 0),
            after_locked=after.get("locked", 0),
            after_sold=after.get("sold", 0),
            order_id=order_id,
            operator_id=operator_id,
            reason=reason
        )
        session.add(log)
        await session.flush()
        return log
    
    @log_execution_time()
    async def lock_stock(
        self,
        session: AsyncSession,
        order_id: str,
        sku_id: str,
        quantity: int,
        is_presale: bool = False,
        idempotency_key: Optional[str] = None,
        expire_minutes: int = 30
    ) -> InventoryLock:
        """
        锁定库存
        
        流程：
        1. 幂等性检查
        2. Redis预锁定（快速失败）
        3. 数据库乐观锁锁定
        4. 写入锁定记录
        5. 写操作日志
        """
        trace_id = LogContext().context.get("trace_id", "")
        
        # 幂等性检查
        if idempotency_key:
            existing = await session.execute(
                select(InventoryLock).where(
                    InventoryLock.idempotency_key == idempotency_key
                )
            )
            if existing.scalar_one_or_none():
                raise IdempotencyException(idempotency_key, order_id, trace_id)
        
        # 获取库存记录
        inventory = await self._get_db_inventory(session, sku_id)
        if not inventory:
            raise InventoryLockException(
                sku_id, quantity, "库存记录不存在", trace_id
            )
        
        # 同步到Redis（如果Redis没有）
        redis_stock = await self._sync_from_redis(inventory)
        if redis_stock["available"] == 0 and redis_stock["presale_total"] == 0:
            await self._sync_to_redis(inventory)
        
        # Redis预锁定
        redis_success = await self.redis_client.lock_stock(sku_id, quantity, is_presale)
        if not redis_success:
            # 获取最新库存信息用于错误提示
            stock = await self._sync_from_redis(inventory)
            available = stock["presale_total"] - stock["presale_locked"] - stock["presale_sold"] if is_presale else stock["available"]
            raise InventoryInsufficientException(
                sku_id, quantity, available, trace_id
            )
        
        try:
            # 数据库乐观锁锁定
            if is_presale:
                if inventory.presale_available < quantity:
                    raise InventoryInsufficientException(
                        sku_id, quantity, inventory.presale_available, trace_id
                    )
                inventory.presale_locked += quantity
            else:
                if inventory.real_can_sale < quantity:
                    raise InventoryInsufficientException(
                        sku_id, quantity, inventory.real_can_sale, trace_id
                    )
                inventory.available_stock -= quantity
                inventory.locked_stock += quantity
            
            inventory.version += 1
            await session.flush()
            
            # 创建锁定记录
            lock_record = InventoryLock(
                order_id=order_id,
                inventory_id=inventory.id,
                sku_id=sku_id,
                quantity=quantity,
                is_presale=is_presale,
                status=InventoryLockStatus.LOCKED,
                expire_at=datetime.utcnow() + timedelta(minutes=expire_minutes),
                idempotency_key=idempotency_key or f"{order_id}:{sku_id}:{datetime.utcnow().timestamp()}"
            )
            session.add(lock_record)
            await session.flush()
            
            # 记录日志
            await self._create_inventory_log(
                session, inventory.id,
                InventoryLogType.PRESALE_LOCK if is_presale else InventoryLogType.LOCK,
                quantity,
                {"available": inventory.available_stock + (0 if is_presale else quantity),
                 "locked": inventory.locked_stock - (0 if is_presale else quantity),
                 "sold": inventory.sold_stock},
                {"available": inventory.available_stock,
                 "locked": inventory.locked_stock,
                 "sold": inventory.sold_stock},
                order_id=order_id,
                reason=f"订单锁定库存: {order_id}"
            )
            
            logger.info(
                f"Stock locked: order={order_id}, sku={sku_id}, qty={quantity}, presale={is_presale}"
            )
            return lock_record
            
        except Exception as e:
            # 回滚Redis锁定
            await self.redis_client.unlock_stock(sku_id, quantity, is_presale)
            raise
    
    @log_execution_time()
    async def unlock_stock(
        self,
        session: AsyncSession,
        lock_id: str,
        operator_id: Optional[str] = None
    ) -> bool:
        """
        解锁库存（取消订单时调用）
        """
        trace_id = LogContext().context.get("trace_id", "")
        
        # 获取锁定记录
        result = await session.execute(
            select(InventoryLock).where(InventoryLock.id == lock_id)
        )
        lock_record = result.scalar_one_or_none()
        
        if not lock_record:
            logger.warning(f"Lock record not found: {lock_id}")
            return False
        
        if lock_record.status != InventoryLockStatus.LOCKED:
            logger.warning(f"Lock status invalid: {lock_id}, status={lock_record.status}")
            return False
        
        # 获取库存
        inventory = await self._get_db_inventory(session, lock_record.sku_id)
        if not inventory:
            raise InventoryLockException(
                lock_record.sku_id, lock_record.quantity, "库存记录不存在", trace_id
            )
        
        # Redis解锁
        redis_success = await self.redis_client.unlock_stock(
            lock_record.sku_id, lock_record.quantity, lock_record.is_presale
        )
        if not redis_success:
            logger.error(f"Redis unlock failed: {lock_record.sku_id}")
        
        # 数据库解锁
        before = {
            "available": inventory.available_stock,
            "locked": inventory.locked_stock,
            "sold": inventory.sold_stock
        }
        
        if lock_record.is_presale:
            inventory.presale_locked -= lock_record.quantity
        else:
            inventory.locked_stock -= lock_record.quantity
            inventory.available_stock += lock_record.quantity
        
        inventory.version += 1
        
        # 更新锁定记录
        lock_record.mark_released()
        
        # 记录日志
        await self._create_inventory_log(
            session, inventory.id,
            InventoryLogType.PRESALE_UNLOCK if lock_record.is_presale else InventoryLogType.UNLOCK,
            lock_record.quantity,
            before,
            {"available": inventory.available_stock,
             "locked": inventory.locked_stock,
             "sold": inventory.sold_stock},
            order_id=lock_record.order_id,
            operator_id=operator_id,
            reason=f"订单解锁库存: {lock_record.order_id}"
        )
        
        logger.info(
            f"Stock unlocked: lock={lock_id}, order={lock_record.order_id}, sku={lock_record.sku_id}"
        )
        return True
    
    @log_execution_time()
    async def deduct_stock(
        self,
        session: AsyncSession,
        order_id: str,
        sku_id: str,
        quantity: int,
        is_presale: bool = False
    ) -> bool:
        """
        扣减库存（支付成功后调用）
        将锁定库存转为已售库存
        """
        trace_id = LogContext().context.get("trace_id", "")
        
        # 获取库存
        inventory = await self._get_db_inventory(session, sku_id)
        if not inventory:
            raise InventoryLockException(
                sku_id, quantity, "库存记录不存在", trace_id
            )
        
        # Redis扣减
        redis_success = await self.redis_client.deduct_stock(sku_id, quantity, is_presale)
        if not redis_success:
            logger.warning(f"Redis deduct failed, will retry from DB: {sku_id}")
        
        # 数据库扣减
        before = {
            "available": inventory.available_stock,
            "locked": inventory.locked_stock,
            "sold": inventory.sold_stock
        }
        
        if is_presale:
            if inventory.presale_locked < quantity:
                raise InventoryLockException(
                    sku_id, quantity, f"预售锁定库存不足: {inventory.presale_locked}", trace_id
                )
            inventory.presale_locked -= quantity
            inventory.presale_sold += quantity
        else:
            if inventory.locked_stock < quantity:
                raise InventoryLockException(
                    sku_id, quantity, f"锁定库存不足: {inventory.locked_stock}", trace_id
                )
            inventory.locked_stock -= quantity
            inventory.sold_stock += quantity
        
        inventory.version += 1
        
        # 更新锁定记录
        result = await session.execute(
            select(InventoryLock).where(
                and_(
                    InventoryLock.order_id == order_id,
                    InventoryLock.sku_id == sku_id,
                    InventoryLock.status == InventoryLockStatus.LOCKED
                )
            )
        )
        lock_record = result.scalar_one_or_none()
        if lock_record:
            lock_record.mark_deducted()
        
        # 记录日志
        await self._create_inventory_log(
            session, inventory.id,
            InventoryLogType.DEDUCT,
            quantity,
            before,
            {"available": inventory.available_stock,
             "locked": inventory.locked_stock,
             "sold": inventory.sold_stock},
            order_id=order_id,
            reason=f"支付成功扣减库存: {order_id}"
        )
        
        logger.info(
            f"Stock deducted: order={order_id}, sku={sku_id}, qty={quantity}, presale={is_presale}"
        )
        return True
    
    async def batch_deduct_for_order(
        self,
        session: AsyncSession,
        order_id: str
    ) -> Tuple[bool, List[str]]:
        """
        为订单批量扣减库存
        返回 (是否全部成功, 失败的SKU列表)
        """
        # 获取订单的所有锁定记录
        result = await session.execute(
            select(InventoryLock).where(
                and_(
                    InventoryLock.order_id == order_id,
                    InventoryLock.status == InventoryLockStatus.LOCKED
                )
            )
        )
        locks = result.scalars().all()
        
        failed_skus = []
        for lock in locks:
            try:
                await self.deduct_stock(
                    session, order_id, lock.sku_id, lock.quantity, lock.is_presale
                )
            except Exception as e:
                logger.error(f"Deduct stock failed for {lock.sku_id}: {e}")
                failed_skus.append(lock.sku_id)
        
        return len(failed_skus) == 0, failed_skus
    
    async def release_expired_locks(self, session: AsyncSession) -> int:
        """
        释放过期锁定
        返回释放的数量
        """
        result = await session.execute(
            select(InventoryLock).where(
                and_(
                    InventoryLock.status == InventoryLockStatus.LOCKED,
                    InventoryLock.expire_at < datetime.utcnow()
                )
            )
        )
        expired_locks = result.scalars().all()
        
        released_count = 0
        for lock in expired_locks:
            try:
                await self.unlock_stock(session, lock.id, operator_id="system")
                lock.mark_expired()
                released_count += 1
            except Exception as e:
                logger.error(f"Release expired lock failed: {lock.id}, error={e}")
        
        logger.info(f"Released {released_count} expired locks")
        return released_count
    
    async def get_inventory_status(
        self,
        session: AsyncSession,
        sku_id: str
    ) -> Dict[str, Any]:
        """获取库存状态"""
        inventory = await self._get_db_inventory(session, sku_id)
        if not inventory:
            return None
        
        redis_stock = await self._sync_from_redis(inventory)
        
        return {
            "sku_id": sku_id,
            "available_stock": inventory.available_stock,
            "locked_stock": inventory.locked_stock,
            "sold_stock": inventory.sold_stock,
            "presale_stock": inventory.presale_stock,
            "presale_locked": inventory.presale_locked,
            "presale_sold": inventory.presale_sold,
            "safety_stock": inventory.safety_stock,
            "redis_stock": redis_stock,
            "version": inventory.version
        }
