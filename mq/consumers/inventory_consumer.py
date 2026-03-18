# -*- coding: utf-8 -*-
"""
库存消息消费者模块
监听订单支付成功事件，实现重试、死信队列、幂等校验
"""
import json
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any, Callable
from enum import Enum

import aio_pika
from aio_pika import DeliveryMode, ExchangeType
from aio_pika.pool import Pool

from models.base import get_db_session
from models.inventory import InventoryLock, InventoryLockStatus
from models.order import Order, OrderStatus
from services.inventory_service import InventoryService
from services.order_service import OrderService
from utils.exceptions import (
    InventoryLockException,
    RetryException,
    IdempotencyException
)
from utils.loggers import get_logger, LogContext
from config.settings import settings

logger = get_logger(__name__)


class MessageType(str, Enum):
    """消息类型"""
    ORDER_PAYMENT_SUCCESS = "order_payment_success"
    ORDER_CANCELLED = "order_cancelled"
    INVENTORY_RELEASE = "inventory_release"
    PRESALE_FINAL_PAID = "presale_final_paid"


class InventoryConsumer:
    """
    库存消息消费者
    
    功能：
    1. 监听订单支付成功事件，扣减库存
    2. 监听订单取消事件，释放库存
    3. 实现重试机制（最多3次）
    4. 死信队列处理（超过重试次数）
    5. 幂等性校验
    """
    
    def __init__(self):
        self._connection: Optional[aio_pika.RobustConnection] = None
        self._channel_pool: Optional[Pool] = None
        self._inventory_service = InventoryService()
        self._order_service = OrderService()
        self._running = False
        self._consumer_tags = []
    
    async def connect(self):
        """连接RabbitMQ"""
        try:
            self._connection = await aio_pika.connect_robust(
                settings.RABBITMQ_URL,
                reconnect_interval=5
            )
            
            # 创建通道池
            async def get_channel():
                return await self._connection.channel()
            
            self._channel_pool = Pool(get_channel, max_size=10)
            
            logger.info("Connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    async def setup_queues(self):
        """设置队列和交换机"""
        async with self._channel_pool.acquire() as channel:
            # 声明死信交换机
            dlx_exchange = await channel.declare_exchange(
                settings.RABBITMQ_DLX_EXCHANGE,
                ExchangeType.DIRECT,
                durable=True
            )
            
            # 声明死信队列
            dlx_queue = await channel.declare_queue(
                settings.RABBITMQ_DLX_QUEUE,
                durable=True
            )
            await dlx_queue.bind(dlx_exchange, routing_key="failed")
            
            # 声明主交换机
            main_exchange = await channel.declare_exchange(
                settings.RABBITMQ_EXCHANGE,
                ExchangeType.TOPIC,
                durable=True
            )
            
            # 声明队列（带死信配置）
            queue_args = {
                "x-dead-letter-exchange": settings.RABBITMQ_DLX_EXCHANGE,
                "x-dead-letter-routing-key": "failed",
                "x-message-ttl": 300000,  # 5分钟TTL
            }
            
            # 订单支付成功队列
            payment_queue = await channel.declare_queue(
                settings.RABBITMQ_QUEUE_PAYMENT_SUCCESS,
                durable=True,
                arguments=queue_args
            )
            await payment_queue.bind(main_exchange, routing_key="order.payment.success")
            
            # 库存扣减队列
            inventory_queue = await channel.declare_queue(
                settings.RABBITMQ_QUEUE_INVENTORY_DEDUCT,
                durable=True,
                arguments=queue_args
            )
            await inventory_queue.bind(main_exchange, routing_key="inventory.deduct")
            
            logger.info("Queues and exchanges set up successfully")
    
    async def process_message(
        self,
        message: aio_pika.IncomingMessage,
        handler: Callable[[Dict[str, Any]], Any]
    ):
        """处理消息（带重试和幂等）"""
        async with message.process():
            trace_id = message.headers.get("trace_id", str(datetime.utcnow().timestamp()))
            
            with LogContext(trace_id=trace_id):
                try:
                    body = json.loads(message.body.decode())
                    logger.info(f"Processing message: {message.message_id}, type={body.get('type')}")
                    
                    # 幂等性检查
                    message_id = message.message_id or body.get("idempotency_key")
                    if await self._check_message_processed(message_id):
                        logger.info(f"Message already processed: {message_id}")
                        return
                    
                    # 处理消息
                    await handler(body)
                    
                    # 标记消息已处理
                    await self._mark_message_processed(message_id)
                    
                    logger.info(f"Message processed successfully: {message_id}")
                    
                except Exception as e:
                    retry_count = message.headers.get("x-retry-count", 0)
                    max_retries = 3
                    
                    if retry_count < max_retries:
                        # 重试
                        logger.warning(f"Message processing failed, retrying ({retry_count + 1}/{max_retries}): {e}")
                        await self._retry_message(message, retry_count + 1)
                    else:
                        # 进入死信队列
                        logger.error(f"Message processing failed after {max_retries} retries: {e}")
                        raise  # 抛出异常让消息进入死信队列
    
    async def _check_message_processed(self, message_id: str) -> bool:
        """检查消息是否已处理（幂等性）"""
        # 这里可以使用Redis或数据库来记录已处理的消息
        # 简化实现，实际项目中需要持久化存储
        return False
    
    async def _mark_message_processed(self, message_id: str):
        """标记消息已处理"""
        # 这里可以使用Redis或数据库来记录
        pass
    
    async def _retry_message(self, message: aio_pika.IncomingMessage, retry_count: int):
        """重试消息"""
        async with self._channel_pool.acquire() as channel:
            exchange = await channel.get_exchange(settings.RABBITMQ_EXCHANGE)
            
            # 重新发布消息，增加重试计数
            await exchange.publish(
                aio_pika.Message(
                    body=message.body,
                    headers={**message.headers, "x-retry-count": retry_count},
                    delivery_mode=DeliveryMode.PERSISTENT,
                ),
                routing_key=message.routing_key
            )
    
    async def handle_payment_success(self, body: Dict[str, Any]):
        """处理支付成功消息"""
        order_id = body.get("order_id")
        payment_type = body.get("payment_type", "full")
        
        if not order_id:
            logger.error("Missing order_id in payment success message")
            return
        
        logger.info(f"Handling payment success for order: {order_id}, type={payment_type}")
        
        async with get_db_session() as session:
            # 获取订单
            from sqlalchemy import select
            result = await session.execute(
                select(Order).where(Order.id == order_id)
            )
            order = result.scalar_one_or_none()
            
            if not order:
                logger.error(f"Order not found: {order_id}")
                return
            
            # 预售订单尾款支付后才扣减库存
            if order.order_type == "presale":
                if payment_type == "final" or order.status == OrderStatus.PRESALE_FINAL_PAYMENT_PAID:
                    # 扣减预售库存
                    success, failed_skus = await self._inventory_service.batch_deduct_for_order(
                        session, order_id
                    )
                    if not success:
                        logger.error(f"Failed to deduct inventory for order: {order_id}, skus={failed_skus}")
                        raise InventoryLockException(
                            ",".join(failed_skus), 0, "扣减库存失败", ""
                        )
                    
                    # 触发发货调度
                    await self._order_service.dispatch_order(order_id)
                    logger.info(f"Presale order dispatched after final payment: {order_id}")
                else:
                    logger.info(f"Presale deposit paid, waiting for final payment: {order_id}")
            else:
                # 普通订单，直接扣减库存
                success, failed_skus = await self._inventory_service.batch_deduct_for_order(
                    session, order_id
                )
                if not success:
                    logger.error(f"Failed to deduct inventory for order: {order_id}, skus={failed_skus}")
                    raise InventoryLockException(
                        ",".join(failed_skus), 0, "扣减库存失败", ""
                    )
                
                # 触发发货调度
                await self._order_service.dispatch_order(order_id)
                logger.info(f"Normal order dispatched after payment: {order_id}")
    
    async def handle_order_cancelled(self, body: Dict[str, Any]):
        """处理订单取消消息"""
        order_id = body.get("order_id")
        
        if not order_id:
            logger.error("Missing order_id in order cancelled message")
            return
        
        logger.info(f"Handling order cancelled: {order_id}")
        
        async with get_db_session() as session:
            # 获取该订单的所有锁定记录
            from sqlalchemy import select, and_
            result = await session.execute(
                select(InventoryLock).where(
                    and_(
                        InventoryLock.order_id == order_id,
                        InventoryLock.status == InventoryLockStatus.LOCKED
                    )
                )
            )
            locks = result.scalars().all()
            
            for lock in locks:
                try:
                    await self._inventory_service.unlock_stock(session, lock.id, "system")
                    logger.info(f"Released inventory lock: {lock.id} for order: {order_id}")
                except Exception as e:
                    logger.error(f"Failed to release lock {lock.id}: {e}")
    
    async def handle_inventory_release(self, body: Dict[str, Any]):
        """处理库存释放消息（定时任务触发）"""
        logger.info("Handling inventory release message")
        
        async with get_db_session() as session:
            released_count = await self._inventory_service.release_expired_locks(session)
            logger.info(f"Released {released_count} expired inventory locks")
    
    async def start_consuming(self):
        """开始消费消息"""
        self._running = True
        
        async with self._channel_pool.acquire() as channel:
            # 支付成功队列
            payment_queue = await channel.get_queue(settings.RABBITMQ_QUEUE_PAYMENT_SUCCESS)
            payment_consumer = await payment_queue.consume(
                lambda msg: self.process_message(msg, self.handle_payment_success)
            )
            self._consumer_tags.append(payment_consumer)
            
            # 库存扣减队列
            inventory_queue = await channel.get_queue(settings.RABBITMQ_QUEUE_INVENTORY_DEDUCT)
            inventory_consumer = await inventory_queue.consume(
                lambda msg: self.process_message(msg, self.handle_inventory_release)
            )
            self._consumer_tags.append(inventory_consumer)
            
            logger.info("Started consuming messages")
            
            # 保持运行
            while self._running:
                await asyncio.sleep(1)
    
    async def stop(self):
        """停止消费者"""
        self._running = False
        
        # 取消所有消费者
        async with self._channel_pool.acquire() as channel:
            for tag in self._consumer_tags:
                await channel.basic_cancel(tag)
        
        # 关闭服务
        await self._inventory_service.close()
        await self._order_service.close()
        
        if self._connection:
            await self._connection.close()
        
        logger.info("Inventory consumer stopped")
    
    async def run(self):
        """运行消费者"""
        await self.connect()
        await self.setup_queues()
        await self.start_consuming()


class InventoryProducer:
    """库存消息生产者"""
    
    def __init__(self):
        self._connection: Optional[aio_pika.RobustConnection] = None
        self._channel: Optional[aio_pika.Channel] = None
        self._exchange: Optional[aio_pika.Exchange] = None
    
    async def connect(self):
        """连接RabbitMQ"""
        self._connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
        self._channel = await self._connection.channel()
        self._exchange = await self._channel.declare_exchange(
            settings.RABBITMQ_EXCHANGE,
            ExchangeType.TOPIC,
            durable=True
        )
        logger.info("Inventory producer connected")
    
    async def publish_payment_success(
        self,
        order_id: str,
        payment_type: str = "full",
        trace_id: Optional[str] = None
    ):
        """发布支付成功消息"""
        if not self._exchange:
            await self.connect()
        
        message = {
            "type": MessageType.ORDER_PAYMENT_SUCCESS,
            "order_id": order_id,
            "payment_type": payment_type,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        await self._exchange.publish(
            aio_pika.Message(
                body=json.dumps(message).encode(),
                headers={"trace_id": trace_id or str(datetime.utcnow().timestamp())},
                delivery_mode=DeliveryMode.PERSISTENT,
                message_id=f"{order_id}:{payment_type}:{datetime.utcnow().timestamp()}"
            ),
            routing_key="order.payment.success"
        )
        
        logger.info(f"Published payment success message: {order_id}")
    
    async def publish_order_cancelled(self, order_id: str, trace_id: Optional[str] = None):
        """发布订单取消消息"""
        if not self._exchange:
            await self.connect()
        
        message = {
            "type": MessageType.ORDER_CANCELLED,
            "order_id": order_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        await self._exchange.publish(
            aio_pika.Message(
                body=json.dumps(message).encode(),
                headers={"trace_id": trace_id or str(datetime.utcnow().timestamp())},
                delivery_mode=DeliveryMode.PERSISTENT,
                message_id=f"cancel:{order_id}:{datetime.utcnow().timestamp()}"
            ),
            routing_key="order.cancelled"
        )
        
        logger.info(f"Published order cancelled message: {order_id}")
    
    async def close(self):
        """关闭连接"""
        if self._connection:
            await self._connection.close()
            logger.info("Inventory producer closed")


# 全局实例
_consumer: Optional[InventoryConsumer] = None
_producer: Optional[InventoryProducer] = None


async def get_consumer() -> InventoryConsumer:
    """获取消费者实例"""
    global _consumer
    if _consumer is None:
        _consumer = InventoryConsumer()
    return _consumer


async def get_producer() -> InventoryProducer:
    """获取生产者实例"""
    global _producer
    if _producer is None:
        _producer = InventoryProducer()
        await _producer.connect()
    return _producer
