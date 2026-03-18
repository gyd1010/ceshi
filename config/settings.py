# -*- coding: utf-8 -*-
"""
系统配置文件
包含数据库、Redis、RabbitMQ等配置
"""
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """应用配置类"""
    
    # 应用配置
    APP_NAME: str = "Order Fulfillment System"
    DEBUG: bool = False
    ENV: str = "production"
    
    # 数据库配置
    DATABASE_URL: str = "postgresql://user:password@localhost:5432/order_db"
    DATABASE_POOL_SIZE: int = 20
    DATABASE_MAX_OVERFLOW: int = 10
    DATABASE_POOL_TIMEOUT: int = 30
    
    # Redis配置
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_POOL_SIZE: int = 50
    REDIS_SOCKET_TIMEOUT: int = 5
    REDIS_SOCKET_CONNECT_TIMEOUT: int = 5
    
    # RabbitMQ配置
    RABBITMQ_URL: str = "amqp://guest:guest@localhost:5672/"
    RABBITMQ_EXCHANGE: str = "order_exchange"
    RABBITMQ_QUEUE_ORDER_CREATED: str = "order_created_queue"
    RABBITMQ_QUEUE_PAYMENT_SUCCESS: str = "payment_success_queue"
    RABBITMQ_QUEUE_INVENTORY_DEDUCT: str = "inventory_deduct_queue"
    RABBITMQ_DLX_EXCHANGE: str = "order_dlx_exchange"
    RABBITMQ_DLX_QUEUE: str = "order_dlx_queue"
    
    # 订单配置
    ORDER_EXPIRE_MINUTES: int = 30
    PRESALE_DEPOSIT_EXPIRE_HOURS: int = 24
    PRESALE_FINAL_PAY_EXPIRE_HOURS: int = 72
    ORDER_AUTO_CANCEL_MINUTES: int = 30
    
    # 库存配置
    INVENTORY_LOCK_TIMEOUT_SECONDS: int = 300  # 库存锁定超时时间
    INVENTORY_RETRY_TIMES: int = 3
    INVENTORY_RETRY_DELAY_SECONDS: float = 0.1
    
    # 日志配置
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"
    LOG_FILE_PATH: Optional[str] = None
    
    # 性能配置
    MAX_CONCURRENT_ORDERS: int = 1000
    API_RATE_LIMIT_PER_MINUTE: int = 1000
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# 全局配置实例
settings = Settings()
