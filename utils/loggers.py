# -*- coding: utf-8 -*-
"""
全局日志模块
支持JSON格式、链路追踪、上下文管理
"""
import json
import logging
import sys
import uuid
from contextvars import ContextVar
from datetime import datetime
from typing import Optional, Dict, Any, Union
from functools import wraps

from config.settings import settings


# 链路追踪ID上下文
trace_id_var: ContextVar[str] = ContextVar('trace_id', default='')
user_id_var: ContextVar[str] = ContextVar('user_id', default='')
request_id_var: ContextVar[str] = ContextVar('request_id', default='')


class JSONFormatter(logging.Formatter):
    """JSON格式日志格式化器"""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "trace_id": trace_id_var.get() or getattr(record, 'trace_id', ''),
            "user_id": user_id_var.get() or getattr(record, 'user_id', ''),
            "request_id": request_id_var.get() or getattr(record, 'request_id', ''),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # 添加额外字段
        if hasattr(record, 'extra'):
            log_data.update(record.extra)
        
        # 添加异常信息
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data, ensure_ascii=False, default=str)


class ContextFilter(logging.Filter):
    """上下文过滤器"""
    
    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = trace_id_var.get()
        record.user_id = user_id_var.get()
        record.request_id = request_id_var.get()
        return True


def setup_logging() -> logging.Logger:
    """配置日志系统"""
    logger = logging.getLogger("order_system")
    logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper()))
    
    # 清除现有处理器
    logger.handlers.clear()
    
    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.addFilter(ContextFilter())
    
    if settings.LOG_FORMAT == "json":
        console_handler.setFormatter(JSONFormatter())
    else:
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(trace_id)s] %(message)s'
        )
        console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    
    # 文件处理器（如果配置）
    if settings.LOG_FILE_PATH:
        file_handler = logging.FileHandler(settings.LOG_FILE_PATH)
        file_handler.addFilter(ContextFilter())
        file_handler.setFormatter(JSONFormatter())
        logger.addHandler(file_handler)
    
    return logger


# 全局日志实例
_logger: Optional[logging.Logger] = None


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """获取日志记录器"""
    global _logger
    if _logger is None:
        _logger = setup_logging()
    
    if name:
        return _logger.getChild(name)
    return _logger


class LogContext:
    """日志上下文管理器"""
    
    def __init__(
        self,
        trace_id: Optional[str] = None,
        user_id: Optional[str] = None,
        request_id: Optional[str] = None
    ):
        self.trace_id = trace_id or str(uuid.uuid4())
        self.user_id = user_id or ""
        self.request_id = request_id or str(uuid.uuid4())
        self.tokens = []
    
    def __enter__(self):
        self.tokens = [
            trace_id_var.set(self.trace_id),
            user_id_var.set(self.user_id),
            request_id_var.set(self.request_id)
        ]
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        for token in reversed(self.tokens):
            if token:
                try:
                    trace_id_var.reset(token)
                except Exception:
                    pass
    
    @property
    def context(self) -> Dict[str, str]:
        """获取当前上下文"""
        return {
            "trace_id": self.trace_id,
            "user_id": self.user_id,
            "request_id": self.request_id
        }


def log_execution_time(logger_name: Optional[str] = None):
    """记录执行时间装饰器"""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            logger = get_logger(logger_name or func.__module__)
            start = datetime.utcnow()
            try:
                result = await func(*args, **kwargs)
                duration = (datetime.utcnow() - start).total_seconds() * 1000
                logger.info(
                    f"Function {func.__name__} executed in {duration:.2f}ms",
                    extra={"extra": {"duration_ms": duration, "function": func.__name__}}
                )
                return result
            except Exception as e:
                duration = (datetime.utcnow() - start).total_seconds() * 1000
                logger.error(
                    f"Function {func.__name__} failed after {duration:.2f}ms: {str(e)}",
                    extra={"extra": {"duration_ms": duration, "function": func.__name__, "error": str(e)}}
                )
                raise
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            logger = get_logger(logger_name or func.__module__)
            start = datetime.utcnow()
            try:
                result = func(*args, **kwargs)
                duration = (datetime.utcnow() - start).total_seconds() * 1000
                logger.info(
                    f"Function {func.__name__} executed in {duration:.2f}ms",
                    extra={"extra": {"duration_ms": duration, "function": func.__name__}}
                )
                return result
            except Exception as e:
                duration = (datetime.utcnow() - start).total_seconds() * 1000
                logger.error(
                    f"Function {func.__name__} failed after {duration:.2f}ms: {str(e)}",
                    extra={"extra": {"duration_ms": duration, "function": func.__name__, "error": str(e)}}
                )
                raise
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


import asyncio
