# -*- coding: utf-8 -*-
"""
全局业务异常定义模块
包含10+业务异常类型，支持错误码、错误消息、链路追踪
"""
from typing import Optional, Dict, Any
from enum import Enum


class ErrorCode(Enum):
    """错误码枚举"""
    # 系统错误 1xxxx
    SYSTEM_ERROR = 10000
    PARAM_ERROR = 10001
    RATE_LIMIT_ERROR = 10002
    
    # 订单错误 2xxxx
    ORDER_NOT_FOUND = 20001
    ORDER_STATUS_ERROR = 20002
    ORDER_CREATE_ERROR = 20003
    ORDER_CANCEL_ERROR = 20004
    ORDER_TIMEOUT_ERROR = 20005
    
    # 库存错误 3xxxx
    INVENTORY_INSUFFICIENT = 30001
    INVENTORY_LOCK_FAILED = 30002
    INVENTORY_UNLOCK_FAILED = 30003
    INVENTORY_DEDUCT_FAILED = 30004
    INVENTORY_ROLLBACK_FAILED = 30005
    INVENTORY_CONFLICT = 30006
    
    # 支付错误 4xxxx
    PAYMENT_FAILED = 40001
    PAYMENT_TIMEOUT = 40002
    PAYMENT_AMOUNT_ERROR = 40003
    PAYMENT_STATUS_ERROR = 40004
    
    # 预售错误 5xxxx
    PRESALE_NOT_START = 50001
    PRESALE_ALREADY_END = 50002
    PRESALE_DEPOSIT_ERROR = 50003
    PRESALE_FINAL_PAY_ERROR = 50004
    PRESALE_STATUS_ERROR = 50005
    
    # 物流错误 6xxxx
    LOGISTICS_CREATE_FAILED = 60001
    LOGISTICS_DISPATCH_FAILED = 60002
    
    # 重试/幂等错误 7xxxx
    RETRY_EXHAUSTED = 70001
    IDEMPOTENCY_VIOLATION = 70002


class BaseException(Exception):
    """基础异常类"""
    
    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.SYSTEM_ERROR,
        trace_id: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.trace_id = trace_id
        self.extra = extra or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "code": self.code.value,
            "message": self.message,
            "trace_id": self.trace_id,
            "extra": self.extra
        }
    
    def __str__(self) -> str:
        return f"[{self.code.value}] {self.message} (trace_id={self.trace_id})"


# ==================== 订单相关异常 ====================

class OrderException(BaseException):
    """订单基础异常"""
    pass


class OrderNotFoundException(OrderException):
    """订单不存在异常"""
    
    def __init__(self, order_id: str, trace_id: Optional[str] = None):
        super().__init__(
            message=f"订单不存在: {order_id}",
            code=ErrorCode.ORDER_NOT_FOUND,
            trace_id=trace_id,
            extra={"order_id": order_id}
        )


class OrderStatusException(OrderException):
    """订单状态异常"""
    
    def __init__(
        self,
        order_id: str,
        current_status: str,
        expected_status: str,
        trace_id: Optional[str] = None
    ):
        super().__init__(
            message=f"订单状态异常: 当前={current_status}, 期望={expected_status}",
            code=ErrorCode.ORDER_STATUS_ERROR,
            trace_id=trace_id,
            extra={
                "order_id": order_id,
                "current_status": current_status,
                "expected_status": expected_status
            }
        )


# ==================== 库存相关异常 ====================

class InventoryException(BaseException):
    """库存基础异常"""
    pass


class InventoryInsufficientException(InventoryException):
    """库存不足异常"""
    
    def __init__(
        self,
        sku_id: str,
        requested: int,
        available: int,
        trace_id: Optional[str] = None
    ):
        super().__init__(
            message=f"库存不足: SKU={sku_id}, 请求={requested}, 可用={available}",
            code=ErrorCode.INVENTORY_INSUFFICIENT,
            trace_id=trace_id,
            extra={
                "sku_id": sku_id,
                "requested": requested,
                "available": available
            }
        )


class InventoryLockException(InventoryException):
    """库存锁定异常"""
    
    def __init__(
        self,
        sku_id: str,
        quantity: int,
        reason: str,
        trace_id: Optional[str] = None
    ):
        super().__init__(
            message=f"库存锁定失败: SKU={sku_id}, 数量={quantity}, 原因={reason}",
            code=ErrorCode.INVENTORY_LOCK_FAILED,
            trace_id=trace_id,
            extra={
                "sku_id": sku_id,
                "quantity": quantity,
                "reason": reason
            }
        )


# ==================== 支付相关异常 ====================

class PaymentException(BaseException):
    """支付基础异常"""
    pass


class PaymentTimeoutException(PaymentException):
    """支付超时异常"""
    
    def __init__(
        self,
        order_id: str,
        timeout_minutes: int,
        trace_id: Optional[str] = None
    ):
        super().__init__(
            message=f"支付超时: 订单={order_id}, 超时={timeout_minutes}分钟",
            code=ErrorCode.PAYMENT_TIMEOUT,
            trace_id=trace_id,
            extra={
                "order_id": order_id,
                "timeout_minutes": timeout_minutes
            }
        )


class PaymentAmountException(PaymentException):
    """支付金额异常"""
    
    def __init__(
        self,
        order_id: str,
        expected: float,
        actual: float,
        trace_id: Optional[str] = None
    ):
        super().__init__(
            message=f"支付金额不匹配: 期望={expected}, 实际={actual}",
            code=ErrorCode.PAYMENT_AMOUNT_ERROR,
            trace_id=trace_id,
            extra={
                "order_id": order_id,
                "expected_amount": expected,
                "actual_amount": actual
            }
        )


# ==================== 预售相关异常 ====================

class PresaleException(BaseException):
    """预售基础异常"""
    pass


class PresaleTimeException(PresaleException):
    """预售时间异常"""
    
    def __init__(
        self,
        sku_id: str,
        current_time: str,
        start_time: str,
        end_time: str,
        trace_id: Optional[str] = None
    ):
        super().__init__(
            message=f"不在预售时间范围内: 当前={current_time}, 预售期={start_time}~{end_time}",
            code=ErrorCode.PRESALE_NOT_START if current_time < start_time else ErrorCode.PRESALE_ALREADY_END,
            trace_id=trace_id,
            extra={
                "sku_id": sku_id,
                "current_time": current_time,
                "start_time": start_time,
                "end_time": end_time
            }
        )


class PresaleStatusException(PresaleException):
    """预售状态异常"""
    
    def __init__(
        self,
        order_id: str,
        current_status: str,
        expected_status: str,
        trace_id: Optional[str] = None
    ):
        super().__init__(
            message=f"预售订单状态异常: 当前={current_status}, 期望={expected_status}",
            code=ErrorCode.PRESALE_STATUS_ERROR,
            trace_id=trace_id,
            extra={
                "order_id": order_id,
                "current_status": current_status,
                "expected_status": expected_status
            }
        )


# ==================== 物流相关异常 ====================

class LogisticsException(BaseException):
    """物流基础异常"""
    pass


# ==================== 重试/幂等相关异常 ====================

class RetryException(BaseException):
    """重试耗尽异常"""
    
    def __init__(
        self,
        operation: str,
        max_retries: int,
        last_error: str,
        trace_id: Optional[str] = None
    ):
        super().__init__(
            message=f"操作重试耗尽: {operation}, 最大重试={max_retries}, 最后错误={last_error}",
            code=ErrorCode.RETRY_EXHAUSTED,
            trace_id=trace_id,
            extra={
                "operation": operation,
                "max_retries": max_retries,
                "last_error": last_error
            }
        )


class IdempotencyException(BaseException):
    """幂等性校验异常"""
    
    def __init__(
        self,
        key: str,
        existing_id: str,
        trace_id: Optional[str] = None
    ):
        super().__init__(
            message=f"重复请求: 幂等键={key}, 已有记录={existing_id}",
            code=ErrorCode.IDEMPOTENCY_VIOLATION,
            trace_id=trace_id,
            extra={
                "idempotency_key": key,
                "existing_id": existing_id
            }
        )
