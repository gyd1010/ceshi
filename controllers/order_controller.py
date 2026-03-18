# -*- coding: utf-8 -*-
"""
订单控制器模块
新增3个API：创建预售订单、支付尾款、取消预售订单
统一入参校验、返回格式
"""
from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request
from pydantic import BaseModel, Field, validator
from sqlalchemy.ext.asyncio import AsyncSession

from models.order import OrderType, OrderStatus, PresaleStatus
from models.base import get_db_session
from services.order_service import OrderService
from services.inventory_service import InventoryService
from utils.exceptions import (
    OrderException,
    OrderNotFoundException,
    OrderStatusException,
    InventoryException,
    PaymentException,
    PresaleException,
    BaseException as AppBaseException
)
from utils.loggers import get_logger, LogContext

logger = get_logger(__name__)
router = APIRouter(prefix="/api/v1/orders", tags=["orders"])


# ==================== 请求/响应模型 ====================

class OrderItemRequest(BaseModel):
    """订单商品项请求"""
    sku_id: str = Field(..., min_length=1, max_length=32, description="SKU ID")
    spu_id: Optional[str] = Field(None, max_length=32, description="SPU ID")
    sku_name: Optional[str] = Field(None, max_length=255, description="SKU名称")
    sku_image: Optional[str] = Field(None, max_length=512, description="SKU图片")
    quantity: int = Field(..., ge=1, le=9999, description="数量")
    price: Decimal = Field(..., gt=0, description="单价")
    original_price: Optional[Decimal] = Field(None, gt=0, description="原价")
    
    @validator('quantity')
    def validate_quantity(cls, v):
        if v <= 0:
            raise ValueError('数量必须大于0')
        return v


class ShippingAddress(BaseModel):
    """收货地址"""
    name: str = Field(..., max_length=100, description="收件人姓名")
    phone: str = Field(..., max_length=20, description="收件人电话")
    province: str = Field(..., max_length=50, description="省份")
    city: str = Field(..., max_length=50, description="城市")
    district: str = Field(..., max_length=50, description="区县")
    address: str = Field(..., max_length=500, description="详细地址")
    zip_code: Optional[str] = Field(None, max_length=10, description="邮编")


class CreateOrderRequest(BaseModel):
    """创建订单请求"""
    user_id: str = Field(..., min_length=1, description="用户ID")
    items: List[OrderItemRequest] = Field(..., min_items=1, max_items=100, description="商品列表")
    shipping_address: ShippingAddress = Field(..., description="收货地址")
    order_type: OrderType = Field(default=OrderType.NORMAL, description="订单类型")
    idempotency_key: Optional[str] = Field(None, max_length=64, description="幂等键")
    source: Optional[str] = Field(default="app", max_length=32, description="来源")
    
    @validator('items')
    def validate_items(cls, v):
        if not v:
            raise ValueError('商品列表不能为空')
        return v


class PresaleConfig(BaseModel):
    """预售配置"""
    deposit_amount: Decimal = Field(..., gt=0, description="定金金额")
    final_payment_amount: Decimal = Field(..., gt=0, description="尾款金额")
    presale_start_time: datetime = Field(..., description="预售开始时间")
    presale_end_time: datetime = Field(..., description="预售结束时间")
    final_payment_start_time: datetime = Field(..., description="尾款支付开始时间")
    final_payment_end_time: datetime = Field(..., description="尾款支付结束时间")
    
    @validator('presale_end_time')
    def validate_presale_time(cls, v, values):
        if 'presale_start_time' in values and v <= values['presale_start_time']:
            raise ValueError('预售结束时间必须晚于开始时间')
        return v
    
    @validator('final_payment_end_time')
    def validate_final_payment_time(cls, v, values):
        if 'final_payment_start_time' in values and v <= values['final_payment_start_time']:
            raise ValueError('尾款支付结束时间必须晚于开始时间')
        return v


class CreatePresaleOrderRequest(BaseModel):
    """创建预售订单请求"""
    user_id: str = Field(..., min_length=1, description="用户ID")
    items: List[OrderItemRequest] = Field(..., min_items=1, max_items=100, description="商品列表")
    shipping_address: ShippingAddress = Field(..., description="收货地址")
    presale_config: PresaleConfig = Field(..., description="预售配置")
    idempotency_key: Optional[str] = Field(None, max_length=64, description="幂等键")
    source: Optional[str] = Field(default="app", max_length=32, description="来源")


class PayOrderRequest(BaseModel):
    """支付订单请求"""
    payment_amount: Decimal = Field(..., gt=0, description="支付金额")
    payment_method: str = Field(..., max_length=32, description="支付方式")
    payment_no: str = Field(..., max_length=128, description="支付流水号")


class PayFinalPaymentRequest(BaseModel):
    """支付尾款请求"""
    payment_amount: Decimal = Field(..., gt=0, description="支付金额")
    payment_method: str = Field(..., max_length=32, description="支付方式")
    payment_no: str = Field(..., max_length=128, description="支付流水号")


class CancelOrderRequest(BaseModel):
    """取消订单请求"""
    reason: str = Field(..., min_length=1, max_length=500, description="取消原因")


class OrderItemResponse(BaseModel):
    """订单商品项响应"""
    id: str
    sku_id: str
    sku_name: str
    quantity: int
    sale_price: Decimal
    subtotal: Decimal
    is_presale_item: bool
    
    class Config:
        from_attributes = True


class OrderResponse(BaseModel):
    """订单响应"""
    id: str
    order_no: str
    user_id: str
    order_type: OrderType
    status: OrderStatus
    presale_status: Optional[PresaleStatus]
    total_amount: Decimal
    deposit_amount: Optional[Decimal]
    final_payment_amount: Optional[Decimal]
    pay_amount: Decimal
    paid_total: Decimal
    created_at: datetime
    expire_at: Optional[datetime]
    items: List[OrderItemResponse]
    
    class Config:
        from_attributes = True


class ApiResponse(BaseModel):
    """统一API响应格式"""
    code: int = Field(default=0, description="错误码，0表示成功")
    message: str = Field(default="success", description="错误信息")
    data: Optional[Any] = Field(None, description="响应数据")
    trace_id: Optional[str] = Field(None, description="链路追踪ID")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="响应时间")


# ==================== 依赖注入 ====================

async def get_order_service():
    """获取订单服务"""
    service = OrderService()
    try:
        yield service
    finally:
        await service.close()


async def get_inventory_service():
    """获取库存服务"""
    service = InventoryService()
    try:
        yield service
    finally:
        await service.close()


# ==================== 异常处理 ====================

async def handle_exception(request: Request, exc: Exception) -> ApiResponse:
    """统一异常处理"""
    trace_id = getattr(request.state, 'trace_id', None)
    
    if isinstance(exc, AppBaseException):
        logger.warning(f"Business exception: {exc.message}")
        return ApiResponse(
            code=exc.code.value,
            message=exc.message,
            trace_id=exc.trace_id or trace_id
        )
    elif isinstance(exc, HTTPException):
        return ApiResponse(
            code=exc.status_code,
            message=exc.detail,
            trace_id=trace_id
        )
    else:
        logger.exception(f"Unexpected error: {str(exc)}")
        return ApiResponse(
            code=500,
            message="系统内部错误",
            trace_id=trace_id
        )


# ==================== API路由 ====================

@router.post("/normal", response_model=ApiResponse)
async def create_normal_order(
    request: Request,
    req: CreateOrderRequest,
    order_service: OrderService = Depends(get_order_service),
    x_trace_id: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None)
):
    """
    创建普通订单
    
    - 支持幂等性控制
    - 自动锁定库存
    - 30分钟支付超时
    """
    trace_id = x_trace_id or str(datetime.utcnow().timestamp())
    user_id = x_user_id or req.user_id
    
    with LogContext(trace_id=trace_id, user_id=user_id):
        try:
            logger.info(f"Creating normal order for user: {user_id}")
            
            items_data = [
                {
                    "sku_id": item.sku_id,
                    "spu_id": item.spu_id,
                    "sku_name": item.sku_name or "",
                    "sku_image": item.sku_image,
                    "quantity": item.quantity,
                    "price": item.price
                }
                for item in req.items
            ]
            
            address_data = req.shipping_address.dict()
            
            order = await order_service.create_order(
                user_id=user_id,
                items=items_data,
                shipping_address=address_data,
                order_type=OrderType.NORMAL,
                idempotency_key=req.idempotency_key,
                source=req.source
            )
            
            return ApiResponse(
                data=OrderResponse(
                    id=order.id,
                    order_no=order.order_no,
                    user_id=order.user_id,
                    order_type=order.order_type,
                    status=order.status,
                    presale_status=order.presale_status,
                    total_amount=order.total_amount,
                    deposit_amount=order.deposit_amount,
                    final_payment_amount=order.final_payment_amount,
                    pay_amount=order.pay_amount,
                    paid_total=order.paid_total,
                    created_at=order.created_at,
                    expire_at=order.expire_at,
                    items=[
                        OrderItemResponse(
                            id=item.id,
                            sku_id=item.sku_id,
                            sku_name=item.sku_name,
                            quantity=item.quantity,
                            sale_price=item.sale_price,
                            subtotal=item.subtotal,
                            is_presale_item=item.is_presale_item
                        )
                        for item in order.items
                    ]
                ),
                trace_id=trace_id
            )
            
        except AppBaseException as e:
            raise HTTPException(status_code=400, detail=e.to_dict())
        except Exception as e:
            logger.exception(f"Create normal order failed: {e}")
            raise HTTPException(status_code=500, detail="创建订单失败")


@router.post("/presale", response_model=ApiResponse)
async def create_presale_order(
    request: Request,
    req: CreatePresaleOrderRequest,
    order_service: OrderService = Depends(get_order_service),
    x_trace_id: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None)
):
    """
    创建预售订单
    
    - 支持定金+尾款模式
    - 预售库存独立锁定
    - 定金24小时支付超时
    """
    trace_id = x_trace_id or str(datetime.utcnow().timestamp())
    user_id = x_user_id or req.user_id
    
    with LogContext(trace_id=trace_id, user_id=user_id):
        try:
            logger.info(f"Creating presale order for user: {user_id}")
            
            items_data = [
                {
                    "sku_id": item.sku_id,
                    "spu_id": item.spu_id,
                    "sku_name": item.sku_name or "",
                    "sku_image": item.sku_image,
                    "quantity": item.quantity,
                    "original_price": item.original_price or item.price,
                    "sale_price": item.price
                }
                for item in req.items
            ]
            
            address_data = req.shipping_address.dict()
            presale_config_data = req.presale_config.dict()
            
            order = await order_service.create_order(
                user_id=user_id,
                items=items_data,
                shipping_address=address_data,
                order_type=OrderType.PRESALE,
                presale_config=presale_config_data,
                idempotency_key=req.idempotency_key,
                source=req.source
            )
            
            return ApiResponse(
                data=OrderResponse(
                    id=order.id,
                    order_no=order.order_no,
                    user_id=order.user_id,
                    order_type=order.order_type,
                    status=order.status,
                    presale_status=order.presale_status,
                    total_amount=order.total_amount,
                    deposit_amount=order.deposit_amount,
                    final_payment_amount=order.final_payment_amount,
                    pay_amount=order.pay_amount,
                    paid_total=order.paid_total,
                    created_at=order.created_at,
                    expire_at=order.expire_at,
                    items=[
                        OrderItemResponse(
                            id=item.id,
                            sku_id=item.sku_id,
                            sku_name=item.sku_name,
                            quantity=item.quantity,
                            sale_price=item.sale_price,
                            subtotal=item.subtotal,
                            is_presale_item=item.is_presale_item
                        )
                        for item in order.items
                    ]
                ),
                trace_id=trace_id
            )
            
        except AppBaseException as e:
            raise HTTPException(status_code=400, detail=e.to_dict())
        except Exception as e:
            logger.exception(f"Create presale order failed: {e}")
            raise HTTPException(status_code=500, detail="创建预售订单失败")


@router.post("/{order_id}/pay", response_model=ApiResponse)
async def pay_order(
    order_id: str,
    req: PayOrderRequest,
    order_service: OrderService = Depends(get_order_service),
    x_trace_id: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None)
):
    """
    支付订单（普通订单全款或预售定金）
    """
    trace_id = x_trace_id or str(datetime.utcnow().timestamp())
    
    with LogContext(trace_id=trace_id, user_id=x_user_id):
        try:
            logger.info(f"Paying order: {order_id}")
            
            order = await order_service.pay_order(
                order_id=order_id,
                payment_amount=req.payment_amount,
                payment_method=req.payment_method,
                payment_no=req.payment_no,
                payment_type="deposit"  # 根据订单类型自动判断
            )
            
            return ApiResponse(
                data=OrderResponse(
                    id=order.id,
                    order_no=order.order_no,
                    user_id=order.user_id,
                    order_type=order.order_type,
                    status=order.status,
                    presale_status=order.presale_status,
                    total_amount=order.total_amount,
                    deposit_amount=order.deposit_amount,
                    final_payment_amount=order.final_payment_amount,
                    pay_amount=order.pay_amount,
                    paid_total=order.paid_total,
                    created_at=order.created_at,
                    expire_at=order.expire_at,
                    items=[]
                ),
                trace_id=trace_id
            )
            
        except AppBaseException as e:
            raise HTTPException(status_code=400, detail=e.to_dict())
        except Exception as e:
            logger.exception(f"Pay order failed: {e}")
            raise HTTPException(status_code=500, detail="支付失败")


@router.post("/{order_id}/pay-final", response_model=ApiResponse)
async def pay_final_payment(
    order_id: str,
    req: PayFinalPaymentRequest,
    order_service: OrderService = Depends(get_order_service),
    x_trace_id: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None)
):
    """
    支付预售订单尾款
    
    - 必须在尾款支付时间段内
    - 支付成功后自动扣减库存
    - 触发发货流程
    """
    trace_id = x_trace_id or str(datetime.utcnow().timestamp())
    
    with LogContext(trace_id=trace_id, user_id=x_user_id):
        try:
            logger.info(f"Paying final payment for order: {order_id}")
            
            order = await order_service.pay_order(
                order_id=order_id,
                payment_amount=req.payment_amount,
                payment_method=req.payment_method,
                payment_no=req.payment_no,
                payment_type="final"
            )
            
            return ApiResponse(
                data=OrderResponse(
                    id=order.id,
                    order_no=order.order_no,
                    user_id=order.user_id,
                    order_type=order.order_type,
                    status=order.status,
                    presale_status=order.presale_status,
                    total_amount=order.total_amount,
                    deposit_amount=order.deposit_amount,
                    final_payment_amount=order.final_payment_amount,
                    pay_amount=order.pay_amount,
                    paid_total=order.paid_total,
                    created_at=order.created_at,
                    expire_at=order.expire_at,
                    items=[]
                ),
                trace_id=trace_id
            )
            
        except AppBaseException as e:
            raise HTTPException(status_code=400, detail=e.to_dict())
        except Exception as e:
            logger.exception(f"Pay final payment failed: {e}")
            raise HTTPException(status_code=500, detail="支付尾款失败")


@router.post("/{order_id}/cancel", response_model=ApiResponse)
async def cancel_order(
    order_id: str,
    req: CancelOrderRequest,
    order_service: OrderService = Depends(get_order_service),
    x_trace_id: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None)
):
    """
    取消订单
    
    - 自动释放库存锁定
    - 支持普通订单和预售订单
    """
    trace_id = x_trace_id or str(datetime.utcnow().timestamp())
    
    with LogContext(trace_id=trace_id, user_id=x_user_id):
        try:
            logger.info(f"Cancelling order: {order_id}")
            
            order = await order_service.cancel_order(
                order_id=order_id,
                reason=req.reason,
                operator_id=x_user_id
            )
            
            return ApiResponse(
                data=OrderResponse(
                    id=order.id,
                    order_no=order.order_no,
                    user_id=order.user_id,
                    order_type=order.order_type,
                    status=order.status,
                    presale_status=order.presale_status,
                    total_amount=order.total_amount,
                    deposit_amount=order.deposit_amount,
                    final_payment_amount=order.final_payment_amount,
                    pay_amount=order.pay_amount,
                    paid_total=order.paid_total,
                    created_at=order.created_at,
                    expire_at=order.expire_at,
                    items=[]
                ),
                trace_id=trace_id
            )
            
        except AppBaseException as e:
            raise HTTPException(status_code=400, detail=e.to_dict())
        except Exception as e:
            logger.exception(f"Cancel order failed: {e}")
            raise HTTPException(status_code=500, detail="取消订单失败")


@router.get("/{order_id}", response_model=ApiResponse)
async def get_order(
    order_id: str,
    order_service: OrderService = Depends(get_order_service),
    x_trace_id: Optional[str] = Header(None)
):
    """获取订单详情"""
    trace_id = x_trace_id or str(datetime.utcnow().timestamp())
    
    with LogContext(trace_id=trace_id):
        try:
            order = await order_service.get_order(order_id)
            
            if not order:
                raise OrderNotFoundException(order_id, trace_id)
            
            return ApiResponse(
                data=OrderResponse(
                    id=order.id,
                    order_no=order.order_no,
                    user_id=order.user_id,
                    order_type=order.order_type,
                    status=order.status,
                    presale_status=order.presale_status,
                    total_amount=order.total_amount,
                    deposit_amount=order.deposit_amount,
                    final_payment_amount=order.final_payment_amount,
                    pay_amount=order.pay_amount,
                    paid_total=order.paid_total,
                    created_at=order.created_at,
                    expire_at=order.expire_at,
                    items=[
                        OrderItemResponse(
                            id=item.id,
                            sku_id=item.sku_id,
                            sku_name=item.sku_name,
                            quantity=item.quantity,
                            sale_price=item.sale_price,
                            subtotal=item.subtotal,
                            is_presale_item=item.is_presale_item
                        )
                        for item in order.items
                    ]
                ),
                trace_id=trace_id
            )
            
        except AppBaseException as e:
            raise HTTPException(status_code=400, detail=e.to_dict())
        except Exception as e:
            logger.exception(f"Get order failed: {e}")
            raise HTTPException(status_code=500, detail="获取订单失败")


@router.get("", response_model=ApiResponse)
async def list_orders(
    user_id: str = Query(..., description="用户ID"),
    status: Optional[OrderStatus] = Query(None, description="订单状态"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    order_service: OrderService = Depends(get_order_service),
    x_trace_id: Optional[str] = Header(None)
):
    """查询用户订单列表"""
    trace_id = x_trace_id or str(datetime.utcnow().timestamp())
    
    with LogContext(trace_id=trace_id, user_id=user_id):
        try:
            orders, total = await order_service.list_user_orders(
                user_id=user_id,
                status=status,
                page=page,
                page_size=page_size
            )
            
            return ApiResponse(
                data={
                    "items": [
                        OrderResponse(
                            id=order.id,
                            order_no=order.order_no,
                            user_id=order.user_id,
                            order_type=order.order_type,
                            status=order.status,
                            presale_status=order.presale_status,
                            total_amount=order.total_amount,
                            deposit_amount=order.deposit_amount,
                            final_payment_amount=order.final_payment_amount,
                            pay_amount=order.pay_amount,
                            paid_total=order.paid_total,
                            created_at=order.created_at,
                            expire_at=order.expire_at,
                            items=[]
                        )
                        for order in orders
                    ],
                    "total": total,
                    "page": page,
                    "page_size": page_size
                },
                trace_id=trace_id
            )
            
        except Exception as e:
            logger.exception(f"List orders failed: {e}")
            raise HTTPException(status_code=500, detail="查询订单列表失败")


@router.get("/inventory/{sku_id}", response_model=ApiResponse)
async def get_inventory_status(
    sku_id: str,
    inventory_service: InventoryService = Depends(get_inventory_service),
    x_trace_id: Optional[str] = Header(None)
):
    """获取库存状态"""
    trace_id = x_trace_id or str(datetime.utcnow().timestamp())
    
    with LogContext(trace_id=trace_id):
        try:
            async with get_db_session() as session:
                status = await inventory_service.get_inventory_status(session, sku_id)
                
                if not status:
                    raise HTTPException(status_code=404, detail="库存记录不存在")
                
                return ApiResponse(
                    data=status,
                    trace_id=trace_id
                )
                
        except Exception as e:
            logger.exception(f"Get inventory status failed: {e}")
            raise HTTPException(status_code=500, detail="获取库存状态失败")
