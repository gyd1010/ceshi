# -*- coding: utf-8 -*-
"""
主应用入口
FastAPI应用配置和启动
"""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from config.settings import settings
from models.base import init_db, close_db
from controllers import order_router
from mq.consumers.inventory_consumer import get_consumer
from utils.loggers import get_logger, LogContext
from utils.exceptions import BaseException as AppBaseException

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时
    logger.info(f"Starting {settings.APP_NAME}...")
    
    # 初始化数据库
    await init_db()
    
    # 启动消息消费者（在后台运行）
    consumer_task = None
    try:
        consumer = await get_consumer()
        await consumer.connect()
        await consumer.setup_queues()
        consumer_task = asyncio.create_task(consumer.run())
        logger.info("Message consumer started")
    except Exception as e:
        logger.error(f"Failed to start message consumer: {e}")
    
    yield
    
    # 关闭时
    logger.info(f"Shutting down {settings.APP_NAME}...")
    
    # 停止消息消费者
    if consumer_task:
        consumer = await get_consumer()
        await consumer.stop()
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    
    # 关闭数据库
    await close_db()
    
    logger.info("Shutdown complete")


# 创建FastAPI应用
app = FastAPI(
    title=settings.APP_NAME,
    description="企业级订单履约系统 - 支持普通订单和预售订单",
    version="2.0.0",
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None,
    lifespan=lifespan
)

# 中间件配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)


# 请求拦截器 - 链路追踪
@app.middleware("http")
async def trace_middleware(request: Request, call_next):
    """链路追踪中间件"""
    trace_id = request.headers.get("X-Trace-ID", "")
    user_id = request.headers.get("X-User-ID", "")
    
    with LogContext(trace_id=trace_id, user_id=user_id):
        # 记录请求
        logger.info(
            f"Request: {request.method} {request.url.path}",
            extra={"extra": {
                "method": request.method,
                "path": request.url.path,
                "query": str(request.query_params),
                "client": request.client.host if request.client else None
            }}
        )
        
        # 设置请求状态
        request.state.trace_id = trace_id
        request.state.user_id = user_id
        
        # 处理请求
        start_time = asyncio.get_event_loop().time()
        try:
            response = await call_next(request)
            
            # 添加响应头
            response.headers["X-Trace-ID"] = trace_id
            
            # 记录响应
            duration = (asyncio.get_event_loop().time() - start_time) * 1000
            logger.info(
                f"Response: {request.method} {request.url.path} - {response.status_code} ({duration:.2f}ms)",
                extra={"extra": {
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": response.status_code,
                    "duration_ms": duration
                }}
            )
            
            return response
            
        except Exception as e:
            duration = (asyncio.get_event_loop().time() - start_time) * 1000
            logger.error(
                f"Error: {request.method} {request.url.path} - {str(e)} ({duration:.2f}ms)",
                extra={"extra": {
                    "method": request.method,
                    "path": request.url.path,
                    "error": str(e),
                    "duration_ms": duration
                }}
            )
            raise


# 全局异常处理
@app.exception_handler(AppBaseException)
async def app_exception_handler(request: Request, exc: AppBaseException):
    """应用异常处理"""
    logger.warning(f"Business exception: {exc.message}")
    return JSONResponse(
        status_code=400,
        content={
            "code": exc.code.value,
            "message": exc.message,
            "trace_id": exc.trace_id,
            "data": None,
            "timestamp": datetime.utcnow().isoformat()
        }
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """全局异常处理"""
    logger.exception(f"Unexpected error: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "code": 500,
            "message": "系统内部错误",
            "trace_id": getattr(request.state, 'trace_id', None),
            "data": None,
            "timestamp": datetime.utcnow().isoformat()
        }
    )


# 注册路由
app.include_router(order_router)


# 健康检查
@app.get("/health")
async def health_check():
    """健康检查接口"""
    return {
        "status": "healthy",
        "service": settings.APP_NAME,
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat()
    }


# 就绪检查
@app.get("/ready")
async def readiness_check():
    """就绪检查接口"""
    # 可以在这里检查数据库、Redis等依赖
    return {
        "status": "ready",
        "timestamp": datetime.utcnow().isoformat()
    }


# 启动入口
if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        workers=1 if settings.DEBUG else 4,
        log_level=settings.LOG_LEVEL.lower()
    )
