# 企业级订单履约系统 (Order Fulfillment System)

## 项目概述

这是一个基于 Python + FastAPI + SQLAlchemy + Redis + RabbitMQ 构建的企业级订单履约系统，支持普通订单和预售订单的完整生命周期管理。

## 核心特性

### 1. 订单管理
- **普通订单**：创建 → 支付 → 发货 → 完成
- **预售订单**：定金支付 → 尾款支付 → 发货 → 完成
- **状态机管理**：严格的状态流转控制，防止非法状态变更

### 2. 库存管理
- **预锁定机制**：下单时锁定库存，支付后扣减
- **预售库存分离**：普通库存和预售库存独立管理
- **并发控制**：基于Redis Lua脚本 + 数据库乐观锁，防止超卖
- **阶梯库存**：支持不同数量区间的不同价格

### 3. 分布式事务
- **可靠消息**：基于RabbitMQ实现最终一致性
- **重试机制**：失败自动重试（最多3次）
- **死信队列**：超过重试次数进入死信队列
- **幂等性**：支持幂等键防止重复处理

### 4. 可观测性
- **统一日志**：JSON格式，支持链路追踪
- **异常体系**：10+业务异常类型，统一错误码
- **性能监控**：接口响应时间记录

## 项目结构

```
.
├── config/                 # 配置模块
│   ├── __init__.py
│   └── settings.py        # 应用配置
├── controllers/           # 控制器层（API）
│   ├── __init__.py
│   └── order_controller.py
├── models/                # 数据模型层
│   ├── __init__.py
│   ├── base.py           # 数据库基础配置
│   ├── order.py          # 订单模型
│   └── inventory.py      # 库存模型
├── services/              # 服务层
│   ├── __init__.py
│   ├── order_service.py  # 订单服务（解耦为4个子服务）
│   └── inventory_service.py  # 库存服务
├── mq/                    # 消息队列
│   ├── __init__.py
│   └── consumers/
│       ├── __init__.py
│       └── inventory_consumer.py  # 库存消息消费者
├── utils/                 # 工具模块
│   ├── __init__.py
│   ├── exceptions.py     # 业务异常定义
│   └── loggers.py        # 日志配置
├── tests/                 # 测试模块
│   ├── __init__.py
│   └── test_order_presale.py  # 完整测试用例
├── main.py               # 应用入口
├── requirements.txt      # 依赖清单
└── .env.example         # 环境变量示例
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

```bash
cp .env.example .env
# 编辑 .env 文件，配置数据库、Redis、RabbitMQ等
```

### 3. 启动服务

```bash
# 开发模式
python main.py

# 或使用 uvicorn
uvicorn main:app --reload
```

### 4. 运行测试

```bash
pytest tests/test_order_presale.py -v
```

## API 文档

### 订单相关接口

#### 创建普通订单
```http
POST /api/v1/orders/normal
Content-Type: application/json

{
    "user_id": "user_123",
    "items": [
        {
            "sku_id": "sku_001",
            "quantity": 2,
            "price": 199.99
        }
    ],
    "shipping_address": {
        "name": "张三",
        "phone": "13800138000",
        "province": "广东省",
        "city": "深圳市",
        "district": "南山区",
        "address": "科技园南路88号"
    }
}
```

#### 创建预售订单
```http
POST /api/v1/orders/presale
Content-Type: application/json

{
    "user_id": "user_123",
    "items": [
        {
            "sku_id": "sku_002",
            "quantity": 1,
            "price": 1000.00
        }
    ],
    "shipping_address": {...},
    "presale_config": {
        "deposit_amount": 100.00,
        "final_payment_amount": 900.00,
        "presale_start_time": "2024-01-01T00:00:00",
        "presale_end_time": "2024-01-07T23:59:59",
        "final_payment_start_time": "2024-01-08T00:00:00",
        "final_payment_end_time": "2024-01-14T23:59:59"
    }
}
```

#### 支付订单
```http
POST /api/v1/orders/{order_id}/pay
Content-Type: application/json

{
    "payment_amount": 999.99,
    "payment_method": "wechat",
    "payment_no": "WX202401011200001"
}
```

#### 支付预售尾款
```http
POST /api/v1/orders/{order_id}/pay-final
Content-Type: application/json

{
    "payment_amount": 900.00,
    "payment_method": "alipay",
    "payment_no": "AL202401081200001"
}
```

#### 取消订单
```http
POST /api/v1/orders/{order_id}/cancel
Content-Type: application/json

{
    "reason": "用户主动取消"
}
```

## 核心设计

### 1. 订单状态机

```
普通订单：
PENDING_PAYMENT → PAID → PROCESSING → SHIPPED → DELIVERED → COMPLETED
       ↓
   CANCELLED

预售订单：
PRESALE_DEPOSIT_PENDING → PRESALE_DEPOSIT_PAID → PRESALE_FINAL_PAYMENT_PENDING → PRESALE_FINAL_PAYMENT_PAID → PROCESSING → ...
         ↓                        ↓                           ↓
     CANCELLED                CANCELLED                   CANCELLED
```

### 2. 库存锁定流程

```
1. 用户下单
   ↓
2. Redis预锁定（原子操作）
   ↓
3. 数据库乐观锁锁定
   ↓
4. 创建锁定记录
   ↓
5. 用户支付
   ↓
6. 扣减库存（锁定→已售）
   ↓
7. 触发发货
```

### 3. 并发控制

- **Redis层**：使用Lua脚本保证原子性
- **数据库层**：使用版本号乐观锁
- **消息层**：幂等键防止重复消费

## 测试覆盖

### 单元测试
- 订单模型测试
- 库存模型测试
- 状态机测试

### 集成测试
- 普通订单完整流程
- 预售订单完整流程
- 库存锁定/解锁/扣减

### 异常测试
- 库存不足
- 状态非法
- 金额不匹配
- 时间校验

### 并发测试
- 100并发库存锁定
- 200并发预售订单

### 边界测试
- 取消后重新下单
- 幂等性验证
- 预售时间校验

## 性能指标

- 接口响应时间：< 200ms
- 并发支持：1000 QPS
- 库存锁定：支持高并发，无超卖

## 部署建议

### 1. 数据库
- 使用PostgreSQL 14+
- 配置连接池（建议20-50连接）
- 启用读写分离（可选）

### 2. Redis
- 使用Redis 6+
- 配置持久化（AOF）
- 启用集群模式（可选）

### 3. RabbitMQ
- 使用RabbitMQ 3.9+
- 配置镜像队列
- 启用消息持久化

### 4. 应用
- 使用Gunicorn + Uvicorn
- 配置4-8工作进程
- 启用健康检查端点

## 监控告警

- **业务指标**：订单量、支付成功率、库存异常
- **性能指标**：接口延迟、数据库慢查询
- **错误告警**：异常率、死信队列积压

## 版本历史

### v2.0.0 (2024-01)
- 重构订单核心链路
- 新增预售订单完整特性
- 实现分布式事务
- 统一日志和异常体系

## 贡献指南

1. Fork 项目
2. 创建特性分支
3. 提交变更
4. 确保测试通过
5. 提交 Pull Request

## 许可证

MIT License
