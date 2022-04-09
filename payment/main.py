import time

import requests
import uvicorn
from fastapi import FastAPI
from fastapi.background import BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from redis_om import get_redis_connection, HashModel
from starlette.requests import Request

__ORDER_CANCELLED = 'order_cancelled'
__ORDER_COMPLETED = 'order_completed'

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_headers=['*'],
    allow_methods=['*'],
)

# this service should have its own database
redis = get_redis_connection(host='127.0.0.1',
                             port=6379,
                             password='',
                             decode_responses=True)


class OrderStatus(object):
    PENDING = 'pending'
    CANCELED = 'canceled'
    COMPLETED = 'completed'

    _ALL_STATUSES = set()  # type: Set[str]

    @classmethod
    def is_valid(cls, status):
        return status in cls.all_types()

    @classmethod
    def all_types(cls):
        if not cls._ALL_STATUSES:
            cls._ALL_STATUSES = {
                getattr(cls, attr)
                for attr in dir(cls)
                if not attr.startswith("_") and not callable(getattr(cls, attr))
            }
        return cls._ALL_STATUSES


class Order(HashModel):
    product_id: str
    price: float
    fee: float
    total: float
    quantity: int
    status: str

    class Meta:
        database = redis


@app.get('/orders')
def all_orders():
    return [get(pk) for pk in Order.all_pks()]


@app.get('/orders/{pk}')
def get(pk: str):
    return Order.get(pk)


@app.post('/orders')
async def create(request: Request, background_tasks: BackgroundTasks):
    """
    get product with id and quantity
    :param request: the request
    :param background_tasks:
    :return: 201 CREATED
    """
    body = await request.json()
    product_id = body['product_id']
    quantity = body['quantity']

    # send req to inventory service
    req = requests.get(f'http://localhost:8000/products/{product_id}')
    product = req.json()
    price = product['price']

    order = Order(
        product_id=product_id,
        price=price,
        fee=0.2 * price,
        total=1.2 * price,
        quantity=quantity,
        status=OrderStatus.PENDING,
    )
    order.save()
    # background task
    background_tasks.add_task(order_completed, order)
    return order


@app.put('/orders/{pk}')
def update(pk: str, new_order: Order):
    order = get(pk)
    return order.update(product_id=new_order.product_id,
                        price=new_order.price,
                        fee=new_order.fee,
                        total=new_order.total,
                        quantity=new_order.quantity,
                        status=new_order.status)


@app.delete('/orders/{pk}')
def delete(pk: str):
    return Order.delete(pk)


def order_completed(order: Order):
    # emulate an order processing task
    time.sleep(5)
    req = requests.get(f'http://localhost:8000/products/{order.product_id}')
    should_cancel = False
    if req.status_code == 204:
        should_cancel = True
    if not should_cancel and req.status_code == 200:
        product = req.json()
        if product['quantity'] - order.quantity < 0:
            should_cancel = True
    if should_cancel:
        redis.xadd(__ORDER_CANCELLED, order.dict(), '*')
    else:
        order.status = OrderStatus.COMPLETED
        order.save()
        # send redis stream command
        # * means auto-generated id
        redis.xadd(__ORDER_COMPLETED, order.dict(), '*')


if __name__ == "__main__":
    uvicorn.run("main:app",
                host="127.0.0.1",
                port=8001,
                log_level="info",
                reload=True)
