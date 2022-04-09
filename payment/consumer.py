import time

from main import redis, Order, OrderStatus

__ORDER_CANCELLED = 'order_cancelled'
__GROUP = 'payment_group'

try:
    redis.xgroup_create(__ORDER_CANCELLED, __GROUP)
except:
    pass

while True:
    try:
        # '>' means get all events for key
        results = redis.xreadgroup(__GROUP, __ORDER_CANCELLED, {__ORDER_CANCELLED: '>'}, None)
        if results:
            for result in results:
                obj = result[1][0][1]
                order: Order = Order.get(obj['pk'])
                order.status = OrderStatus.CANCELED
                order.save()
                print(order)
    except Exception as e:
        print(str(e))
    time.sleep(1)
