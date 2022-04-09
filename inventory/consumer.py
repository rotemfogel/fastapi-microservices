import time

from main import redis, Product

__ORDER_COMPLETED = 'order_completed'
__GROUP = 'inventory_group'

try:
    redis.xgroup_create(__ORDER_COMPLETED, __GROUP)
except:
    pass

while True:
    try:
        # '>' means get all events for key
        results = redis.xreadgroup(__GROUP, __ORDER_COMPLETED, {__ORDER_COMPLETED: '>'}, None)
        if results:
            for result in results:
                order = result[1][0][1]
                pk = order['product_id']
                print(f'fetching Product: {pk}')
                product: Product = Product.get(pk)
                product.quantity = product.quantity - int(order['quantity'])
                product.save()
                print(product)
    except Exception as e:
        print(str(e))
    time.sleep(1)
