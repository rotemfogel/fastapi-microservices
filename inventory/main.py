import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis_om import get_redis_connection, HashModel

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_headers=['*'],
    allow_methods=['*'],
)

redis = get_redis_connection(host='127.0.0.1',
                             port=6379,
                             password='',
                             decode_responses=True)


class Product(HashModel):
    name: str
    price: float
    quantity: int

    class Meta:
        database = redis


@app.get('/products')
def all_products():
    return [get(pk) for pk in Product.all_pks()]


@app.get('/products/{pk}')
def get(pk: str):
    return Product.get(pk)


@app.post('/products')
def create(product: Product):
    return product.save()


@app.put('/products/{pk}')
def update(pk: str, new_product: Product):
    product = get(pk)
    return product.update(name=new_product.name,
                          price=new_product.price,
                          quantity=new_product.quantity)


@app.delete('/products/{pk}')
def delete(pk: str):
    return Product.delete(pk)


if __name__ == "__main__":
    uvicorn.run("main:app",
                host="127.0.0.1",
                port=8000,
                log_level="info",
                reload=True)
