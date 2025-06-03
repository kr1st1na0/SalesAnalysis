from pydantic import BaseModel

class NewProduct(BaseModel):
    product_num_id: int
    name: str
    category: str
    price: float
    cost: float
    stock_quantity: int
    manufacturer: str