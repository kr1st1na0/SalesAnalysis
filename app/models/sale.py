from pydantic import BaseModel

class NewSale(BaseModel):
    customer_id: int
    seller_id: int
    product_id: int
    quantity: int
    amount: float
    discount: float