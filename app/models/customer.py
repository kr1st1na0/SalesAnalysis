from pydantic import BaseModel

class NewCustomer(BaseModel):
    first_name: str
    last_name: str
    email: str
    phone: str
    loyalty_level: int