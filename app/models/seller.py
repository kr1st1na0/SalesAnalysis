from pydantic import BaseModel

class NewSeller(BaseModel):
    first_name: str
    last_name: str
    email: str
    phone: str
    department: str