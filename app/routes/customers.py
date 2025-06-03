from fastapi import APIRouter, Depends, HTTPException
from app.models.customer import NewCustomer
from app.config.database import get_pg_connection
from datetime import datetime

router = APIRouter()

@router.get("/customers", tags=["Customers"])
async def get_customers(limit: int = 10):
    conn = await get_pg_connection()
    try:
        customers = await conn.fetch(
            "SELECT * FROM customers ORDER BY registration_date DESC LIMIT $1",
            limit
        )
        return {"status": "success", "customers": customers}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/customers", tags=["Customers"], status_code=201)
async def add_customer(customer: NewCustomer):
    conn = await get_pg_connection()
    try:
        await conn.execute(
            """
            INSERT INTO customers 
            (first_name, last_name, email, phone, registration_date, loyalty_level)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            customer.first_name,
            customer.last_name,
            customer.email,
            customer.phone,
            datetime.now(),
            customer.loyalty_level
        )
        return {"status": "success", "message": "customer added"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
