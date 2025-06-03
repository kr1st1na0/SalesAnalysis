from fastapi import APIRouter, Depends, HTTPException
from app.models.seller import NewSeller
from app.config.database import get_pg_connection
from datetime import datetime

router = APIRouter()

@router.get("/sellers", tags=["Sellers"])
async def get_sellers(limit: int = 10):
    conn = await get_pg_connection()
    try:
        sellers = await conn.fetch(
            "SELECT * FROM sellers ORDER BY hire_date DESC LIMIT $1",
            limit
        )
        return {"status": "success", "sellers": sellers}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sellers", tags=["Sellers"], status_code=201)
async def add_seller(seller: NewSeller):
    conn = await get_pg_connection()
    try:
        await conn.execute(
            """
            INSERT INTO sellers 
            (first_name, last_name, email, phone, hire_date, department)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            seller.first_name,
            seller.last_name,
            seller.email,
            seller.phone,
            datetime.now(),
            seller.department
        )
        return {"status": "success", "message": "seller added"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))