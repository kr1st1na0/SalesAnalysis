from fastapi import APIRouter, Depends, HTTPException
from app.models.sale import NewSale
from app.config.database import get_kafka_producer
from datetime import datetime

router = APIRouter()

@router.post("/sales", tags=["Sales"], status_code=201)
async def add_sale(sale: NewSale):
    kafka_producer = await get_kafka_producer()
    try:
        sale_data = {
            'customer_id': sale.customer_id,
            'seller_id': sale.seller_id,
            'product_id': sale.product_id,
            'quantity': sale.quantity,
            'sale_date': datetime.now().isoformat(),
            'amount': sale.amount,
            'discount': sale.discount
        }

        await kafka_producer.send_and_wait(
            topic='sales',
            value=sale_data
        )
        return {"status": "success", "message": "sale added", "data": sale_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))