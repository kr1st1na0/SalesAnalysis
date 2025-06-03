from fastapi import APIRouter, HTTPException
from app.models.product import NewProduct
from app.config.database import get_mongo_client
from datetime import datetime

router = APIRouter()

@router.get("/products", tags=["Products"])
async def get_products(limit: int = 10):
    mongo_client = await get_mongo_client()
    try:
        products = []
        async for product in mongo_client.sales.products.find().limit(limit):
            product["_id"] = str(product["_id"])
            products.append(product)
        return {"status": "success", "products": products}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/products", tags=["Products"], status_code=201)
async def add_product(product: NewProduct):
    mongo_client = await get_mongo_client()
    try:
        result = await mongo_client.sales.products.insert_one({
            "product_num_id": product.product_num_id,
            "name": product.name,
            "category": product.category,
            "price": product.price,
            "cost": product.cost,
            "stock_quantity": product.stock_quantity,
            "manufacturer": product.manufacturer,
            "created_at": datetime.now()
        })
        return {
            "status": "success",
            "message": "product added",
            "product_id": str(result.inserted_id)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))