from app.core.application import app
from app.config.database import startup_event, shutdown_event
from app.routes import sales, products, customers, sellers

app.add_event_handler("startup", startup_event)
app.add_event_handler("shutdown", shutdown_event)

app.include_router(sales.router, prefix="/api", tags=["Sales"])
app.include_router(products.router, prefix="/api", tags=["Products"])
app.include_router(customers.router, prefix="/api", tags=["Customers"])
app.include_router(sellers.router, prefix="/api", tags=["Sellers"])
