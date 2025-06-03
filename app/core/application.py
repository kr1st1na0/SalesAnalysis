from fastapi import FastAPI

app = FastAPI(
    title="Data Addition Service",
    description="API for adding new data to various data stores",
    version="1.0.0"
)