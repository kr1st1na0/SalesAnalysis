FROM python:3.9-slim

WORKDIR /app

COPY requirements_fastapi.txt .
RUN pip install --no-cache-dir -r requirements_fastapi.txt

COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]