# SalesAnalysis

### Сборка и запуск

Пересборка образов
```
docker-compose build data-generator data-processing
```
Права на скрипт
```
chmod +x start.sh
```
Запуск
```
./start.sh
```
### Для повторного запуска сначала останавливаем контейнер
```
docker-compose down -v
```
Или полностью все очищаем
```
docker-compose down --volumes --remove-orphans \
  && docker-compose build --no-cache
```

### MongoDB (товары)
```
docker exec -it mongodb mongosh -u admin -p admin --authenticationDatabase admin sales
```

```
show collections

db.products.find().limit(5).pretty()

db.products.countDocuments()
```

### PostgreSQL(продавцы и клиенты)
```
docker exec -it postgres_people psql -U admin -d people
```

### PostgreSQL (стейдж слой)
```
docker exec -it postgres_stage psql -U admin -d stage_layer
```

### ClickHouse
```
docker exec -it clickhouse clickhouse-client
SELECT * FROM sales_stage LIMIT 5;
```

### API

Запуск (из SalesAnalysis)
```
uvicorn app.main:app --reload
```

[Swagger](http://127.0.0.1:8000/docs)
_________________
### Пересборка докер файла:

```docker-compose build --no-cache```


### Полная пересборка с удалением всех образов:

```docker-compose down --volumes --remove-orphans \
  && docker-compose build --no-cache \
  && docker-compose up -d
```
