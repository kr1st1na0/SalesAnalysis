# SalesAnalysis


Пересборка докер файла:

```docker-compose build --no-cache```


Полная пересборка с удалением всех образов:

```docker-compose down --volumes --remove-orphans \
  && docker-compose build --no-cache \
  && docker-compose up -d
```


MongoDB
```
docker exec -it salesanalysis-mongodb-1 mongosh "mongodb://admin:admin@mongodb:27017"
use sales
show collections
db.products.find({}, { name: 1, price: 1 }).limit(5).pretty()
```

PostgreSQL(изначальная)
```
docker exec -it postgres_people psql -U admin -d people
```

PostgreSQL (стейдж слой)
```
docker exec -it postgres_stage psql -U admin -d stage_layer
```

ClickHouse
```
docker exec -it clickhouse clickhouse-client
SELECT * FROM sales_stage LIMIT 5;
```

## Сборка

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
Для повторного запуска сначала останавливаем контейнер
```
docker-compose down -v
```
Или полностью все очищаем
```
docker-compose down --volumes --remove-orphans \
  && docker-compose build --no-cache
```
