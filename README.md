# SalesAnalysis


Пересборка докер файла:

```docker-compose build --no-cache```


Полная пересборка с удалением всех образов:

```docker-compose down --volumes --remove-orphans \
  && docker-compose build --no-cache \
  && docker-compose up --force-recreate
```


MongoDB
```
docker exec -it salesanalysis-mongodb-1 mongosh "mongodb://admin:admin@mongodb:27017"
use sales
show collections
db.products.find({}, { name: 1, price: 1 }).limit(5).pretty()
```

PostgreSQL
```
docker exec -it salesanalysis-postgres-1 psql -U admin -d sales
```

ClickHouse
```
docker exec -it clickhouse clickhouse-client
SELECT * FROM sales_stage LIMIT 5;
```