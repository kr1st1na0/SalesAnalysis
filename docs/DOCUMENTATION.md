# SalesAnalysis

## Структура проекта

Сервис генерации данных - ```data```:
  - генерация клиентов и покупателей в PostgreSQL - ```people```
  - генерация товаров в MongoDB - ```products```
  - генерация продаж в Kafka - ```sales```

Stage layer - ```pipeline/stage layer```

Сервис обработки данных - ```pipeline/processing```:
  - Потоковая обработка - ```streaming```
  - Батчевая обработка - ```batch```

Создание витрин в ClickHouse - ```pipeline/processing/clickhouse_views```

Визуализация данных - ```grafana```

Веб-приложение для добавления новых данных - ```app```

Скрипт запуска сервисов - ```start.sh```
