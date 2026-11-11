# Учебные данные

Проект содержит воспроизводимый набор данных для практики с Kafka, ksqlDB и REST Proxy.

## Подготовка

Запустите полный стек и дождитесь готовности сервисов:

```bash
docker compose up -d
docker compose ps
```

Затем выполните:

```bash
./scripts/reset-lab.sh
```

Скрипт:

1. Останавливает активные persistent queries ksqlDB.
2. Удаляет пользовательские объекты ksqlDB и Kafka topics.
3. Создаёт три исходных topic с двумя партициями.
4. Загружает данные через Kafka REST Proxy.

Будут загружены:

1. 12 товаров в `Items`.
2. 81 событие заказов в `OrderChanges`.
3. 32 события изменения остатков в `StockChanges`.

Все `eventTs` заданы в Unix epoch milliseconds UTC и соответствуют полю `updatedAt`.

> `reset-lab.sh` удаляет пользовательские Kafka topics и объекты ksqlDB в локальном стенде. Не используйте его с внешними или рабочими кластерами.

## Проверка

```bash
docker compose exec -T kafka \
  kafka-topics --bootstrap-server kafka:29092 --list
```

До выполнения SQL-запросов должны существовать три пользовательских topic:

1. `Items`
2. `OrderChanges`
3. `StockChanges`

Дальнейшие упражнения описаны в [KSQLDB_AND_REST_PROXY_GUIDE.md](KSQLDB_AND_REST_PROXY_GUIDE.md).
