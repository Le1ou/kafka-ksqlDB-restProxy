# Практическое руководство по ksqlDB и Kafka REST Proxy

Руководство последовательно знакомит с объектами и запросами ksqlDB, а затем с административным, producer и consumer API Kafka REST Proxy.

Перед началом запустите стек и подготовьте данные по инструкции [LAB_DATA.md](LAB_DATA.md).

1. **Состав локального стенда**

Вся система развернута локально через Docker Compose.

**Проверка**

- Выполните `docker compose ps`.
- Убедитесь, что все контейнеры запущены.

**Ключевые моменты**

- `Kafka` — брокер, который хранит сообщения в topics и распределяет их по партициям.
- `ZooKeeper` — хранит служебные метаданные Kafka в используемой версии стенда.
- `Schema Registry` — хранит схемы Avro, Protobuf и JSON Schema. В примерах используется обычный JSON без регистрации схем.
- `ksqlDB Server` — выполняет потоковые SQL-запросы поверх Kafka topics.
- `Kafka REST Proxy` — предоставляет HTTP API для административных операций, публикации и чтения сообщений.
- `Conduktor Console` — интерфейс для просмотра topics, сообщений, consumer groups и объектов ksqlDB.
- `PostgreSQL` — внутренняя база Conduktor; сообщения Kafka в ней не хранятся.

Kafka является центральным хранилищем событий. ksqlDB читает исходные topics, обрабатывает события и записывает результаты в новые topics. REST Proxy преобразует HTTP-запросы в операции Kafka Producer и Kafka Consumer. Conduktor используется для наблюдения и управления.

2. **Создание Source Objects**

Создайте объекты ksqlDB поверх уже существующих Kafka topics.

**Семантика Stream и Table**

- Stream — неизменяемая последовательность событий. Каждая новая запись означает новый факт, а не замену предыдущего.
- Table — текущее состояние по ключу. Новая запись с тем же ключом заменяет предыдущее значение, а `NULL` может означать удаление строки.
- Один Kafka topic можно интерпретировать как Stream или Table в зависимости от смысла данных.
- Stream подходит для заказов, платежей и изменений остатков.
- Table подходит для справочников и текущего состояния сущностей.

**Как ksqlDB обрабатывает SQL**

- Клиент отправляет SQL-запрос в ksqlDB Server.
- Лексер разбивает текст на токены: ключевые слова, имена объектов, поля, операторы и литералы.
- Парсер проверяет грамматику и строит синтаксическое дерево запроса.
- Семантический анализ проверяет существование Stream и Table, доступность полей, типы данных, ключи и совместимость операций.
- Планировщик строит логический, а затем физический план выполнения.
- Для потокового запроса ksqlDB создаёт topology на базе Kafka Streams.
- Topology читает входные topics, выполняет фильтрацию, преобразования, Join или агрегацию и при необходимости записывает результат в output topic.
- Для stateful-операций ksqlDB использует локальные state stores и внутренние changelog/repartition topics Kafka.

**Создать Stream**

```sql
CREATE STREAM ORDER_CHANGES (
    orderId STRING KEY,
    storeId INTEGER,
    itemId STRING,
    counterpartyId STRING,
    status STRING,
    amount DOUBLE,
    eventTs BIGINT,
    updatedAt STRING
) WITH (
    KAFKA_TOPIC='OrderChanges',
    KEY_FORMAT='JSON',
    VALUE_FORMAT='JSON',
    TIMESTAMP='eventTs'
);
```

**Создать Table**

```sql
CREATE TABLE ITEMS (
    itemId STRING PRIMARY KEY,
    itemName STRING,
    category STRING,
    price INTEGER,
    isActive BOOLEAN
) WITH (
    KAFKA_TOPIC='Items',
    KEY_FORMAT='JSON',
    VALUE_FORMAT='JSON'
);
```

**Проверка**

- Откройте список объектов ksqlDB.
- Проверьте созданные Stream и Table.
- Проверьте связанные с ними Kafka topics.

**Ключевые моменты**

- Stream и Table — логические объекты ksqlDB поверх существующих Kafka topics.
- Stream представляет поток событий.
- Table представляет текущее состояние по ключу.
- На этом шаге новые Kafka topics не создаются.

3. **Transient Push Query**

**Типы запросов**

- Pull Query читает текущее материализованное состояние Table и сразу возвращает конечный результат.
- Push Query остаётся запущенным и выдаёт новые результаты по мере поступления событий.
- Transient Push Query существует только пока открыт и выполняется запрос клиента. Он не создаёт постоянный output topic.
- Persistent Push Query создаётся через `CREATE ... AS SELECT`, продолжает работать независимо от открытого окна клиента и записывает результат в Kafka topic.
- `SELECT ... EMIT CHANGES` без `CREATE` — Transient Push Query.
- CTAS и CSAS с `EMIT CHANGES` — Persistent Push Queries.

```sql
SELECT itemName, price
FROM ITEMS
EMIT CHANGES;
```

**Порядок выполнения**

- Запустите запрос.
- Отправить новое сообщение в `Items`.
- Проверьте немедленное появление новой строки.
- Остановите запрос и убедитесь, что результат больше не обновляется.

**Ключевые моменты**

- Это временный Push Query.
- `EMIT CHANGES` означает непрерывную выдачу изменений.
- Persistent Query и новый Kafka topic не создаются.

4. **CTAS: Table → Table**

```sql
SET 'auto.offset.reset'='earliest';

CREATE TABLE ITEMS_UNDER_200
WITH (
    KAFKA_TOPIC='ItemsUnder200',
    PARTITIONS=2
) AS
SELECT *
FROM ITEMS
WHERE price < 200
EMIT CHANGES;
```

**Результат и ключевые моменты**

- Создались Persistent Query, новая Table и новый Kafka topic.
- В результат попали только товары дешевле 200.
- `earliest` позволяет обработать уже существующие сообщения.
- Исходная Table не изменяется.
- Новые подходящие записи автоматически попадают в результат.

5. **CSAS: Stream → Stream**

```sql
SET 'auto.offset.reset'='earliest';

CREATE STREAM ORDER_CHANGES_STORE_103
WITH (
    KAFKA_TOPIC='OrderChangesStore103',
    PARTITIONS=2
) AS
SELECT orderId, storeId, itemId, counterpartyId, status, amount, eventTs, updatedAt
FROM ORDER_CHANGES
WHERE storeId = 103
EMIT CHANGES;
```

**Результат и ключевые моменты**

- Создались Persistent Query, новый Stream и новый Kafka topic.
- В новый topic попадают только события магазина 103.
- Фильтрация выполняется постоянно.
- Исходный Stream продолжает существовать без изменений.
- Во время REST-части новый заказ магазина 103 появится и в исходном, и в отфильтрованном topic.

6. **Repartition**

```sql
SET 'auto.offset.reset'='earliest';

CREATE STREAM ORDERS_BY_STORE_WITH_VAT
WITH (
    KAFKA_TOPIC='OrdersByStoreWithVat',
    PARTITIONS=2
) AS
SELECT
    CAST(storeId AS STRING) AS storeId,
    orderId,
    itemId,
    counterpartyId,
    amount * 1.2 AS amount_with_vat
FROM ORDER_CHANGES
PARTITION BY CAST(storeId AS STRING)
EMIT CHANGES;
```

**Результат и ключевые моменты**

- Kafka key изменился с `orderId` на `storeId`.
- В Value появилось вычисленное поле `amount_with_vat`.
- После изменения ключа сообщения заново распределяются по партициям.
- Сообщения с одинаковым `storeId` попадают в одну партицию.
- `PARTITION BY` меняет Kafka key, а не только поле в Value.

7. **Aggregation: Stream → Table**

```sql
SET 'auto.offset.reset'='earliest';

CREATE TABLE STORE_AMOUNT
WITH (
    KAFKA_TOPIC='StoreAmount',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='JSON',
    PARTITIONS=2
) AS
SELECT
    storeId,
    COUNT(*) AS orderCount,
    SUM(amount) AS totalAmount
FROM ORDER_CHANGES
GROUP BY storeId
EMIT CHANGES;
```

**Результат и ключевые моменты**

- Создались Persistent Query, Table и topic `StoreAmount`.
- После новых заказов меняются `orderCount` и `totalAmount`.
- Результат является текущим состоянием по каждому `storeId`, поэтому создаётся Table.
- ksqlDB хранит состояние агрегации.

**Pull Query**

```sql
SELECT *
FROM STORE_AMOUNT
WHERE storeId = 103;
```

Pull Query сразу возвращает текущее материализованное состояние и не использует `EMIT CHANGES`.

8. **Window Aggregation: Stream → Table**

**Создать Stream**

```sql
CREATE STREAM STOCK_CHANGES (
    storeItemId STRING KEY,
    storeId INTEGER,
    itemId STRING,
    quantity INTEGER,
    operation STRING,
    eventTs BIGINT,
    updatedAt STRING
) WITH (
    KAFKA_TOPIC='StockChanges',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='JSON',
    TIMESTAMP='eventTs'
);
```

`eventTs` используется как Event Time. Окна строятся по времени события, а не по времени обработки.

**Создать Windowed Table**

```sql
SET 'auto.offset.reset'='earliest';

CREATE TABLE STOCK_ITEM_5MIN
WITH (
    KAFKA_TOPIC='StockItem5min',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='JSON',
    PARTITIONS=2
) AS
SELECT
    storeItemId,
    FORMAT_TIMESTAMP(
        FROM_UNIXTIME(WINDOWSTART),
        'yyyy-MM-dd HH:mm:ss.SSSX',
        'UTC'
    ) AS startWindow,
    FORMAT_TIMESTAMP(
        FROM_UNIXTIME(WINDOWEND),
        'yyyy-MM-dd HH:mm:ss.SSSX',
        'UTC'
    ) AS endWindow,
    SUM(quantity) AS totalQuantity
FROM STOCK_CHANGES
WINDOW TUMBLING (
    SIZE 5 MINUTES,
    GRACE PERIOD 2 MINUTES
)
WHERE operation = 'UPSERT'
GROUP BY storeItemId
EMIT CHANGES;
```

**Результат и ключевые моменты**

- Создались Persistent Query, Windowed Table и новый topic.
- Tumbling Window делит время на непересекающиеся интервалы.
- Для каждого ключа и окна хранится отдельный результат.
- `startWindow` и `endWindow` попадают в Value итогового topic в читаемом виде.
- `FROM_UNIXTIME` преобразует границы окна из Unix epoch milliseconds.
- `FORMAT_TIMESTAMP` с timezone `UTC` выводит время явно в UTC с суффиксом `Z`.
- Grace Period позволяет принять немного опоздавшие события.
- Windowed key содержит исходный ключ и бинарную информацию об окне.
- `EMIT FINAL` можно использовать для получения финального результата после закрытия окна.

9. **Stream–Table Join**

**Сначала repartition по ключу соединения**

```sql
SET 'auto.offset.reset'='earliest';

CREATE STREAM ORDER_CHANGES_BY_ITEM
WITH (
    KAFKA_TOPIC='OrderChangesByItem',
    KEY_FORMAT='JSON',
    VALUE_FORMAT='JSON',
    PARTITIONS=2
) AS
SELECT *
FROM ORDER_CHANGES
PARTITION BY itemId
EMIT CHANGES;
```

**Затем Join**

```sql
SET 'auto.offset.reset'='earliest';

CREATE STREAM ORDER_CHANGES_WITH_ITEM
WITH (
    KAFKA_TOPIC='OrderChangesWithItem',
    VALUE_FORMAT='JSON',
    PARTITIONS=2
) AS
SELECT
    o.itemId,
    o.orderId,
    o.storeId,
    o.counterpartyId,
    o.status,
    o.amount,
    i.itemName,
    i.category,
    i.price,
    i.isActive
FROM ORDER_CHANGES_BY_ITEM o
LEFT JOIN ITEMS i
    ON o.itemId = i.itemId
EMIT CHANGES;
```

**Результат и ключевые моменты**

- Первый запрос меняет Kafka key с `orderId` на `itemId`.
- Второй запрос обогащает заказы текущими данными справочника товаров.
- `LEFT JOIN` сохраняет событие, даже если товар не найден.
- Для подготовленного `ITEM-999` поля Table будут равны `NULL`.
- Table должна быть заполнена до обработки события Stream.

10. **REST Proxy**

REST Proxy предоставляет HTTP-интерфейс для работы с Kafka. Ниже рассматриваются административные запросы v3 и работа с сообщениями через v2.

**Как это работает архитектурно**

- При публикации REST Proxy принимает HTTP-запрос, сериализует `key` и `value`, создаёт Producer Record и отправляет его Kafka broker.
- REST Proxy не хранит сообщения: после записи они находятся в Kafka topic.
- При создании consumer REST Proxy создаёт и хранит у себя экземпляр Kafka Consumer.
- Subscription задаёт topics, а запрос `/records` вызывает `poll()` Kafka Consumer.
- Consumer Group и committed offsets хранятся в Kafka; Consumer Instance является временным объектом REST Proxy.
- Удаление Consumer Instance закрывает consumer, но не удаляет сообщения или committed offsets.

**Версии API**

- v3 используется для clusters, topics, brokers, consumer groups и configs; версия находится в URL.
- v2 используется для produce и consumer API; версия и формат находятся в HTTP media type.

11. **Получить список Kafka Clusters**

```http
GET {{baseUrl}}/v3/clusters
```

Ответ содержит `cluster_id` и ссылки на topics, brokers и consumer groups. REST Proxy возвращает только кластеры, к которым он подключён.

12. **Получить список Kafka Topics**

```http
GET {{baseUrl}}/v3/clusters/{{clusterId}}/topics
```

Ответ содержит `topic_name`, `partitions_count`, `replication_factor`, `is_internal` и ссылки. Исходные и созданные ksqlDB topics для REST Proxy являются обычными Kafka topics.

13. **Получить информацию о конкретном Topic**

```http
GET {{baseUrl}}/v3/clusters/{{clusterId}}/topics/OrderChanges
```

Ответ содержит имя topic, две партиции, replication factor и ссылки на partitions и configs.

14. **Опубликовать сообщения через REST Proxy**

```http
POST {{baseUrl}}/topics/OrderChanges
Content-Type: application/vnd.kafka.json.v2+json
Accept: application/vnd.kafka.v2+json
```

```json
{
  "records": [
    {
      "key": "ORDER-EXAMPLE-103",
      "value": {
        "orderId": "ORDER-EXAMPLE-103",
        "storeId": 103,
        "itemId": "ITEM-003",
        "counterpartyId": "C001",
        "status": "NEW",
        "amount": 1500.0,
        "eventTs": 1784100600000,
        "updatedAt": "2026-07-15T07:30:00.000Z"
      }
    },
    {
      "key": "ORDER-EXAMPLE-104",
      "value": {
        "orderId": "ORDER-EXAMPLE-104",
        "storeId": 104,
        "itemId": "ITEM-006",
        "counterpartyId": "C002",
        "status": "PAID",
        "amount": 320.5,
        "eventTs": 1784100660000,
        "updatedAt": "2026-07-15T07:31:00.000Z"
      }
    }
  ]
}
```

**Результат и ключевые моменты**

- Версия v2 находится в `Content-Type` и `Accept`, а не в URL.
- `key` и `value` передаются отдельно.
- Массив `records` позволяет отправить несколько сообщений одним запросом.
- Kafka Producer выбирает партицию по ключу.
- Заказ магазина 103 появляется в исходном topic и `OrderChangesStore103`.
- Заказ магазина 104 появляется только в исходном topic.

15. **Разобрать Produce Response**

```json
{
  "offsets": [
    {
      "partition": 1,
      "offset": 41,
      "error_code": null,
      "error": null
    }
  ],
  "key_schema_id": null,
  "value_schema_id": null
}
```

Ответ содержит фактические `partition`, `offset`, `error_code` и `error`. Offset уникален только внутри партиции. Schema IDs равны `null`, потому что используется JSON без Schema Registry.

16. **Создать Consumer**

```http
POST {{baseUrl}}/consumers/{{consumerGroup}}
Content-Type: application/vnd.kafka.v2+json
Accept: application/vnd.kafka.v2+json
```

```json
{
  "name": "{{consumerInstance}}",
  "format": "json",
  "auto.offset.reset": "earliest",
  "auto.commit.enable": "false"
}
```

REST Proxy создаёт сессионный Kafka Consumer. Ответ содержит `instance_id` и `base_uri`. До подписки и первого чтения он не получает сообщения.

17. **Подписаться на Topic**

```http
POST {{baseUrl}}/consumers/{{consumerGroup}}/instances/{{consumerInstance}}/subscription
Content-Type: application/vnd.kafka.v2+json
Accept: application/vnd.kafka.v2+json
```

```json
{
  "topics": [
    "OrderChanges"
  ]
}
```

Ответ содержит `204 No Content`. Kafka распределяет партиции между consumers одной группы.

18. **Прочитать сообщения**

```http
GET {{baseUrl}}/consumers/{{consumerGroup}}/instances/{{consumerInstance}}/records
Accept: application/vnd.kafka.json.v2+json
```

Первый запрос запускает `poll()`: consumer вступает в Consumer Group и получает партиции. Ответ содержит topic, key, value, partition и offset.

19. **Commit Offset**

```http
POST {{baseUrl}}/consumers/{{consumerGroup}}/instances/{{consumerInstance}}/offsets
Content-Type: application/vnd.kafka.v2+json
Accept: application/vnd.kafka.v2+json
```

```json
{}
```

Пустой JSON-объект фиксирует текущие позиции всех назначенных партиций. Commit не меняет сообщения, а сохраняет позицию Consumer Group в Kafka.

20. **Удалить Consumer**

```http
DELETE {{baseUrl}}/consumers/{{consumerGroup}}/instances/{{consumerInstance}}
```

Ответ содержит `204 No Content`. REST Proxy закрывает Consumer Instance и освобождает ресурсы. Committed offsets сохраняются в Kafka. Повторное обращение к instance вернёт `40403 Consumer instance not found`.
