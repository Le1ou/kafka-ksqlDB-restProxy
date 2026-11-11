# Практическое руководство по ksqlDB

Руководство последовательно знакомит с объектами и запросами ksqlDB: Stream, Table, push- и pull-запросами, преобразованиями, агрегациями, окнами и Join.

Перед началом запустите стек. Исходные topics и тестовые сообщения можно создать самостоятельно через Conduktor Console, Kafka CLI или Kafka REST Proxy.

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
- Если в Table нет записи для указанного `itemId`, её поля в результате будут равны `NULL`.
- Table должна быть заполнена до обработки события Stream.
