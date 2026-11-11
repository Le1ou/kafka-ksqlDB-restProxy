# Kafka, ksqlDBб REST Proxy, Conductor Console

Локальный Kafka-стек для изучения потоковой обработки данных и HTTP-интеграции с Kafka. Проект запускает готовое окружение с Kafka, Schema Registry, ksqlDB, Kafka REST Proxy и Conduktor Console.

Репозиторий подходит для самостоятельной практики: можно создавать topics, выполнять запросы ksqlDB, публиковать и читать сообщения через REST Proxy, работать со схемами и наблюдать за состоянием Kafka через веб-интерфейс.

> Проект предназначен для локального обучения и разработки.

## Состав

| Компонент | Назначение | Локальный адрес |
| --- | --- | --- |
| Kafka | Хранение и доставка сообщений | `localhost:9092` |
| ZooKeeper | Координация Kafka в используемой версии | `localhost:2181` |
| Schema Registry | Хранение Avro, JSON Schema и Protobuf схем | `http://localhost:8081` |
| Kafka REST Proxy | HTTP API для Kafka | `http://localhost:8082` |
| ksqlDB Server | Потоковые SQL-запросы | `http://localhost:8088` |
| Conduktor Console | Веб-интерфейс для работы с Kafka | `http://localhost:8080` |
| PostgreSQL | Служебная база Conduktor | Только внутри Docker-сети |

Все компоненты запускаются одним `docker-compose.yml`.

## Требования

- Docker Desktop или Docker Engine;
- Docker Compose v2;
- `curl`;
- Node.js для загрузки учебных данных.

## Запуск

1. Клонируйте репозиторий и перейдите в его каталог.

2. Создайте локальный файл конфигурации:

   ```bash
   cp .env.example .env
   ```

3. Замените примерные пароли в `.env`. Пароль PostgreSQL должен совпадать в `POSTGRES_PASSWORD` и `CONDUKTOR_DATABASE_URL`.

4. Запустите стек:

   ```bash
   docker compose up -d
   ```

5. Проверьте состояние контейнеров:

   ```bash
   docker compose ps
   ```
   
## Доступ к сервисам

После запуска доступны:

```text
Conduktor Console: http://localhost:8080
Schema Registry:   http://localhost:8081
Kafka REST Proxy:  http://localhost:8082
ksqlDB Server:     http://localhost:8088
Kafka:             localhost:9092
```

Для входа в Conduktor Console используйте `CONDUKTOR_ADMIN_EMAIL` и `CONDUKTOR_ADMIN_PASSWORD` из локального `.env`.

## Быстрая проверка

Проверить REST Proxy:

```bash
curl http://localhost:8082/v3/clusters
```

Проверить Schema Registry:

```bash
curl http://localhost:8081/subjects
```

Посмотреть Kafka topics:

```bash
docker compose exec -T kafka \
  kafka-topics --bootstrap-server kafka:29092 --list
```

## Учебные данные

Для практических упражнений можно загрузить подготовленный набор товаров, заказов и изменений остатков:

```bash
./scripts/reset-lab.sh
```

Скрипт пересоздаёт пользовательские topics и загружает данные через Kafka REST Proxy:

- `Items`;
- `OrderChanges`;
- `StockChanges`.

Подробное описание находится в [docs/LAB_DATA.md](docs/LAB_DATA.md).

> Скрипт удаляет пользовательские Kafka topics и объекты ksqlDB. Используйте его только с этим локальным стендом.

## Практические руководства

- [Работа с ksqlDB и REST Proxy](docs/KSQLDB_AND_REST_PROXY_GUIDE.md) — Stream, Table, push/pull queries, CTAS, CSAS, repartition, агрегации, windowing, join и REST API;
- [Kafka REST Proxy](docs/REST_PROXY_GUIDE.md) — JSON, Avro, Schema Registry, producer и consumer API;
- [Учебные данные](docs/LAB_DATA.md) — подготовка и проверка исходных topics.

## Полезные команды

Посмотреть логи всех сервисов:

```bash
docker compose logs -f
```

Посмотреть логи одного сервиса:

```bash
docker compose logs -f kafka-rest
```

Остановить контейнеры без удаления volume PostgreSQL:

```bash
docker compose down
```

Полностью удалить контейнеры и volumes:

```bash
docker compose down -v
```

## Ограничения

- один Kafka broker;
- replication factor равен `1`;
- Kafka использует PLAINTEXT;
- отсутствуют TLS, SASL и Kafka ACL;
- REST Proxy не защищён внешней аутентификацией;
- конфигурация не обеспечивает отказоустойчивость.

## Безопасность

Не добавляйте в репозиторий `.env`, Postman environments, токены, сертификаты или рабочие данные. `.gitignore` исключает наиболее распространённые локальные файлы с секретами.
