# Kafka, ksqlDB, REST Proxy, Conduktor Console

Локальный Kafka-стек для изучения потоковой обработки данных и HTTP-интеграции с Kafka. Проект запускает готовое окружение с Kafka, Schema Registry, ksqlDB, Kafka REST Proxy и Conduktor Console.

Репозиторий подходит для самостоятельной практики: можно создавать topics, выполнять запросы ksqlDB, публиковать и читать сообщения через REST Proxy, работать со схемами и наблюдать за состоянием Kafka через веб-интерфейс.

> Проект предназначен для локального обучения и разработки.

## Состав

| Компонент | Docker-образ | Назначение | Локальный адрес |
| --- | --- | --- | --- |
| Kafka | `confluentinc/cp-kafka:7.3.0` | Хранение и доставка сообщений | `localhost:9092` |
| ZooKeeper | `confluentinc/cp-zookeeper:7.3.0` | Координация Kafka в используемой версии | `localhost:2181` |
| Schema Registry | `confluentinc/cp-schema-registry:7.3.0` | Хранение Avro, JSON Schema и Protobuf схем | `http://localhost:8081` |
| Kafka REST Proxy | `confluentinc/cp-kafka-rest:7.3.0` | HTTP API для Kafka | `http://localhost:8082` |
| ksqlDB Server | `confluentinc/cp-ksqldb-server:7.3.0` | Потоковые SQL-запросы | `http://localhost:8088` |
| Conduktor Console | `conduktor/conduktor-console:1.43.3` | Веб-интерфейс для работы с Kafka | `http://localhost:8080` |
| PostgreSQL | `postgres:14` | Служебная база Conduktor | Только внутри Docker-сети |

Все компоненты запускаются одним `docker-compose.yml`.

Версии образов зафиксированы в `docker-compose.yml`, чтобы окружение запускалось воспроизводимо. При обновлении рекомендуется менять версии компонентов Confluent согласованно.

## Требования

- Docker Desktop или Docker Engine;
- Docker Compose v2;
- `curl`.

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

## Практические руководства

- [Работа с ksqlDB](docs/KSQLDB_GUIDE.md) — Stream, Table, push/pull queries, CTAS, CSAS, repartition, агрегации, windowing и join;
- [Kafka REST Proxy](docs/REST_PROXY_GUIDE.md) — JSON, Avro, Schema Registry, producer и consumer API.

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
