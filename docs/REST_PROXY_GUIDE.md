# Kafka REST Proxy: практическое руководство

Kafka REST Proxy предоставляет HTTP-интерфейс для работы с Kafka. В локальном стенде сервис доступен на `http://localhost:8082`.

Документ содержит примеры публикации и чтения сообщений в форматах JSON и Avro. При использовании другого окружения замените адрес, имена топиков и идентификаторы схем.

1. **Подключение**

| Параметр | Значение по умолчанию |
| --- | --- |
| REST Proxy URL | `http://localhost:8082` |
| Авторизация | Не используется |
| Kafka | `kafka:29092` внутри Docker-сети |
| Schema Registry | `http://schema-registry:8081` внутри Docker-сети |

Внешние окружения могут требовать TLS, авторизацию и дополнительные HTTP-заголовки. Не храните пароли, токены и сертификаты в документации, Postman-коллекциях или исходном коде.

2. **Переменные для примеров**

Перед выполнением команд задайте адрес REST Proxy и имя топика:

```bash
export REST_PROXY_URL="http://localhost:8082"
export TOPIC_NAME="<topic-name>"
```

Если REST Proxy требует авторизацию, добавьте в каждый запрос выданный инфраструктурной командой заголовок. Например:

```http
Authorization: Bearer <token>
```

Для локального стенда этот заголовок не требуется.

3. **Проверка доступности REST Proxy**

```bash
curl --request GET \
  --url "${REST_PROXY_URL}/v3/clusters" \
  --header "Accept: application/json"
```

Успешный ответ содержит Kafka-кластер, к которому подключён REST Proxy. Если используется авторизация, к запросу необходимо добавить соответствующий заголовок.

4. **Публикация обычного JSON**

Этот вариант не использует Schema Registry и применим только к топикам с JSON-сериализацией.

```bash
curl --request POST \
  --url "${REST_PROXY_URL}/topics/${TOPIC_NAME}" \
  --header "Content-Type: application/vnd.kafka.json.v2+json" \
  --header "Accept: application/vnd.kafka.v2+json" \
  --data '{
    "records": [
      {
        "key": "EVENT-1001",
        "value": {
          "eventId": "EVENT-1001",
          "eventType": "ORDER_UPDATED",
          "eventTs": 1792058400000,
          "payload": {
            "orderId": "ORDER-1001",
            "status": "CONFIRMED"
          }
        }
      }
    ]
  }'
```

Несколько сообщений можно отправить одним запросом:

```bash
curl --request POST \
  --url "${REST_PROXY_URL}/topics/${TOPIC_NAME}" \
  --header "Content-Type: application/vnd.kafka.json.v2+json" \
  --header "Accept: application/vnd.kafka.v2+json" \
  --data '{
    "records": [
      {
        "key": "EVENT-1001",
        "value": {
          "eventId": "EVENT-1001",
          "eventType": "ORDER_CREATED",
          "eventTs": 1792058400000
        }
      },
      {
        "key": "EVENT-1002",
        "value": {
          "eventId": "EVENT-1002",
          "eventType": "ORDER_CANCELLED",
          "eventTs": 1792058460000
        }
      }
    ]
  }'
```

`key` является Kafka Key, а `value` — Kafka Value. Если `partition` не задана явно, Kafka выбирает партицию на основании ключа. Одинаковые ключи направляются в одну партицию при неизменном количестве партиций и алгоритме партиционирования.

Не отправляйте обычный JSON в топик, потребители которого ожидают Avro.

5. **Публикация Avro с передачей схемы**

В этом варианте REST Proxy регистрирует переданную схему в Schema Registry, проверяет сообщение и сериализует Kafka Value в Avro.

```bash
curl --request POST \
  --url "${REST_PROXY_URL}/topics/${TOPIC_NAME}" \
  --header "Content-Type: application/vnd.kafka.avro.v2+json" \
  --header "Accept: application/vnd.kafka.v2+json" \
  --data '{
    "value_schema": "{\"type\":\"record\",\"name\":\"ExampleEvent\",\"namespace\":\"org.example.kafka\",\"fields\":[{\"name\":\"eventId\",\"type\":\"string\"},{\"name\":\"eventType\",\"type\":\"string\"},{\"name\":\"eventTs\",\"type\":\"long\"},{\"name\":\"payload\",\"type\":\"string\"}]}",
    "records": [
      {
        "value": {
          "eventId": "EVENT-1001",
          "eventType": "ORDER_UPDATED",
          "eventTs": 1792058400000,
          "payload": "{\"orderId\":\"ORDER-1001\",\"status\":\"CONFIRMED\"}"
        }
      }
    ]
  }'
```

Схема и событие выше приведены только как технический пример. Для интеграции необходимо использовать целевую Avro-схему.

Передача полной схемы удобна для первичной проверки или регистрации схемы. Для штатной отправки рекомендуется использовать уже зарегистрированный идентификатор схемы.

6. **Публикация Avro по Schema ID**

В JSON-теле поля `key_schema_id` и `value_schema_id` должны быть числами. В следующем примере необходимо заменить `101` и `102` фактическими идентификаторами зарегистрированных схем:

```bash
curl --request POST \
  --url "${REST_PROXY_URL}/topics/${TOPIC_NAME}" \
  --header "Content-Type: application/vnd.kafka.avro.v2+json" \
  --header "Accept: application/vnd.kafka.v2+json" \
  --data '{
    "key_schema_id": 101,
    "value_schema_id": 102,
    "records": [
      {
        "key": "ORDER-1001",
        "value": {
          "eventId": "EVENT-1001",
          "eventType": "ORDER_UPDATED",
          "eventTs": 1792058400000,
          "payload": "{\"orderId\":\"ORDER-1001\",\"status\":\"CONFIRMED\"}"
        }
      }
    ]
  }'
```

Если Kafka Key не используется, удалите `key_schema_id` и `key` из запроса. Если ключ используется в Avro-запросе, он должен соответствовать схеме `key_schema_id`. В приведённом примере схема ключа должна описывать строку.

7. **Разбор ответа на публикацию**

Пример успешного ответа:

```json
{
  "offsets": [
    {
      "partition": 1,
      "offset": 42,
      "error_code": null,
      "error": null
    }
  ],
  "key_schema_id": 101,
  "value_schema_id": 102
}
```

Поля ответа:

- `partition` — партиция, в которую записано сообщение;
- `offset` — позиция сообщения внутри партиции;
- `error_code` и `error` — ошибка записи конкретного сообщения;
- `key_schema_id` и `value_schema_id` — использованные идентификаторы схем;
- при обычном JSON идентификаторы схем равны `null`.

HTTP-ответ может содержать результаты для нескольких сообщений. Необходимо проверять `error_code` и `error` у каждого элемента массива `offsets`.

8. **Чтение сообщений: JSON**

Этот раздел нужен только в том случае, если клиентское приложение будет читать сообщения через REST Proxy.

Задайте уникальные имена consumer group и consumer instance:

```bash
export CONSUMER_GROUP="example-group"
export CONSUMER_INSTANCE="example-consumer"
```

8.1. Создать consumer

```bash
curl --request POST \
  --url "${REST_PROXY_URL}/consumers/${CONSUMER_GROUP}" \
  --header "Content-Type: application/vnd.kafka.v2+json" \
  --header "Accept: application/vnd.kafka.v2+json" \
  --data "{
    \"name\": \"${CONSUMER_INSTANCE}\",
    \"format\": \"json\",
    \"auto.offset.reset\": \"earliest\",
    \"auto.commit.enable\": \"false\"
  }"
```

Ответ содержит `base_uri`. Сохраните его и используйте для последующих запросов этого consumer:

```bash
export CONSUMER_BASE_URI="<base_uri из ответа>"
```

Последующие запросы consumer API должны направляться в экземпляр REST Proxy, который создал consumer. Если REST Proxy работает в нескольких репликах, инфраструктура должна обеспечивать соответствующую маршрутизацию.

8.2. Подписаться на топик

```bash
curl --request POST \
  --url "${CONSUMER_BASE_URI}/subscription" \
  --header "Content-Type: application/vnd.kafka.v2+json" \
  --header "Accept: application/vnd.kafka.v2+json" \
  --data "{\"topics\":[\"${TOPIC_NAME}\"]}"
```

Успешный ответ: `204 No Content`.

8.3. Прочитать сообщения

```bash
curl --request GET \
  --url "${CONSUMER_BASE_URI}/records?timeout=3000&max_bytes=300000" \
  --header "Accept: application/vnd.kafka.json.v2+json"
```

Ответ содержит `topic`, `key`, `value`, `partition` и `offset` каждого сообщения.

8.4. Зафиксировать текущие offsets

Commit необходимо выполнять после успешной обработки полученных сообщений:

```bash
curl --request POST \
  --url "${CONSUMER_BASE_URI}/offsets" \
  --header "Content-Type: application/vnd.kafka.v2+json" \
  --header "Accept: application/vnd.kafka.v2+json" \
  --data '{}'
```

Пустой JSON-объект фиксирует текущие позиции полученных consumer records.

8.5. Удалить consumer

```bash
curl --request DELETE \
  --url "${CONSUMER_BASE_URI}" \
  --header "Accept: application/vnd.kafka.v2+json"
```

Успешный ответ: `204 No Content`. Удаление consumer instance освобождает ресурсы REST Proxy, но не удаляет сообщения или committed offsets в Kafka.

Для чтения Avro при создании consumer укажите `"format": "avro"`, а при чтении используйте:

```http
Accept: application/vnd.kafka.avro.v2+json
```

9. **Типовые ошибки**

| Ситуация | Возможная причина | Что проверить |
| --- | --- | --- |
| `401 Unauthorized` | Отсутствуют или неверны credentials | Заголовок авторизации и срок действия credentials |
| `403 Forbidden` | Недостаточно прав | Доступ к REST Proxy и Kafka ACL |
| `404 Not Found` | Неверный URL, топик или consumer instance | Адрес контура, имя топика и имя consumer |
| `40403 Consumer instance not found` | Consumer удалён, истёк или запрос попал не в тот экземпляр REST Proxy | `base_uri` и маршрутизацию между репликами |
| `406 Not Acceptable` | Формат consumer не совпадает с `Accept` | `format` consumer и media type запроса чтения |
| `40902` | Consumer instance с таким именем уже существует | Использовать уникальное имя либо удалить старый instance |
| `422` | Некорректное тело или конфигурация consumer | JSON-тело и значения параметров |
| Ошибка схемы | Value или Key не соответствует Avro-схеме | Schema ID, типы и обязательные поля |
| `5xx` | Недоступны REST Proxy, Kafka или Schema Registry | Состояние сервисов; повторить запрос с ограниченным backoff |

10. **Рекомендации для клиентского приложения**

- Устанавливайте timeout для каждого HTTP-запроса.
- Повторяйте только временные ошибки и ограничивайте количество попыток.
- Используйте задержку между повторными попытками.
- Учитывайте возможность повторной публикации сообщения после timeout.
- Используйте стабильный Kafka Key, если важен порядок событий одной сущности.
- Проверяйте результат записи каждого элемента массива `offsets`.
- Логируйте имя топика, partition и offset успешной записи.
- Не логируйте пароли, токены, сертификаты и чувствительное содержимое сообщений.
- Выполняйте commit только после успешной обработки прочитанных сообщений.
- Всегда удаляйте consumer instance после завершения ручной проверки.

11. **Postman**

К документу рекомендуется приложить очищенную Postman-коллекцию со следующими запросами:

1. Проверить доступность REST Proxy.
2. Отправить обычный JSON.
3. Отправить Avro с полной схемой.
4. Отправить Avro по Schema ID.
5. Создать consumer.
6. Подписаться на топик.
7. Прочитать сообщения.
8. Выполнить commit offsets.
9. Удалить consumer.

Рекомендуемые переменные Postman:

```text
restProxyUrl
topicName
keySchemaId
valueSchemaId
consumerGroup
consumerInstance
accessToken
```

В экспортируемой коллекции и environment-файле не должно быть реальных credentials, cookies или production-данных.

12. **Адаптация для другого окружения**

1. Замените URL REST Proxy.
2. Добавьте требуемые заголовки авторизации.
3. Укажите целевые Kafka topics.
4. Используйте фактические Avro-схемы ключа и значения.
5. Укажите Schema ID из Schema Registry выбранного окружения.
6. Замените примеры сообщений на данные своей предметной области.
7. Настройте уникальные имена consumer group и consumer instance.
