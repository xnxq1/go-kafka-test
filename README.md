# Тестовое задание: микросервис обработки сообщений на Go

## Цель задания

Необходимо разработать микросервис на Go, который принимает сообщения через HTTP API, сохраняет их в PostgreSQL, отправляет в Kafka для дальнейшей обработки, читает обработанные сообщения из Kafka и помечает их как обработанные в базе данных.

Также сервис должен предоставлять API для получения статистики по сообщениям.

## Технологические требования

* Go 1.20+
* PostgreSQL
* Kafka
* Docker / Docker Compose
* REST API
* README с инструкцией по запуску и тестированию

## Функциональные требования

### 1. Прием сообщений через HTTP API

Сервис должен предоставлять endpoint для создания сообщения.

Пример запроса:

```http
POST /messages
Content-Type: application/json
```

Тело запроса:

```json
{
  "content": "Hello world"
}
```

После получения сообщения сервис должен:

1. Провалидировать входные данные.
2. Сохранить сообщение в PostgreSQL.
3. Отправить событие с сообщением в Kafka.
4. Вернуть клиенту идентификатор созданного сообщения.

Пример ответа:

```json
{
  "id": "b7a1f7d2-3e91-4c9e-9f3a-1b7a9c4f1234",
  "status": "created"
}
```

### 2. Хранение сообщений в PostgreSQL

В базе данных должна храниться информация о сообщениях.

Минимальная структура таблицы `messages`:

```sql
id UUID PRIMARY KEY,
content TEXT NOT NULL,
status VARCHAR(32) NOT NULL,
created_at TIMESTAMP NOT NULL,
processed_at TIMESTAMP NULL
```

Возможные статусы:

* `created` — сообщение создано и сохранено;
* `sent` — сообщение отправлено в Kafka;
* `processed` — сообщение обработано;
* `failed` — при обработке произошла ошибка.

### 3. Отправка сообщений в Kafka

После сохранения сообщения в PostgreSQL сервис должен отправить событие в Kafka topic.

Пример topic:

```text
messages.created
```

Пример сообщения в Kafka:

```json
{
  "id": "b7a1f7d2-3e91-4c9e-9f3a-1b7a9c4f1234",
  "content": "Hello world",
  "created_at": "2026-06-26T12:00:00Z"
}
```

После успешной отправки в Kafka статус сообщения должен быть обновлен на `sent`.

### 4. Чтение сообщений из Kafka

Сервис должен читать сообщения из Kafka topic, имитирующего обработку сообщений.

Пример topic:

```text
messages.processed
```

После получения события об обработке сообщения сервис должен:

1. Найти сообщение в PostgreSQL по `id`.
2. Обновить статус сообщения на `processed`.
3. Заполнить поле `processed_at`.

Пример сообщения из Kafka:

```json
{
  "id": "b7a1f7d2-3e91-4c9e-9f3a-1b7a9c4f1234",
  "processed_at": "2026-06-26T12:01:00Z"
}
```

### 5. API для получения сообщений

Сервис должен предоставлять endpoint для получения списка сообщений.

Пример запроса:

```http
GET /messages
```

Пример ответа:

```json
[
  {
    "id": "b7a1f7d2-3e91-4c9e-9f3a-1b7a9c4f1234",
    "content": "Hello world",
    "status": "processed",
    "created_at": "2026-06-26T12:00:00Z",
    "processed_at": "2026-06-26T12:01:00Z"
  }
]
```

Желательно предусмотреть пагинацию:

```http
GET /messages?limit=20&offset=0
```

### 6. API для получения статистики

Сервис должен предоставлять endpoint для получения статистики по сообщениям.

Пример запроса:

```http
GET /stats
```

Пример ответа:

```json
{
  "total": 100,
  "created": 5,
  "sent": 10,
  "processed": 80,
  "failed": 5
}
```

Дополнительно можно добавить:

```json
{
  "average_processing_time_seconds": 12.5
}
```

## Нефункциональные требования

### Конфигурация

Все настройки должны задаваться через переменные окружения:

* порт HTTP-сервера;
* параметры подключения к PostgreSQL;
* адрес Kafka;
* названия Kafka topics;
* consumer group id.

Пример:

```env
HTTP_PORT=8080
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=messages
KAFKA_BROKERS=kafka:9092
KAFKA_TOPIC_CREATED=messages.created
KAFKA_TOPIC_PROCESSED=messages.processed
KAFKA_CONSUMER_GROUP=message-service
```

### Docker

Проект должен запускаться через Docker Compose.

В `docker-compose.yml` должны быть описаны:

* приложение на Go;
* PostgreSQL;
* Kafka;
* при необходимости Zookeeper или Kafka в KRaft mode.

Команда запуска:

```bash
docker compose up --build
```

### Миграции

Необходимо предусмотреть механизм создания таблиц в PostgreSQL.

Можно использовать:

* SQL-файлы миграций;
* golang-migrate;
* автоматическое создание таблицы при старте приложения.

Предпочтительно использовать миграции.

### Логирование

Сервис должен логировать основные события:

* старт приложения;
* успешное подключение к PostgreSQL;
* успешное подключение к Kafka;
* создание сообщения;
* отправку сообщения в Kafka;
* получение сообщения из Kafka;
* ошибки обработки.

### Обработка ошибок

Сервис должен корректно обрабатывать ошибки:

* невалидный JSON;
* пустое поле `content`;
* ошибка подключения к PostgreSQL;
* ошибка отправки сообщения в Kafka;
* сообщение не найдено при обработке;
* повторная обработка сообщения.

В случае ошибки API должно возвращать понятный JSON-ответ.

Пример:

```json
{
  "error": "content is required"
}
```

## Минимальные API endpoints

### Создание сообщения

```http
POST /messages
```

### Получение списка сообщений

```http
GET /messages
```

### Получение сообщения по ID

```http
GET /messages/{id}
```

### Получение статистики

```http
GET /stats
```

### Healthcheck

```http
GET /health
```

Пример ответа:

```json
{
  "status": "ok"
}
```

## Желательно, но не обязательно

Будет плюсом:

* Swagger / OpenAPI документация;
* unit-тесты;
* integration-тесты;
* graceful shutdown;
* retry при отправке сообщений в Kafka;
* отдельный producer и consumer слой;
* чистая архитектура проекта;
* Makefile;
* CI pipeline;
* линтер;
* rate limiting;
* idempotency при повторной обработке сообщений.

## Ожидаемая структура проекта

Пример структуры:

```text
.
├── cmd
│   └── app
│       └── main.go
├── internal
│   ├── config
│   ├── handler
│   ├── service
│   ├── repository
│   ├── kafka
│   └── model
├── migrations
├── docker-compose.yml
├── Dockerfile
├── Makefile
├── README.md
└── go.mod
```

## Требования к результату

На выходе необходимо предоставить:

1. Ссылку на Git-репозиторий с исходным кодом.
2. Ссылку на развернутый проект, доступный через интернет.
3. Инструкцию по подключению и тестированию API.
4. README с описанием:

    * как запустить проект локально;
    * как запустить проект через Docker;
    * какие переменные окружения используются;
    * какие endpoints доступны;
    * примеры curl-запросов;
    * краткое описание архитектуры.

## Примеры curl-запросов

Создание сообщения:

```bash
curl -X POST http://localhost:8080/messages \
  -H "Content-Type: application/json" \
  -d '{"content":"Hello world"}'
```

Получение списка сообщений:

```bash
curl http://localhost:8080/messages
```

Получение сообщения по ID:

```bash
curl http://localhost:8080/messages/{id}
```

Получение статистики:

```bash
curl http://localhost:8080/stats
```

Проверка состояния сервиса:

```bash
curl http://localhost:8080/health
```

## Критерии оценки

При проверке задания будет оцениваться:

* корректность работы HTTP API;
* корректность сохранения сообщений в PostgreSQL;
* корректность отправки и чтения сообщений из Kafka;
* обновление статуса обработанных сообщений;
* качество структуры проекта;
* читаемость и поддерживаемость кода;
* наличие Docker-запуска;
* качество README;
* обработка ошибок;
* возможность протестировать проект через интернет.

## Срок выполнения

Срок выполнения тестового задания: по договоренности.
