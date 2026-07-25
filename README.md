# Сервис рекомендаций фильмов и сериалов

Рекомендательная система для фильмов и сериалов, построенная на комбинации контентных признаков и данных о пользовательских предпочтениях (лайках/дизлайках). Проект реализован в рамках финального задания курса по рекомендательным системам Karpov Courses HardML.

## Архитектура

Система состоит из четырёх микросервисов, взаимодействующих через брокер сообщений RabbitMQ, базу данных Redis и HTTP-запросы:

```mermaid
graph TD
    subgraph "Web"
        WA[Web App<br/>Flask, порт 8000]
    end

    subgraph "Backend"
        RS[Recommendations Service<br/>FastAPI, порт 5001]
        EC[Event Collector<br/>FastAPI, порт 5000]
    end

    subgraph "Data & ML"
        RP[Regular Pipeline<br/>Асинхронный скрипт]
    end

    subgraph "Infrastructure"
        RMQ[(RabbitMQ<br/>порт 5672)]
        RD[(Redis Stack<br/>порт 6379)]
    end

    WA -->|GET /recs/{user_id}| RS
    WA -->|POST /interact| EC
    WA -->|POST /add_items| RS

    EC -->|publish message| RMQ
    RMQ -->|consume messages| RP

    RP -->|чтение/запись| RD
    RS -->|чтение| RD
    EC -->|запись истории| RD

    style WA fill:#e1f5fe
    style RS fill:#e8f5e9
    style EC fill:#e8f5e9
    style RP fill:#fff3e0
    style RMQ fill:#f3e5f5
    style RD fill:#f3e5f5
```

### Поток данных

1. Пользователь через **Web App** просматривает рекомендации (HTTP-запрос к **Recommendations Service**).
2. Пользователь ставит лайк/дизлайк — **Web App** отправляет событие в **Event Collector**.
3. **Event Collector** публикует событие в очередь **RabbitMQ** и сохраняет факт взаимодействия в **Redis**.
4. **Regular Pipeline** асинхронно забирает сообщения из RabbitMQ, накапливает их в CSV и периодически пересчитывает рекомендации.
5. Результаты расчётов (топ-популярные и персональные ALS-рекомендации) сохраняются в **Redis**.
6. **Recommendations Service** читает готовые рекомендации из Redis и отдаёт их пользователю.

## Компоненты

### 1. Shared-пакет (`shared/`)

Общий пакет, используемый всеми микросервисами. Устанавливается как локальная зависимость (`./shared` в `requirements.txt`).

#### `shared/config.py` — Конфигурация приложения

Централизованное управление настройками через переменные окружения с использованием `pydantic-settings`. Все настройки агрегируются в единый объект `AppSettings`.

**Группы настроек:**

| Класс | Env-префикс | Назначение |
|-------|-------------|------------|
| `RedisSettings` | `REDIS_` | Подключение к Redis, префиксы ключей |
| `RabbitSettings` | `RABBIT_` | Подключение к RabbitMQ, очередь/обмен |
| `WatchedFilterSettings` | `WF_` | Префикс для фильтра просмотренных |
| `RegularPipelineSettings` | `REGULAR_PIPE_` | Параметры ALS-модели и интервал сбора |
| `ServicesSettings` | `SERVICES_` | URL сервисов и порт webapp |
| `RecommendationSettings` | `RECOMMENDATION_` | Epsilon для exploration, TOP_K |

#### `shared/models.py` — Pydantic-модели

- `RecommendationsResponse` — ответ с рекомендациями (`item_ids: list[str]`)
- `InteractEvent` — событие взаимодействия пользователя (`user_id`, `item_ids`, `actions`, `timestamp`)
- `NewItemsEvent` — запрос на добавление новых объектов (`item_ids`, `genres`)

#### `shared/watched_filter.py` — Фильтр просмотренных

Утилита для хранения в Redis фактов просмотра фильмов пользователями. Используется для исключения уже показанных фильмов из рекомендаций. Ключи хранятся в формате `{prefix}-{user_id}-{item_id}`.

**Методы:**
- `add(user_id, item_ids)` — отметить фильмы как просмотренные
- `get(user_id, item_id)` — проверить, просмотрен ли фильм
- `filter_user_items(user_id, item_ids)` — отфильтровать уже просмотренные
- `remove_all()` — очистить всю историю просмотров

#### `shared/logger.py` — Настройка логирования

Фабрика логгеров с выводом в stdout и форматированием `%(asctime)s - %(name)s - %(levelname)s - %(message)s`.

---

### 2. Recommendations Service (`recommendations/`)

Бекенд-сервис на **FastAPI** (порт **5001**), отвечающий за выдачу персонализированных рекомендаций.

**Эндпоинты:**

| Метод | Путь | Описание |
|-------|------|----------|
| `GET` | `/healthcheck` | Проверка статуса сервиса (возвращает `200`) |
| `GET` | `/cleanup` | Сброс состояния: очищает Redis и список известных `item_id` |
| `POST` | `/add_items` | Добавление новых объектов рекомендаций в систему |
| `GET` | `/recs/{user_id}` | Получение списка рекомендаций для пользователя |

**Логика рекомендаций (`/recs/{user_id}`):**

1. **Холодный пользователь** (нет истории взаимодействий в Redis по ключу `{INTERACTION_PREFIX}-{user_id}`) — возвращаются **топ-10 популярных** фильмов из Redis (ключ `{TOP_RECOMMENDATION_PREFIX}`). Из топ-списка случайно выбирается `TOP_K` элементов.
2. **Пользователь с историей** — из Redis загружаются персональные рекомендации, полученные алгоритмом ALS (ключ `{ALS_RECOMMENDATION_PREFIX}-{user_id}`). Из списка исключаются уже просмотренные фильмы (через `WatchedFilter`), затем случайно выбирается `TOP_K` элементов.
3. **Epsilon-жадная стратегия** — с вероятностью ε = 0.05 (по умолчанию) в рекомендации добавляются случайные фильмы из глобального множества `unique_item_ids` для исследования новых интересов пользователя. Если рекомендаций нет совсем — возвращаются только случайные.
4. После формирования списка все показанные фильмы сохраняются в `WatchedFilter`.

---

### 3. Event Collector (`event_collector/`)

Сервис сбора обратной связи от пользователей на **FastAPI** (порт **5000** внутри контейнера, наружу маппится на **5002**). Принимает события лайков/дизлайков, передаёт их в очередь RabbitMQ и сохраняет историю в Redis.

**Эндпоинты:**

| Метод | Путь | Описание |
|-------|------|----------|
| `GET` | `/healthcheck` | Проверка статуса сервиса |
| `POST` | `/interact` | Приём события взаимодействия пользователя |

**Формат запроса `/interact`:**

```json
{
  "user_id": "uuid-пользователя",
  "item_ids": ["movie_1", "movie_2"],
  "actions": ["like", "dislike"],
  "timestamp": 1696000000.0
}
```

**При получении события сервис:**
1. Публикует сообщение в RabbitMQ (exchange `user.interact`, routing key `user.interact.message`).
2. Сохраняет историю взаимодействий пользователя в Redis через JSON-структуру (ключ `{INTERACTION_PREFIX}-{user_id}`) — список всех `item_ids`, с которыми взаимодействовал пользователь.

**CORS:** Разрешены запросы с любых источников (`allow_origins=["*"]`).

---

### 4. Regular Pipeline (`regular_pipeline/`)

Асинхронный скрипт на Python, выполняющий роль планировщика задач. Работает в бесконечном цикле и параллельно выполняет две ключевые задачи через `asyncio.gather`:

#### Сбор событий (`collect_messages`)

- Подключается к очереди RabbitMQ `user_interactions` (exchange `user.interact`, direct).
- Каждые `MESSAGES_COLLECTION_INTERVAL` секунд (по умолчанию 10) сохраняет накопленные события в CSV-файл (`data/interactions.csv`).
- При сохранении данные "взрываются" (explode) по колонкам `item_ids` и `actions`, чтобы каждое взаимодействие стало отдельной строкой.
- Если файл уже существует — новые данные дописываются к существующим.

#### Расчёт рекомендаций (`calculate_recommendations`)

Запускается в бесконечном цикле с интервалом 10 секунд:

1. **Top recommendations** — вычисляет топ-100 фильмов по количеству лайков (учитывается только последнее действие пользователя по каждой паре user-item) и сохраняет в Redis (ключ `{TOP_RECOMMENDATION_PREFIX}`).
2. **ALS рекомендации** — строит матрицу взаимодействий пользователь-товар и обучает модель **Alternating Least Squares** (ALS) из библиотеки `implicit`. Персональные рекомендации для каждого пользователя сохраняются в Redis (ключ `{ALS_RECOMMENDATION_PREFIX}-{user_id}`).

**Компоненты пайплайна:**

| Класс | Назначение |
|-------|------------|
| `RedisManager` | Управление записью в Redis (топ-рекомендации и ALS) |
| `DataPreprocessor` | Маппинг строковых ID пользователей/товаров в числовые индексы, создание разреженной матрицы взаимодействий (`scipy.sparse.csr_matrix`). Лайки → +1, дизлайки → -1. Фильтрация пользователей без положительных действий |
| `ALSRecommender` | Обёртка над `implicit.ALS`. Параметры: `factors=64`, `iterations=15`, `alpha=1.0`, `regularization=0.1`, `random_state=42`. Генерация рекомендаций с фильтрацией уже просмотренных |
| `ModelEvaluator` | Оценка качества по метрике **HitRate@k** (доля пользователей, у которых хотя бы один релевантный фильм попал в топ-k рекомендаций) |
| `HyperparameterOptimizer` | Оптимизация гиперпараметров ALS через **Optuna** (закомментирована в текущей версии, но код присутствует) |

---

### 5. Web App (`webapp/`)

Веб-интерфейс на **Flask** (порт **8000**) для демонстрации работы рекомендательной системы.

**Функциональность:**
- Генерация уникального `user_id` для каждого нового посетителя (через cookies Flask session).
- Возможность передать `user_id` через query-параметр `?user_id=...` для тестирования конкретного пользователя.
- Отображение топ-12 рекомендованных фильмов с постерами, названиями и ссылками на IMDb.
- Кнопки **Like** и **Dislike** для каждого фильма — отправляют обратную связь через AJAX на `/interact` (прокси-запрос к Event Collector).
- При запуске автоматически загружает все доступные фильмы в Recommendations Service через `/add_items` (с жанрами).

**Данные:**
- `static/movies.csv` — данные о фильмах (movieId, title, genres)
- `static/links.csv` — связки movieId → IMDb ID
- `static/images/{movieId}.jpg` — постеры фильмов

---

## Инфраструктура

| Компонент | Назначение |
|-----------|------------|
| **RabbitMQ** (3-management) | Брокер сообщений для передачи событий взаимодействия от Event Collector к Regular Pipeline. Management UI на порту 15672 |
| **Redis Stack** (redis/redis-stack-server) | Хранилище данных: топ-популярные фильмы, персональные ALS-рекомендации, история взаимодействий, фильтр просмотренных. Поддержка JSON-модуля |
| **Docker Compose** | Оркестрация всех сервисов |

## Переменные окружения

Для запуска проекта необходимо создать файл `shared/.env` со следующими переменными:

```env
# === Redis ===
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0
REDIS_INTERACTION_PREFIX=interaction
REDIS_ALS_RECOMMENDATION_PREFIX=als_recommendation
REDIS_TOP_RECOMMENDATION_PREFIX=top_recommendation

# === RabbitMQ ===
RABBIT_USER=guest
RABBIT_PASSWORD=guest
RABBIT_HOST=rabbitmq
RABBIT_PORT=5672
RABBIT_QUEUE_NAME=user_interactions
RABBIT_ROUTING_KEY=user.interact.message
RABBIT_EXCHANGE=user.interact

# === Watched Filter ===
WF_PREFIX=watched

# === Regular Pipeline ===
REGULAR_PIPE_MESSAGES_COLLECTION_INTERVAL=10
REGULAR_PIPE_ALS_FACTORS=64
REGULAR_PIPE_ALS_ITERATIONS=15
REGULAR_PIPE_ALS_ALPHA=1.0
REGULAR_PIPE_ALS_REGULARIZATION=0.1
REGULAR_PIPE_ALS_RANDOM_STATE=42

# === Services ===
SERVICES_RECOMMENDATION_SERVICE_URL="http://recommendations:5001"
SERVICES_INTERACTIONS_SERVICE_URL="http://event-collector:5000"
SERVICES_WEBAPP_PORT=8000

# === Recommendation ===
RECOMMENDATION_EPSILON=0.05
RECOMMENDATION_TOP_K=10

# === App ===
LOG_LEVEL=INFO
```

> **Примечание:** `SERVICES_RECOMMENDATION_SERVICE_URL` и `SERVICES_INTERACTIONS_SERVICE_URL` должны указывать на адреса, доступные из браузера пользователя (для Web App). Внутри Docker-сети сервисы обращаются друг к другу по именам контейнеров.

## Запуск проекта

```bash
# 1. Создать файл с переменными окружения
cp shared/.env.example shared/.env
# и отредактировать при необходимости

# 2. Запуск всех сервисов
docker-compose up --build

# Сервисы будут доступны на портах:
# - Web App: http://localhost:8000
# - Recommendations Service: http://localhost:5001
# - Event Collector: http://localhost:5002 (внешний) / 5000 (внутренний)
# - RabbitMQ Management: http://localhost:15672 (guest/guest)
# - Redis: localhost:6379
```

## Технологический стек

- **Язык:** Python 3.11
- **Веб-фреймворки:** FastAPI, Flask
- **Рекомендации:** implicit (ALS), scipy, numpy
- **Обработка данных:** polars
- **Брокер сообщений:** RabbitMQ (aio-pika)
- **Кэш/БД:** Redis Stack (redis-py, JSON-модуль)
- **Валидация:** pydantic, pydantic-settings
- **Оптимизация:** optuna
- **Инфраструктура:** Docker, Docker Compose
- **Тестирование:** pytest

## Структура проекта

```
├── docker-compose.yml              # Оркестрация сервисов
├── README.md                       # Документация проекта
├── shared/                         # Общий пакет для всех сервисов
│   ├── __init__.py
│   ├── config.py                   # Pydantic-модели конфигурации (переменные окружения)
│   ├── logger.py                   # Настройка логирования
│   ├── models.py                   # Pydantic-модели данных (InteractEvent, NewItemsEvent, RecommendationsResponse)
│   ├── setup.py                    # setup.py для установки пакета
│   └── watched_filter.py           # Фильтр просмотренных фильмов (Redis)
├── recommendations/                # Сервис рекомендаций
│   ├── Dockerfile
│   ├── main.py                     # FastAPI приложение (порт 5001)
│   └── requirements.txt
├── event_collector/                # Сборщик событий
│   ├── Dockerfile
│   ├── main.py                     # FastAPI приложение (порт 5000)
│   └── requirements.txt
├── regular_pipeline/               # Пайплайн обучения
│   ├── Dockerfile
│   ├── main.py                     # Асинхронный скрипт (ALS + сбор сообщений)
│   ├── requirements.txt
│   └── data/                       # Директория для хранения взаимодействий (interactions.csv)
└── webapp/                         # Веб-интерфейс
    ├── Dockerfile
    ├── app.py                      # Flask приложение (порт 8000)
    ├── requirements.txt
    ├── static/
    │   ├── links.csv               # Связки movieId -> IMDb ID
    │   ├── movies.csv              # Данные о фильмах (movieId, title, genres)
    │   └── images/                 # Постеры фильмов ({movieId}.jpg)
    └── templates/
        └── index.html              # Шаблон главной страницы