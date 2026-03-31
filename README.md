# Clickstream ETL

Проект реализует ETL-пайплайн для датасета eCommerce clickstream:

- источник: PostgreSQL (`public.events`)
- обработка: Apache Spark + onETL
- назначение: ClickHouse (`clickstream.silver_events`)
- режимы: full snapshot и incremental load через HWM

## Цель проекта

Построить воспроизводимый ETL-пайплайн, который:

- загружает исходные clickstream-данные в PostgreSQL;
- обрабатывает данные через Spark + onETL;
- пишет результат в ClickHouse;
- поддерживает full snapshot и incremental загрузку по HWM (`event_time`);
- предоставляет API для запуска пайплайна и контроля статуса.

## Архитектура пайплайна

```text
Kaggle CSV -> scripts/load_csv.py -> PostgreSQL.public.events
            -> app.etl.pipeline (Spark + onETL + transforms)
            -> ClickHouse.clickstream.silver_events
            -> HWM store (data/hwm_store/*.yml)
```

## Технологии

- Python 3.11
- Apache Spark 3.5.1
- onETL
- PostgreSQL 16
- ClickHouse 24.3
- FastAPI
- Docker Compose

## Структура

- `app/etl/pipeline.py` — запуск full и incremental ETL
- `app/etl/transforms.py` — трансформации данных
- `scripts/load_csv.py` — загрузка CSV в PostgreSQL
- `scripts/reset_etl_state.py` — очистка источника, назначения и HWM
- `scripts/check_etl_counts.py` — проверка количества строк
- `app/web/main.py` — REST API
- `config/default.yaml` — параметры подключения и Spark/HWM
- `.env.example` — пример переменных окружения

## Конфигурация

`.env.example` уже содержит рабочие значения для локального запуска через Docker Compose.
Можно копировать без правок.

1. Создать `.env`:

```bash
cp .env.example .env
```

2. При необходимости изменить значения в `.env` и `config/default.yaml`.

3. При необходимости переопределить путь к конфигу:

```bash
export CONFIG_PATH=/absolute/path/to/config/default.yaml
```

## Запуск инфраструктуры

```bash
docker compose build spark
docker compose up -d
docker compose ps
```

## Подготовка данных

1. Скачать датасет с Kaggle:
   [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
2. Распаковать CSV-файл `2019-Oct.csv` (или другой месяц).
3. Положить файл в каталог проекта:

```bash
/Users/kirill/Clickstream-ETL/data/raw/2019-Oct.csv
```

В контейнере этот путь доступен как:

```bash
/app/data/raw/2019-Oct.csv
```

## Загрузка исходных данных

Скрипт `scripts/load_csv.py` автоматически создает таблицу `events` и индексы.

Поведение:

- без `--append`: выполняется `TRUNCATE events` и загрузка с нуля
- с `--append`: новые строки добавляются к существующим

Примеры:

```bash
docker compose exec spark python scripts/load_csv.py --file /app/data/raw/2019-Oct.csv --limit 5000
```

```bash
docker compose exec spark python scripts/load_csv.py --append --file /app/data/raw/2019-Oct.csv --offset 5000 --limit 5000
```

## ETL

Full snapshot:

```bash
docker compose exec spark python -m app.etl.full_load
```

Incremental:

```bash
docker compose exec spark python -m app.etl.incremental_load
```

CLI через корневой entrypoint:

```bash
docker compose exec spark python main.py full
docker compose exec spark python main.py incremental
docker compose exec spark python main.py api --host 0.0.0.0 --port 8000
```

### Логика incremental

- HWM колонка: `event_time`
- читаются только строки `event_time > last_hwm`
- HWM обновляется после успешной записи

## Logging setup

Логирование настроено через:

```python
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
```

Ключевые шаги, которые логируются в ETL:

- старт ETL и режим запуска;
- параметры источника и назначения;
- старт чтения из PostgreSQL;
- количество прочитанных строк;
- количество строк после трансформаций;
- запись в ClickHouse;
- обновление HWM (через `IncrementalStrategy`);
- длительность выполнения.

## Трансформации

Реализовано в `app/etl/transforms.py`:

1. Фильтрация невалидных данных:
   - удаление строк без `product_id`
   - фильтрация по допустимым `event_type`
2. Фильтрация подозрительной активности:
   - удаление сессий с числом событий выше `etl.max_events_per_session`
3. Парсинг категорий:
   - `top_category`, `sub_category` из `category_code`
4. Производные поля:
   - `event_date`, `event_hour`, `day_of_week`

## Полный сценарий проверки

```bash
docker compose exec spark python scripts/reset_etl_state.py
docker compose exec spark python scripts/load_csv.py --file /app/data/raw/2019-Oct.csv --limit 5000
docker compose exec spark python -m app.etl.full_load
docker compose exec spark python scripts/load_csv.py --append --file /app/data/raw/2019-Oct.csv --offset 5000 --limit 5000
docker compose exec spark python -m app.etl.incremental_load
docker compose exec spark python scripts/check_etl_counts.py
```

Ожидаемо после этого сценария:

- в PostgreSQL: 10000 строк
- в ClickHouse: 10000 строк

## Сброс состояния

```bash
docker compose exec spark python scripts/reset_etl_state.py
```

Скрипт очищает:

- `public.events` в PostgreSQL
- `clickstream.silver_events` в ClickHouse
- HWM-файлы в `data/hwm_store`

## REST API

Запуск:

```bash
docker compose exec spark uvicorn app.web.main:app --host 0.0.0.0 --port 8000
```

Эндпоинты:

- `POST /etl/full`
- `POST /etl/incremental`
- `GET /etl/status/{id}`
- `GET /etl/history`

Swagger: `/docs`

### Примеры curl

```bash
curl -X POST http://localhost:8000/etl/full
```

```bash
curl -X POST http://localhost:8000/etl/incremental
```

```bash
curl http://localhost:8000/etl/status/<job_id>
```

```bash
curl http://localhost:8000/etl/history
```

## Проверка результата в PostgreSQL и ClickHouse

Проверка через готовый скрипт:

```bash
docker compose exec spark python scripts/check_etl_counts.py
```

Проверка напрямую SQL:

```bash
docker compose exec postgres psql -U clickstream_user -d clickstream -c "SELECT COUNT(*) FROM public.events;"
```

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM clickstream.silver_events;"
```
