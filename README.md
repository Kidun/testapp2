# Airflow Data Collection and Analysis Pipeline

Домашнее задание №5 "Оркестратор Airflow"

## Описание проекта

Проект представляет собой полноценный пайплайн сбора и анализа данных на базе Apache Airflow.

### Основные возможности:

1. **Сбор данных каждые 3 минуты** из 3 источников:
   - Погода (Open-Meteo API)
   - Курсы валют (ExchangeRate API)
   - Криптовалюты (CoinGecko API)

2. **Почасовой описательный анализ**:
   - Расчет базовых статистик
   - Построение графиков распределения
   - Сохранение результатов в отдельную папку

3. **Анализ ошибок каждые 2 часа**:
   - Анализ времени ответа сервисов
   - Подсчет количества ошибок
   - Автоматическое завершение работы при превышении порога ошибок (>10)

4. **База данных**:
   - SQLite для хранения данных
   - SQLAlchemy ORM
   - 10+ колонок данных + метаданные

## Структура проекта

```
.
├── dags/
│   ├── data_collection_dag.py    # Основной DAG сбора данных (каждые 3 мин)
│   ├── hourly_analysis_dag.py    # Почасовой анализ данных
│   └── error_analysis_dag.py     # Анализ ошибок (каждые 2 часа)
├── plugins/
│   ├── database.py               # Модели базы данных (SQLAlchemy ORM)
│   └── data_collectors.py        # Функции сбора данных из источников
├── data/
│   ├── pipeline_data.db          # SQLite база данных (создается автоматически)
│   ├── hourly_analysis/          # Результаты почасового анализа
│   └── error_analysis/           # Результаты анализа ошибок
├── logs/                         # Логи Airflow
├── Dockerfile                    # Docker образ с зависимостями
├── docker-compose.yml            # Конфигурация Docker Compose
├── requirements.txt              # Python зависимости
└── README.md                     # Этот файл
```

## Требования

- Docker
- Docker Compose
- 4GB+ свободной RAM
- Доступ к интернету для API запросов

## Установка и запуск

### 1. Подготовка окружения

```bash
# Создайте файл .env для настройки AIRFLOW_UID
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### 2. Запуск контейнеров

```bash
# Сборка и запуск всех сервисов
docker-compose up -d

# Проверка статуса контейнеров
docker-compose ps
```

### 3. Доступ к веб-интерфейсу

После запуска откройте браузер:

```
URL: http://localhost:8080
Логин: airflow
Пароль: airflow
```

### 4. Активация DAG'ов

В веб-интерфейсе Airflow:
1. Перейдите в раздел "DAGs"
2. Найдите три DAG'а:
   - `data_collection_pipeline` (каждые 3 минуты)
   - `hourly_analysis_pipeline` (каждый час)
   - `error_analysis_pipeline` (каждые 2 часа)
3. Активируйте их, переключив тумблер в положение "On"

## Описание DAG'ов

### 1. Data Collection Pipeline (каждые 3 минуты)

**Задачи:**
- `initialize_iteration` - инициализация итерации
- `collect_source1` - сбор данных о погоде
- `collect_source2` - сбор курсов валют
- `collect_source3` - сбор цен криптовалют
- `wait_for_all_sources` - ожидание завершения всех источников
- `save_to_database` - сохранение в базу данных

**Собираемые данные (10 колонок):**
1. temperature (температура)
2. humidity (влажность)
3. wind_speed (скорость ветра)
4. usd_rate (курс USD)
5. eur_rate (курс EUR)
6. gbp_rate (курс GBP)
7. cny_rate (курс CNY)
8. btc_price (цена Bitcoin)
9. eth_price (цена Ethereum)
10. stock_index (индекс рынка)

**Метаданные:**
- parse_time (время парсинга)
- failed_requests (количество ошибок)
- source1/2/3_response_time (время ответа каждого источника)
- source1/2/3_success (успешность запроса)

### 2. Hourly Analysis Pipeline (каждый час)

**Задачи:**
- `calculate_statistics` - расчет статистик (среднее, медиана, std, и т.д.)
- `plot_distributions` - построение графиков распределения для каждой колонки
- `save_analysis_summary` - сохранение сводки анализа

**Выходные файлы (в `data/hourly_analysis/`):**
- `statistics_YYYYMMDD_HHMMSS.csv` - статистики
- `overview_distributions_YYYYMMDD_HHMMSS.png` - обзорный график всех распределений
- `{column}_distribution_YYYYMMDD_HHMMSS.png` - графики для каждой колонки
- `analysis_summary_YYYYMMDD_HHMMSS.txt` - сводка анализа

### 3. Error Analysis Pipeline (каждые 2 часа)

**Задачи:**
- `analyze_response_times` - анализ времени ответа и ошибок
- `create_error_plots` - построение графиков ошибок
- `check_error_threshold` - проверка порога ошибок
- `terminate_pipeline` / `continue_pipeline` - завершение или продолжение работы

**Выходные файлы (в `data/error_analysis/`):**
- `response_stats_YYYYMMDD_HHMMSS.txt` - статистика времени ответа
- `response_times_YYYYMMDD_HHMMSS.png` - графики времени ответа
- `error_analysis_YYYYMMDD_HHMMSS.png` - графики анализа ошибок
- `pipeline_termination.log` - лог завершения (если превышен порог)

**Условие завершения:**
- Если количество ошибок за последние 2 часа > 10, пайплайн завершается с ошибкой

## Полезные команды

### Просмотр логов

```bash
# Логи всех сервисов
docker-compose logs -f

# Логи только scheduler
docker-compose logs -f airflow-scheduler

# Логи только webserver
docker-compose logs -f airflow-webserver
```

### Просмотр данных

```bash
# Подключение к контейнеру
docker-compose exec airflow-scheduler bash

# Просмотр базы данных
cd /opt/airflow/data
ls -lh pipeline_data.db

# Просмотр результатов анализа
ls -lh /opt/airflow/data/hourly_analysis/
ls -lh /opt/airflow/data/error_analysis/
```

### Остановка и перезапуск

```bash
# Остановка всех контейнеров
docker-compose down

# Остановка с удалением volumes (полная очистка)
docker-compose down -v

# Перезапуск
docker-compose restart
```

### Очистка данных

```bash
# Удаление данных (сохранив контейнеры)
rm -rf data/pipeline_data.db
rm -rf data/hourly_analysis/*
rm -rf data/error_analysis/*
```

## Тестирование компонентов

### Тест сбора данных

```bash
docker-compose exec airflow-scheduler python /opt/airflow/plugins/data_collectors.py
```

### Тест базы данных

```bash
docker-compose exec airflow-scheduler python /opt/airflow/plugins/database.py
```

## Мониторинг и отладка

### Проверка работы DAG'ов

1. Откройте веб-интерфейс: http://localhost:8080
2. Перейдите в раздел "DAGs"
3. Кликните на название DAG для просмотра деталей
4. Вкладка "Graph" - визуализация задач
5. Вкладка "Logs" - логи выполнения

### Ручной запуск DAG

В веб-интерфейсе:
1. Найдите нужный DAG
2. Нажмите кнопку "Trigger DAG" (▶️)

Или через CLI:

```bash
docker-compose exec airflow-scheduler airflow dags trigger data_collection_pipeline
docker-compose exec airflow-scheduler airflow dags trigger hourly_analysis_pipeline
docker-compose exec airflow-scheduler airflow dags trigger error_analysis_pipeline
```

## Решение проблем

### Контейнеры не запускаются

```bash
# Проверьте логи
docker-compose logs

# Пересоздайте контейнеры
docker-compose down -v
docker-compose up -d
```

### DAG'и не видны в интерфейсе

```bash
# Проверьте наличие ошибок в DAG'ах
docker-compose exec airflow-scheduler airflow dags list

# Перезапустите scheduler
docker-compose restart airflow-scheduler
```

### Ошибки при сборе данных

- Проверьте доступ к интернету
- Проверьте, что API не блокируют запросы
- Просмотрите логи задачи в веб-интерфейсе

## Архитектура решения

### База данных

Используется SQLite с файловым хранилищем для простоты развертывания. В production-окружении рекомендуется использовать PostgreSQL в отдельном контейнере.

### ORM и управление соединениями

- SQLAlchemy ORM для работы с базой данных
- Контекстные менеджеры (`with get_db()`) для автоматического закрытия соединений
- Транзакции с автоматическим commit/rollback

### Параллелизм

Сбор данных из 3 источников выполняется параллельно через отдельные задачи Airflow, что ускоряет процесс сбора.

## Соответствие требованиям

✅ **1.0 балла** - Сбор данных каждые 3 минуты:
- ✅ 0.1 - Инициализация итерации
- ✅ 0.3 - Параллельный сбор из 3 источников
- ✅ 0.2 - Запись времени парсинга и количества ошибок
- ✅ 0.2 - Передача в базу данных
- ✅ 0.2 - 10+ колонок данных

✅ **0.3 балла** - Почасовой описательный анализ:
- ✅ 0.1 - Базовые статистики
- ✅ 0.1 - Графики распределения
- ✅ 0.1 - Сохранение в отдельную папку

✅ **0.4 балла** - Анализ ошибок каждые 2 часа:
- ✅ 0.3 - Описание, графики, отдельная папка
- ✅ 0.1 - Завершение при превышении порога ошибок

✅ **0.1 балла** - База данных:
- ✅ SQLite в том же контейнере
- ✅ SQLAlchemy ORM
- ✅ Контекстные менеджеры для соединений

**Итого: 1.8 баллов** (можно получить +0.2 балла, используя отдельный контейнер для базы данных)

## Дополнительная информация

### Используемые API

1. **Open-Meteo** - бесплатный API погоды (без ключа)
2. **ExchangeRate API** - курсы валют (бесплатный tier)
3. **CoinGecko** - цены криптовалют (без ключа)

Все API имеют лимиты на количество запросов, но для данного домашнего задания их достаточно.

### Время работы

- Для накопления данных для анализа рекомендуется запустить пайплайн минимум на 2-3 часа
- Для демонстрации всех функций лучше оставить на сутки

### Автор

Домашнее задание выполнено для курса по оркестраторам данных.

## Лицензия

Для образовательных целей.
