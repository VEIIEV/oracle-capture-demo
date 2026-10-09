# Общая инфраструктура и создание БД

Эта инструкция общая для всех трёх подходов. Здесь нет пользователей коннекторов, LogMiner grants или supplemental logging: они выдаются в своих инструкциях. Контейнерный образ создаёт готовую Oracle XE — отдельную команду CREATE DATABASE выполнять не нужно.

## 1. Подготовить Windows/WSL/IDEA

Требуется Docker Desktop с Linux containers и WSL2 backend, интеграция с Ubuntu, IDEA и JDK 17 на Windows. Желательно 16 GB RAM, несколько GB свободной RAM для контейнеров и место для образов/данных.

В PowerShell, после скачивания ZIP (если папка уже есть, выберите новый каталог вместо перезаписи работающего проекта):

```powershell
New-Item -ItemType Directory -Force 'C:\Users\VEIIEV\IdeaProjects'
Expand-Archive -LiteralPath 'C:\Users\VEIIEV\Downloads\oracle-capture-demo.zip' -DestinationPath 'C:\Users\VEIIEV\IdeaProjects'
```

Если имя скачанного файла имеет суффикс `(1)`, подставьте точное имя. В IDEA Open → `C:\Users\VEIIEV\IdeaProjects\oracle-capture-demo\pom.xml`; Maven project; JDK 17. Дождитесь импорта. Корневой POM агрегирует модуль custom.

В **Ubuntu WSL**:

```bash
cd /mnt/c/Users/VEIIEV/IdeaProjects/oracle-capture-demo
docker version
docker compose version
```

Все последующие bash-команды во всех документах выполняйте в WSL из **корня проекта**, даже когда открыт Markdown из docs. PowerShell используется только в явно помеченных блоках. `/mnt/c/...` — путь WSL, `C:\...` — путь Windows.

Если старый стенд работает, остановите его из его папки с `docker compose stop`, чтобы освободить порты. Новый project name из compose.yaml создаёт отдельные volumes. На Windows и в WSL используется одна копия исходников, конфигураций и custom/state.

## 2. Посмотреть необходимые файлы

В `../docker-compose.yaml` четыре сервиса: oracle, kafka, connect-jdbc, connect-cdc. Последние два пока не запускайте. Файлы уже в проекте, вручную перепечатывать Compose/Dockerfile не требуется.

```bash
docker compose config --quiet
docker compose up -d oracle kafka
docker compose ps
docker compose logs --tail=80 oracle
docker compose logs --tail=80 kafka
```

config --quiet только проверяет Compose; без вывода — проверка прошла. Дождитесь healthy у Oracle и Kafka. Первая загрузка Oracle image около 1.2 GB и инициализация занимают время.

Oracle: CDB XE, PDB XEPDB1, ROOT = CDB$ROOT. Контейнеры обращаются к `oracle:1521` / `kafka:29092`; с Windows — `localhost:1521` / `localhost:9092`. Oracle password SYS/SYSTEM для этой лаборатории — oracle; административные команды ниже используют OS authentication `/ as sysdba` внутри контейнера.

## 3. Создать APP и таблицу один раз

Откройте `sql/common/01-schema.sql` в IDEA, затем:

```bash
docker compose exec -T oracle sqlplus -s / as sysdba < sql/common/01-schema.sql
```

Файл создаёт только отсутствующие объекты:

- APP/app с CREATE SESSION, CREATE TABLE и quota на USERS. Это владелец данных, не пользователь коннектора.
- APP.ORDERS: ID NUMBER(18) NOT NULL PRIMARY KEY, STATUS VARCHAR2(50), AMOUNT NUMBER(12,2), UPDATED_AT TIMESTAMP(6) NOT NULL.
- Индекс (UPDATED_AT, ID), нужный для эффективного polling.

При повторном выполнении существующие строки/пользователи сохраняются; пароль существующего APP не меняется. Скрипт не мигрирует несовместимую ранее созданную таблицу. Используйте новую БД этого проекта или проверьте схему вручную.

ID и UPDATED_AT — cursor для JDBC и custom. Для CDC timestamp-колонка не обязательна; таблица одна для удобства сравнения. UTC timestamps в этой лаборатории записывает приложение/наши DML-команды; триггер не добавлен.

## 4. Добавить начальные строки от APP

```bash
docker compose exec -T oracle sqlplus -s app/app@//localhost:1521/XEPDB1 < sql/common/02-seed.sql
```

```bash
docker compose exec -T oracle sqlplus -s app/app@//localhost:1521/XEPDB1 < sql/common/03-check.sql
```

Должны появиться ID 1–3 с одинаковым UPDATED_AT `2020-01-01 00:00:00.123456`. batch=2 у polling-коннекторов позволит увидеть, что ID используется при одинаковом timestamp. Seed вставляет только отсутствующие IDs, существующие не перезаписывает. Начальные DML выполняются под APP, не под SYS.

SQL-файлы включают WHENEVER SQLERROR EXIT и отдельный EXIT; при ошибке остановитесь и посмотрите ORA-код. Для подключения вручную:

```bash
docker compose exec oracle sqlplus app/app@//localhost:1521/XEPDB1
```

В SQLPlus: `SELECT * FROM ORDERS ORDER BY ID;`, затем `EXIT;`. COMMIT/ROLLBACK всегда на отдельных строках после DML.

## 5. Далее — настройки коннекторов

Для полного демо сначала [CDC](CDC.md): включение ARCHIVELOG перезапускает Oracle. Затем [JDBC](JDBC.md), затем [custom](CUSTOM.md). Если показываете только JDBC/custom, настройки LogMiner и ARCHIVELOG не требуются.

## Завершение и возобновление

Stop Application в IDEA. В WSL:

```bash
docker compose stop connect-jdbc connect-cdc
docker compose stop oracle kafka
```

Данные сохраняются. Продолжить: `docker compose start oracle kafka`, дождаться healthy; `docker compose start connect-jdbc connect-cdc`; Run Application в IDEA. Повторная выдача прав и seed не нужны.

После `docker compose down` контейнеры удалены: вместо start требуется up -d. Не добавляйте -v для обычного завершения — это удаляет volumes. Файл custom offsets и Kafka/Oracle volumes должны принадлежать одному стенду.
