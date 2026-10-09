# Debezium Oracle + LogMiner: настройки и права

Предусловие: Oracle/Kafka, APP и ORDERS уже созданы по [общей инструкции](ORACLE.md). Начните с этого варианта до запуска JDBC и custom: ARCHIVELOG может потребовать перезапуска БД.

## 1. Какие настройки Oracle нужны именно CDC

| Настройка | Где | Что даёт |
|---|---|---|
| ARCHIVELOG | CDB$ROOT, переключение в MOUNT | Архивирование redo logs |
| Archive destination | Каталог в Oracle volume + LOG_ARCHIVE_DEST_1 | Файлы для LogMiner и восстановления после паузы |
| Minimal supplemental logging | База/CDB$ROOT | Дополнительные сведения в redo |
| ALL-column supplemental logging | APP.ORDERS в XEPDB1 | Данные колонок для восстановления изменения |
| Common user и grants | CDB$ROOT, CONTAINER=ALL | Snapshot, переход CDB/PDB, LogMiner |

UPDATED_AT не является cursor CDC; позиция основана на SCN. Для выбранного LogMiner не включаем enable_goldengate_replication. Настройки logging вынесены из общей инструкции, потому что polling их не использует.

## 2. Проверить ARCHIVELOG и включить при необходимости

```bash
docker compose exec -T oracle sqlplus -s / as sysdba <<'SQL'
WHENEVER SQLERROR EXIT SQL.SQLCODE
SELECT LOG_MODE FROM V$DATABASE;
EXIT;
SQL
```

**Только если ответ NOARCHIVELOG**, выполните два следующих действия. Если ARCHIVELOG уже включён, их пропустите; существующий destination проверьте на этапе 5.

```bash
docker compose exec -u oracle oracle mkdir -p /opt/oracle/oradata/archive
```

```bash
docker compose exec -T oracle sqlplus -s / as sysdba < sql/cdc/01-archivelog.sql
```

Этот файл назначает LOG_ARCHIVE_DEST_1, делает SHUTDOWN/STARTUP MOUNT, включает ARCHIVELOG, открывает БД/PDB и сохраняет open state XEPDB1. Каталог находится на Oracle volume. Ожидайте Archive Mode в ARCHIVE LOG LIST.

Если JDBC/custom уже были запущены, остановите их перед изменением режима и возобновите после подготовки Oracle. На свежем демо они ещё не запущены.

## 3. Включить supplemental logging

```bash
docker compose exec -T oracle sqlplus -s / as sysdba < sql/cdc/02-logging.sql
```

Файл выполняет, если ещё не включено:

```sql
-- В корне CDB:
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
-- В XEPDB1:
ALTER TABLE APP.ORDERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

ALL-column logging добавляется только на нашу таблицу. Оно увеличивает объём redo. Эти команды нужны администратору при подготовке, не самому коннектору.

## 4. Пользователь и права Debezium

Откройте `sql/cdc/03-grants.sql`: это полный набор создания tablespaces, common user и grants для данной лаборатории.

```bash
docker compose exec -T oracle sqlplus -s / as sysdba < sql/cdc/03-grants.sql
```

Файл создаёт LOGMINER_TBS в ROOT и XEPDB1, а затем C##DBZUSER/dbz с CONTAINER=ALL. Существующие объекты/пароли не заменяет. Common user создаётся в ROOT, не как локальный пользователь PDB.

| Группа | Права |
|---|---|
| Соединение и переход CDB/PDB | CREATE SESSION, SET CONTAINER |
| Snapshot | SELECT ANY TABLE, FLASHBACK ANY TABLE, LOCK ANY TABLE |
| Каталог | SELECT_CATALOG_ROLE, EXECUTE_CATALOG_ROLE |
| Mining | LOGMINING, EXECUTE ON DBMS_LOGMNR/DBMS_LOGMNR_D |
| Транзакции | SELECT ANY TRANSACTION |
| Служебные возможности | CREATE TABLE, CREATE SEQUENCE, quota на LOGMINER_TBS |
| Сведения о БД/redo/archive/сеансе | SELECT на перечисленные ниже V_$ views |

Полный список конкретных views:

```text
V_$DATABASE
V_$LOG
V_$LOG_HISTORY
V_$LOGMNR_LOGS
V_$LOGMNR_CONTENTS
V_$LOGMNR_PARAMETERS
V_$LOGFILE
V_$ARCHIVED_LOG
V_$ARCHIVE_DEST_STATUS
V_$TRANSACTION
V_$MYSTAT
V_$STATNAME
```

Каждый GRANT в SQL имеет CONTAINER=ALL. C##DBZUSER не получает DBA/SYSDBA. Это широкий лабораторный набор для Debezium 2.7 initial snapshot + LogMiner; не все права используются постоянно во всех режимах. Права на создание служебных объектов и DBMS_LOGMNR_D сохраняем для дальнейших опытов; минимальный production-набор зависит от snapshot/mining/signaling конфигурации.

APP уже существует, отдельный polling reader для CDC не нужен. SYSDBA используется только для подготовки. Тестовые DML исполняйте под APP: изменения SYS/SYSTEM не являются корректным способом демонстрировать этот захват.

## 5. Проверить Oracle и доступ Debezium

```bash
docker compose exec -T oracle sqlplus -s / as sysdba < sql/cdc/04-check.sql
```

Ожидайте ARCHIVELOG, SUPPLEMENTAL_LOG_DATA_MIN не NO, VALID / LOCAL archive destination без ERROR, имена архивных файлов и ALL COLUMN LOGGING у APP.ORDERS. Этот check делает log switch и архивирование, чтобы проверить destination явно.

```bash
docker compose exec -T oracle sqlplus -s 'C##DBZUSER/dbz@//localhost:1521/XEPDB1' <<'SQL'
WHENEVER SQLERROR EXIT SQL.SQLCODE
SELECT COUNT(*) FROM APP.ORDERS;
EXIT;
SQL
```

Нет ORA-01017/ORA-00942. Если count отличается от 3 после предыдущего демо, это нормально.

## 6. Проверить конфиги и запустить worker

- `docker/Dockerfile.cdc`: Debezium Oracle 2.7.3.Final + один Oracle JDBC driver. Удаляет конфликтующие ojdbc jars перед добавлением закреплённого.
- `config/cdc.json`: CDB XE, PDB XEPDB1, table.include.list APP.ORDERS, LogMiner online_catalog, initial snapshot, topic.prefix cdc.
- `../docker-compose.yaml`: REST 8084 снаружи, служебные cdc-configs/cdc-offsets/cdc-status.

```bash
docker compose build connect-cdc
docker compose up -d connect-cdc
docker compose logs --tail=100 connect-cdc
curl -fsS http://localhost:8084/connector-plugins
```

Дождитесь REST; в plugins должен быть io.debezium.connector.oracle.OracleConnector.

```bash
curl -fsS -X PUT http://localhost:8084/connectors/oracle-cdc/config \
  -H 'Content-Type: application/json' --data-binary @config/cdc.json
curl -fsS http://localhost:8084/connectors/oracle-cdc/status
```

Ожидайте connector/task RUNNING. Логи показывают snapshot и переход к streaming. RUNNING ещё не доказывает завершение snapshot — проверьте seed в consumer.

## 7. Посмотреть snapshot

```bash
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic cdc.APP.ORDERS --from-beginning --property print.key=true --property print.offset=true
```

На первом старте без offsets ожидайте ID 1–3 с op=r. Value — envelope before/after/op/source. INSERT=c, UPDATE=u, DELETE=d; затем tombstone при tombstones.on.delete=true. Общий сценарий — [DEMO.md](DEMO.md).

## Сохранение позиции и журналы

cdc-offsets и cdc-schema-history нужно сохранять вместе с Kafka данными. Archive logs должны быть доступны на время максимального простоя/отставания connector. В лабораторном каталоге автоматическая очистка не настроена; контролируйте место на volume и не удаляйте журналы, нужные для сохранённого SCN. Если журнал утрачен, повторная выдача прав не восстановит его; может потребоваться новый snapshot с повторами текущих строк.

Production-параметры redo size, archive retention и UNDO_RETENTION зависят от нагрузки и длительности snapshot; фиксированные универсальные значения в демо не задаём.

Источник набора прав/настроек: [Debezium Oracle 2.7](https://debezium.io/documentation/reference/2.7/connectors/oracle.html).
