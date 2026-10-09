# Oracle Capture Demo — проект IDEA

Один Oracle, одна APP.ORDERS, один Kafka и три подхода одновременно. Oracle/Kafka/JDBC/Debezium работают в Docker. Наш Kafka Connect SourceConnector/SourceTask запускается **только из IDEA**.

Все действия ручные: прямые docker/curl/SQLPlus команды, без shell-скриптов. SQL-файлы оставлены для однообразной подготовки и отдельных действий демо; прочитайте их перед выполнением. Именованные Oracle/Kafka volumes сохраняют данные.

## Порядок первого запуска и демо

1. [Общая инфраструктура и БД](docs/ORACLE.md): открыть проект, поднять Oracle/Kafka, создать APP.ORDERS и seed.
2. [Debezium / LogMiner](docs/CDC.md): подготовить журналы и выдать права, запустить CDC. **Этот этап идёт первым**, поскольку ARCHIVELOG может потребовать перезапуска Oracle.
3. [Confluent JDBC](docs/JDBC.md): отдельный пользователь и права, запуск worker, регистрация.
4. [Custom из IDEA](docs/CUSTOM.md): права, запуск готового Application, краткие отличия от JDBC.
5. [Сценарий выступления](docs/DEMO.md): consumers, INSERT, UPDATE, UPDATE без timestamp, ROLLBACK, DELETE и перезапуск custom.

[Полный список файлов и назначение](docs/FILES.md). [Проверки и ограничения](VERIFICATION.md).

## Открыть в IDEA

Распакуйте ZIP в `C:\Users\VEIIEV\IdeaProjects`: он содержит папку `oracle-capture-demo`. Откройте **корневой** `pom.xml` как Maven project. Project SDK и Maven JDK = 17. Reload Maven Projects. В списке Run появится `Oracle Custom Connect (IDEA only)` из `.run`.

Работайте с одной копией проекта. Docker-команды выполняются из WSL в `/mnt/c/Users/VEIIEV/IdeaProjects/oracle-capture-demo`; IDEA использует ту же папку через Windows-путь. Отдельно копировать custom в другую папку не нужно.

| Компонент | Доступ с ПК | Учётная запись Oracle | Топик |
|---|---|---|---|
| Oracle XE 21.3 | localhost:1521/XEPDB1 | APP/app для тестовых DML | — |
| Kafka, CP 7.7.1 | localhost:9092 | — | — |
| JDBC Source 10.8.0 | REST localhost:8083 | KAFKA_JDBC/jdbc_reader | jdbc.ORDERS |
| Debezium 2.7.3.Final | REST localhost:8084 | C##DBZUSER/dbz | cdc.APP.ORDERS |
| Custom Connect 3.7.1, JDK 17 | REST localhost:8085, Windows | KAFKA_CUSTOM/custom_reader | custom.connect.orders |

Compose-проект называется `oracle-capture-demo`. Он создаёт новые volumes и не использует данные старого `oracle-capture-lab`. Старые стенды нужно остановить, если они занимают порты 1521, 9092, 8083, 8084. Не выполняйте down -v для обычного завершения демо.

## Что обязательно подготовить до выступления

Загрузите Docker images, соберите оба worker и Maven-модуль, пройдите начальную загрузку и пробный прогон сценария. На выступлении не планируйте ждать скачивания образов/зависимостей. Для второго прогона выберите новые свободные IDs в `sql/demo` — сценарий объясняет как; историю и offsets без необходимости не сбрасывайте.
