# Проверки упаковки для IDEA

Реализация SourceConnector/SourceTask сохранена из предыдущей версии. Добавлены корневой Maven aggregator и shared IDEA Application; разделены Oracle readers, SQL-подготовка и инструкции.

## Выполнено

- JDK 17, Maven 3.9.9: `mvn -B -f pom.xml clean verify` — оба проекта reactor SUCCESS, BUILD SUCCESS.
- 6 unit-тестов: 0 failures/errors/skipped. Проверены task configuration, plugin loading реальным Kafka Plugins, cursor recovery с TIMESTAMP(6)+ID, SourceRecord mapping/offsets, null-поля, ошибка corrupt offsets и сохранение/повторное чтение реальным FileOffsetBackingStore.
- Compose 2.29.7: config --quiet прошёл. Четыре сервиса, custom Docker service отсутствует.
- POM и Run Configuration XML, JSON connector configs — корректный синтаксис.
- 36 bash command blocks — bash -n прошёл; SQL-файлы, внутренние ссылки и раздельные credentials соответствуют проекту; .sh-файлов нет.

Логи: evidence/maven-verify.log, evidence/java-tests.txt, evidence/static-checks.txt.

## Ограничения

Docker daemon отсутствует в среде подготовки: сборка worker images, Oracle SQL, live JDBC/LogMiner захват не выполнялись для этой упаковки. Windows IDEA Application также не запускалась здесь; конфигурация подготовлена и проверена как XML, а в docs/CUSTOM.md есть ручной способ настройки.

Перед выступлением выполните README → docs/DEMO.md на вашем ПК. Именно этот live-прогон проверяет статус task, snapshot и реальные события. Unit-тесты не заменяют эту проверку.
