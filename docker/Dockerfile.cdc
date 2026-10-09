FROM confluentinc/cp-kafka-connect:7.7.1
USER root
RUN mkdir -p /opt/plugins && curl -fSL --retry 3 \
    https://repo.maven.apache.org/maven2/io/debezium/debezium-connector-oracle/2.7.3.Final/debezium-connector-oracle-2.7.3.Final-plugin.tar.gz \
    -o /tmp/oracle-plugin.tgz && tar -xzf /tmp/oracle-plugin.tgz -C /opt/plugins && rm /tmp/oracle-plugin.tgz
RUN find /opt/plugins -type f -name 'ojdbc*.jar' -delete && \
    curl -fSL --retry 3 https://repo.maven.apache.org/maven2/com/oracle/database/jdbc/ojdbc11/23.3.0.23.09/ojdbc11-23.3.0.23.09.jar \
    -o /opt/plugins/debezium-connector-oracle/ojdbc11.jar
USER 1000
