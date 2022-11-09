FROM docker.io/bitnami/spark:3.3.1

USER root

RUN apt-get update && apt-get install liblz4-1 && apt-get install liblzf1 && apt-get install libzstd1

USER 1001

# Add Incite dependencies
ADD --chown=1001 incite-ignite/target/incite-ignite-0.0.1-SNAPSHOT.jar /opt/bitnami/spark/jars/incite-ignite-0.0.1-SNAPSHOT.jar
ADD --chown=1001 incite-service/target/incite-service-0.0.1-SNAPSHOT.jar /opt/bitnami/spark/jars/incite-service-0.0.1-SNAPSHOT.jar

# Add JDBC drivers
ADD --chown=1001 https://repo1.maven.org/maven2/org/apache/ignite/ignite-core/2.14.0/ignite-core-2.14.0.jar /opt/bitnami/spark/jars/ignite-core-2.14.0.jar
ADD --chown=1001 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.0/postgresql-42.5.0.jar /opt/bitnami/spark/jars/postgresql-42.5.0.jar

# Add Apache Spark dependencies
ADD --chown=1001 https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar
ADD --chown=1001 https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.1.2/kafka-clients-3.1.2.jar /opt/bitnami/spark/jars/kafka-clients-3.1.2.jar
ADD --chown=1001 https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.0/spark-sql-kafka-0-10_2.12-3.3.0.jar /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.3.0.jar
ADD --chown=1001 https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.0/spark-token-provider-kafka-0-10_2.12-3.3.0.jar /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.3.0.jar

# Add Spring Framework dependencies
ADD --chown=1001 https://repo1.maven.org/maven2/org/springframework/spring-core/5.3.23/spring-core-5.3.23.jar /opt/bitnami/spark/jars/spring-core-5.3.23.jar
ADD --chown=1001 https://repo1.maven.org/maven2/org/springframework/spring-beans/5.3.23/spring-beans-5.3.23.jar /opt/bitnami/spark/jars/spring-beans-5.3.23.jar
ADD --chown=1001 https://repo1.maven.org/maven2/org/springframework/spring-jcl/5.3.23/spring-jcl-5.3.23.jar /opt/bitnami/spark/jars/spring-jcl-5.3.23.jar
ADD --chown=1001 https://repo1.maven.org/maven2/org/springframework/spring-jdbc/5.3.23/spring-jdbc-5.3.23.jar /opt/bitnami/spark/jars/spring-jdbc-5.3.23.jar
ADD --chown=1001 https://repo1.maven.org/maven2/org/springframework/spring-tx/5.3.23/spring-tx-5.3.23.jar /opt/bitnami/spark/jars/spring-tx-5.3.23.jar

RUN chmod 664 /opt/bitnami/spark/jars/commons-pool2-*.jar
RUN chmod 664 /opt/bitnami/spark/jars/kafka-clients-*.jar
RUN chmod 664 /opt/bitnami/spark/jars/incite-*.jar
RUN chmod 664 /opt/bitnami/spark/jars/ignite-*.jar
RUN chmod 664 /opt/bitnami/spark/jars/postgresql-*.jar
RUN chmod 664 /opt/bitnami/spark/jars/spark-sql-kafka-*.jar
RUN chmod 664 /opt/bitnami/spark/jars/spark-token-provider-kafka-*.jar
RUN chmod 664 /opt/bitnami/spark/jars/spring-*.jar

WORKDIR /opt/bitnami/spark
ENTRYPOINT [ "/opt/bitnami/scripts/spark/entrypoint.sh" ]

CMD /opt/bitnami/spark/sbin/start-master.sh | /opt/bitnami/spark/sbin/start-worker.sh spark://$(hostname):7077