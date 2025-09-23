FROM eclipse-temurin:21-jdk-jammy

ENV MB_PLUGINS_DIR=/home/plugins/

ADD https://downloads.metabase.com/v0.54.9.x/metabase.jar /home
ADD https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/download/0.4.1/duckdb.metabase-driver.jar /home/plugins/

RUN chmod 744 /home/plugins/duckdb.metabase-driver.jar

CMD ["java", "-jar", "/home/metabase.jar"]