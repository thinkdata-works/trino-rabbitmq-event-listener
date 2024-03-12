FROM gradle:8.6-jdk21
WORKDIR /trino-rabbitmq-event-listener
COPY . /trino-rabbitmq-event-listener

RUN ./gradlew clean shadowJar