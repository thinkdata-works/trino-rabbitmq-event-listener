FROM gradle:8.6.0-jdk17
WORKDIR /trino-rabbitmq-event-listener
COPY . /trino-rabbitmq-event-listener

RUN ./gradlew shadowJar