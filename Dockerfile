FROM gradle:8.9-jdk22
WORKDIR /trino-rabbitmq-event-listener
COPY . /trino-rabbitmq-event-listener

RUN ./gradlew clean shadowJar