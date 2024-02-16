FROM gradle:8.6.0-jdk17
WORKDIR /trino-event-listener
COPY . /trino-event-listener
RUN ./gradlew shadowJar