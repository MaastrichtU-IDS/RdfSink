FROM maven:3-jdk-8

LABEL maintainer "Alexander Malic <alexander.malic@maastrichtuniversity.nl>"

ENV APP_DIR /app
ENV TMP_DIR /tmp/dqa

WORKDIR $TMP_DIR

COPY . .

RUN mvn clean install && \
    mkdir $APP_DIR && \
    mv target/RdfSink-0.9.0.jar $APP_DIR/RdfSink.jar && \
    rm -rf $TMP_DIR

WORKDIR $APP_DIR

EXPOSE 80

ENTRYPOINT ["java","-jar","RdfSink.jar"]
