FROM maven:3-jdk-8

LABEL maintainer "Alexander Malic <alexander.malic@maastrichtuniversity.nl>"

ENV APP_DIR /app
ENV TMP_DIR /tmp/dqa

WORKDIR $TMP_DIR

COPY . .

RUN mvn clean install && \
    mkdir $APP_DIR && \
    mv target/RdfUploadService-0.0.1-SNAPSHOT.jar $APP_DIR/RdfUploadService.jar && \
    rm -rf $TMP_DIR

WORKDIR $APP_DIR

EXPOSE 80

ENTRYPOINT ["java","-jar","RdfUploadService.jar"]
