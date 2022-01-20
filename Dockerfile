FROM maven:3-openjdk-17

ENV APP_DIR /app
ENV TMP_DIR /tmp/dqa

WORKDIR $TMP_DIR

COPY . .

RUN mvn install:install-file -DgroupId=virtuoso.rdf4j -DartifactId=virtuoso-rdf4j -Dversion=4 -Dpackaging=jar -Dfile=lib/virt_rdf4j.jar
RUN mvn install:install-file -DgroupId=virtuoso.rdf4j -DartifactId=virtuoso-jdbc -Dversion=4.3 -Dpackaging=jar -Dfile=lib/virtjdbc4_3.jar

RUN mvn clean install && \
    mkdir $APP_DIR && \
    mv target/RdfSink-0.9.0.jar $APP_DIR/RdfSink.jar && \
    rm -rf $TMP_DIR

WORKDIR $APP_DIR

EXPOSE 80

ENTRYPOINT ["java","-jar","RdfSink.jar"]
