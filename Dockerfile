FROM maven:3-jdk-8

LABEL maintainer "Alexander Malic <alexander.malic@maastrichtuniversity.nl>"

ENV APP_DIR /app
ENV TMP_DIR /tmp/dqa

WORKDIR $TMP_DIR

COPY . .

# This can be removed once the latest version is on Maven Central:
RUN git clone https://github.com/trustyuri/trustyuri-java.git && \
    cd trustyuri-java && \
    mvn clean install -Dmaven.test.skip=true

# This can be removed once the latest version is on Maven Central:
RUN git clone https://github.com/Nanopublication/nanopub-java.git && \
    cd nanopub-java && \
    mvn clean install -Dmaven.test.skip=true

RUN mvn clean install && \
    mkdir $APP_DIR && \
    mv target/RdfSink-0.9.0.jar $APP_DIR/RdfSink.jar && \
    rm -rf $TMP_DIR

WORKDIR $APP_DIR

EXPOSE 80

ENTRYPOINT ["java","-jar","RdfSink.jar"]
