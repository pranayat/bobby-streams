# only used with docker-compose.debug.yml

FROM storm

ENV MAVEN_VERSION="3.8.8"

RUN cd / && \
    wget "https://downloads.apache.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz" && \
    tar -zxvf "apache-maven-$MAVEN_VERSION-bin.tar.gz"
ENV PATH $PATH:/apache-maven-$MAVEN_VERSION/bin

WORKDIR /storm-nimbus
