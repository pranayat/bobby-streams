FROM ubuntu
ENV DEBIAN_FRONTEND=noninteractive

# Install required packages

RUN apt-get update && \
    apt-get install -y build-essential && \
    apt-get install -y software-properties-common && \
    apt-get -y install wget

RUN apt-get install python-is-python3

# Install Java.
RUN apt-get install -y openjdk-11-jdk
# RUN apt-get install -y openjdk-8-jdk

# Install Storm.
ENV STORM_USER=pranaya \
    STORM_CONF_DIR=/conf \
    STORM_DATA_DIR=/data \
    STORM_LOG_DIR=/logs

# Add a user and make dirs
RUN set -ex; \
    adduser "$STORM_USER"; \
    mkdir -p "$STORM_CONF_DIR" "$STORM_DATA_DIR" "$STORM_LOG_DIR"; \
    chown -R "$STORM_USER:$STORM_USER" "$STORM_CONF_DIR" "$STORM_DATA_DIR" "$STORM_LOG_DIR"``

ARG DISTRO_NAME=apache-storm-2.5.0

# Download Apache Storm.
RUN set -ex;
RUN wget -q "http://archive.apache.org/dist/storm/$DISTRO_NAME/$DISTRO_NAME.tar.gz";
RUN export GNUPGHOME="$(mktemp -d)";
RUN tar -xzf "$DISTRO_NAME.tar.gz";
RUN chown -R "$STORM_USER:$STORM_USER" "$DISTRO_NAME";
RUN rm "$DISTRO_NAME.tar.gz";

ENV PATH $PATH:/$DISTRO_NAME/bin

#ADD storm.yaml /conf

RUN chown -R "$STORM_USER" "$STORM_CONF_DIR" "$STORM_DATA_DIR" "$STORM_LOG_DIR"


# Install Maven.

ENV MAVEN_VERSION="3.8.8"

RUN cd / && \
    wget "https://downloads.apache.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz" && \
    tar -zxvf "apache-maven-$MAVEN_VERSION-bin.tar.gz"
ENV PATH $PATH:/apache-maven-$MAVEN_VERSION/bin

# RUN mkdir /storm-nimbus
WORKDIR /storm-nimbus

ENTRYPOINT ["/bin/bash", "startup.sh"]
