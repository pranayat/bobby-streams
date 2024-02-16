#!/bin/bash

cd /

# Download Apache Maven for some reason nimbus leader election fails if I do this in the dockerfile
MAVEN_VERSION="3.8.8"
wget "https://downloads.apache.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"

tar -zxvf "apache-maven-$MAVEN_VERSION-bin.tar.gz"

# this doesn't seem to be working
PATH=$PATH:/apache-maven-$MAVEN_VERSION/bin; export PATH

storm nimbus & storm ui