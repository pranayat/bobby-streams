#!/bin/bash
echo "STARTING UP IN LOCAL MODE"
echo "mvn package to build jar"
echo "storm local target/aqp-2.5.0.jar aqp.AqpTopology to start local topology"
# could also do the above to in the script here, but doing it manually as we don't have to build everytime
set -x
sleep infinity # so container won't exit