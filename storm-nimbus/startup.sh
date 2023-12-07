#!/bin/bash
echo "STARTING UP IN LOCAL MODE"
set -x
storm nimbus &
storm dev-zookeeper &
storm ui