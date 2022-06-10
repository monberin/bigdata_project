#!/bin/bash
docker build -f Dockerfile.from_kafka -t from-kafka .
docker run --rm --network project-network -v --rm from-kafka --name batches-kafka