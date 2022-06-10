#!/bin/bash

docker run --name cassandra-server --network project-network -p 9042:9042 -d cassandra:latest
sleep 80
docker cp ./ddl-script.cql cassandra-server:/
docker exec -it cassandra-server cqlsh -f ddl-script.cql
