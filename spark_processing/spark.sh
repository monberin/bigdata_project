#!/bin/bash

docker build -f Dockerfile -t sparkp .
docker run --network project-network --rm sparkp
