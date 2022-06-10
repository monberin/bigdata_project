#!/bin/bash
docker build -f Dockerfile.stream_reading -t reading-stream .
docker run --rm --network project-network --rm reading-stream --name reading-stream