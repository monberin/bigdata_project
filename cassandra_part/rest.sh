#!/bin/bash
docker build -f Dockerfile.app -t rest_api .
docker run --rm --network project-network -v --rm rest_api --name rest