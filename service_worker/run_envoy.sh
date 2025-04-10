#!/bin/sh
docker pull envoyproxy/envoy:v1.33.0
docker stop Ingress
docker run --rm -p 0.0.0.0:10000:10000 -p 0.0.0.0:9001:9001 -v "$PWD":/etc/envoy --name Ingress -t envoyproxy/envoy:v1.33.0