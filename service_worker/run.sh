#!/bin/bash

docker run -it --rm -p 443:443 -v $(pwd)/default.conf:/etc/nginx/conf.d/default.conf -v $(pwd)/frontend:/usr/share/nginx/html \
	-v $(pwd)/certificate.crt:/etc/nginx/conf.d/certificate.crt -v $(pwd)/private.key:/etc/nginx/conf.d/private.key --name web nginx 