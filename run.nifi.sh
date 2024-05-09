#!/bin/sh

docker run --rm \
	-u 1000:1000 \
	-v /usr/src/nifi/user_data_volume/gh_archive/data:/usr/src/app/data \
	-v /usr/src/nifi/user_data_volume/gh_archive/output:/usr/src/app/output \
	-v /usr/src/nifi/user_data_volume/gh_archive/src/.env:/usr/src/app/src/.env \
	python312:devranker-gh