CONTAINERS=`docker ps -a -q`
IMAGES=`docker images -q`

# pull the tag from version.py
TAG=0.1.0
WORKERIMAGE=lcmap-change-worker:$(TAG)

docker-build:
	docker build -t $(WORKERIMAGE) $(PWD)

docker-shell:
	docker run -it --entrypoint=/bin/bash usgseros/$(WORKERIMAGE)

docker-deps-up:
	docker-compose -f resources/docker-compose.yml up -d

docker-deps-up-nodaemon:
	docker-compose -f resources/docker-compose.yml up

docker-deps-down:
	docker-compose -f resources/docker-compose.yml down

deploy-pypi:

deploy-dockerhub:

clean:
	@rm -rf dist build lcmap_change_worker.egg-info __pycache__ *.pyc
