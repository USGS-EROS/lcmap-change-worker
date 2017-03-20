CONTAINERS=`docker ps -a -q`
IMAGES=`docker images -q`

# pull the tag from version.py
TAG=0.1.0
WORKERIMAGE=lcmap-pyccd-worker:$(TAG)

CON_NAME=container.fu
get-mesos-lib:
	docker run -it --name $(CON_NAME) mesosphere/mesos:1.1.0-1.0.104.rc3.ubuntu1404 echo '$(CON_NAME)'
	docker cp `docker ps -aq --filter "name=$(CON_NAME)"`:/usr/lib/libmesos-1.1.0.so .
	docker rm `docker ps -aq --filter "name=$(CON_NAME)"`

docker-build: get-mesos-lib
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

clean-venv:
	@rm -rf .venv

clean:
	@rm -rf dist build lcmap_pyccd_worker.egg-info
	@find . -name '*.pyc' -delete
	@find . -name '__pycache__' -delete
