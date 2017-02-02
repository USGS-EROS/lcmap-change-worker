CONTAINERS=`docker ps -a -q`
IMAGES=`docker images -q`
TAG=0.1.0
WORKERIMAGE=lcmap-change-worker:$(TAG)

docker-build:
	docker build -t $(WORKERIMAGE) $(PWD) 

docker-shell:
	docker run -it --entrypoint=/bin/bash usgseros/$(WORKERIMAGE)


