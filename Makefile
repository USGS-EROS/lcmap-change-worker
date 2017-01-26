CONTAINERS=`docker ps -a -q`
IMAGES=`docker images -q`
WORKERIMAGE=lcmap-landsat:0.1.0-SNAPSHOT


docker-shell:
	docker run -it --entrypoint=/bin/bash usgseros/$(WORKERIMAGE)


