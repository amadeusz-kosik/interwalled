.PHONY: clean run-docker-cluster

clean:
	sbt clean

run-docker-cluster:
	docker compose -f docker/docker-compose.yaml up spark-master -d
	docker compose -f docker/docker-compose.yaml up spark-worker -d
	docker compose -f docker/docker-compose.yaml up spark-history-server -d

test-ailist:
	sbt ailist/test