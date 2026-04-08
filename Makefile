.PHONY: clean

clean:
	sbt clean

run-benchmark-sequila: docker/template/docker-compose.yaml.j2
	echo "Create docker compose file."
	sed 's/{{ spark_version }}/3.4.3-scala2.12-java11-r-ubuntu/g' ./docker/template/docker-compose.yaml.j2 > ./docker/docker-compose-sequila.yaml
	echo "Start Apache Spark cluster."
	docker compose -f docker/docker-compose-sequila.yaml up spark-master -d
	docker compose -f docker/docker-compose-sequila.yaml up spark-worker -d
	docker compose -f docker/docker-compose-sequila.yaml up spark-history-server -d
	echo "Run Sequila job."
	sbt benchmarkSequila/assembly
	export JAR_FILE_PATH="/mnt/jar/benchmark-sequila/benchmark-sequila.jar"	&& \
	export CLASS_NAME="me.kosik.interwalled.benchmark.sequila.Main"    		&& \
	export IW_DRIVER_MEMORY="4G"											&& \
	export IW_EXECUTOR_CORES="4"											&& \
	export IW_TOTAL_EXECUTOR_CORES="20"										&& \
	export IW_EXECUTOR_MEMORY="4G"											&& \
	docker compose -f docker/docker-compose-sequila.yaml up spark-driver

test-ailist:
	sbt ailist/test