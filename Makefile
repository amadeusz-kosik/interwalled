.PHONY: clean
clean:
	sbt clean

test-ailist:
	sbt ailist/test

include make/Makefile.interwalled
include make/Makefile.sequila

# AIListCore benchmarks with JMH

.PHONY: build-ailist-core-benchmark
build-ailist-core-benchmark:
	sbt 'ailistCoreBenchmark/clean'
	sbt 'ailistCoreBenchmark/Jmh/compile'

.PHONY: build-ailist-core-benchmark
run-ailist-core-benchmark:
	sbt 'ailistCoreBenchmark/Jmh/run'
