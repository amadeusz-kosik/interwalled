.PHONY: clean
clean:
	sbt clean

test-ailist:
	sbt ailist/test

include make/Makefile.sequila
