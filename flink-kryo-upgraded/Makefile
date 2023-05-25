.PHONY: clean
clean:
	rm -rf .idea .gradle build out

.PHONY: build
build:
	gradle --warning-mode all installDist

.PHONY: run
run: build
	 ./build/install/flink-kryo-upgraded/bin/flink-kryo-upgraded
