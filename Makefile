.PHONY: test
test:
	go test

.PHONY: clean
clean:
	rm -f ./tmp/*.idx ./tmp/*.dat

.PHONY: fmt
fmt:
	go fmt
