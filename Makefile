.PHONY: build run

BIN_FILENAME=ds_to_json

build:
	mkdir -p ${GOPATH}/bin
	go build -o ${GOPATH}/bin/${BIN_FILENAME} cmd/cli/main.go

install: build
	-sudo ln -s ${GOPATH}/bin/${BIN_FILENAME} /usr/local/bin