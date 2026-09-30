.PHONY: proto

# Plugins installed with `go install` live in GOPATH/bin.
# protoc may be there or elsewhere on PATH (for example after `brew install protobuf`).
# Invoke it by path: Apple's GNU Make 3.81 does not search a PATH set in this file.
GOBIN := $(shell go env GOPATH)/bin
PROTOC := $(shell PATH="$(GOBIN):$$PATH" command -v protoc)
export PATH := $(GOBIN):$(PATH)

proto:
	$(PROTOC) \
		--proto_path=proto \
		--go_out=. --go_opt=module=temperature-sensors \
		--go-grpc_out=. --go-grpc_opt=module=temperature-sensors \
		proto/temperature/v1/temperature.proto
