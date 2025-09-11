.PHONY: all
all: build

#LDFLAGS += -extldflags '-static' -w

.PHONY: build
build: trialpb/trial.pb.go
	go build -ldflags "${LDFLAGS}"

trialpb/trial.pb.go: trialpb/trial.proto
	cd trialpb && \
		protoc --go_out=. --go_opt=paths=source_relative  trial.proto

