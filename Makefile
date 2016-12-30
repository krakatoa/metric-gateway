build:
	for pkg in $(shell go list ./cmd/...); do go build $$pkg; done

proto-deps:
	apt-get install protobuf-compiler; \
	go get -u github.com/golang/protobuf/proto; \
	go get -u github.com/golang/protobuf/protoc-gen-go

riemann_proto/riemann_proto.pb.go: riemann_proto/riemann_proto.proto
	protoc --go_out=. riemann_proto/riemann_proto.proto

proto: riemann_proto/riemann_proto.pb.go
