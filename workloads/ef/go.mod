module tfaas/executor

go 1.18

replace tfaas/golang => ./CacheClient/golang

replace github.com/lechou-0/Client => ./CacheClient

replace github.com/lechou-0/common => ./CacheClient/common

require (
	github.com/lechou-0/Client v0.0.0-00010101000000-000000000000
	tfaas/golang v0.0.0-00010101000000-000000000000
)

require (
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/lechou-0/common v0.0.0-00010101000000-000000000000 // indirect
	github.com/pebbe/zmq4 v1.2.9 // indirect
	go.etcd.io/etcd/api/v3 v3.5.9 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.9 // indirect
	go.etcd.io/etcd/client/v3 v3.5.9 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.17.0 // indirect
	golang.org/x/net v0.0.0-20211112202133-69e39bad7dc2 // indirect
	golang.org/x/sys v0.0.0-20210615035016-665e8c7367d1 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20210602131652-f16073e35f0c // indirect
	google.golang.org/grpc v1.50.1 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
)
