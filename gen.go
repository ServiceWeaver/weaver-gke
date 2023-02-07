package gen

//go:generate ./dev/protoc.sh internal/gke/container.proto
//go:generate ./dev/protoc.sh internal/clients/babysitter.proto
//go:generate ./dev/protoc.sh internal/config/config.proto
//go:generate ./dev/protoc.sh internal/nanny/nanny.proto
//go:generate ./dev/protoc.sh internal/nanny/controller/controller.proto
//go:generate ./dev/protoc.sh internal/nanny/distributor/distributor.proto
//go:generate ./dev/protoc.sh internal/nanny/assigner/assigner.proto
//go:generate ./dev/protoc.sh internal/store/store.proto
//go:generate ./dev/protoc.sh internal/local/local.proto
//go:generate ./dev/protoc.sh internal/local/proxy/proxy.proto
