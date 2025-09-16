module github.com/data-power-io/noesis-connectors/connectors/postgres

go 1.24.0

require (
	github.com/data-power-io/noesis-connectors/sdks/go v0.0.0
	github.com/data-power-io/noesis-protocol/languages/go v1.0.0
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.7.1
	go.uber.org/zap v1.27.0
	google.golang.org/grpc v1.71.0
)

require (
	github.com/apache/arrow/go/v18 v18.0.0-20241007013041-ab95a4d25142 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/google/flatbuffers v24.3.25+incompatible // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.41.0 // indirect
	golang.org/x/exp v0.0.0-20240222234643-814bf88cf225 // indirect
	golang.org/x/mod v0.26.0 // indirect
	golang.org/x/net v0.43.0 // indirect
	golang.org/x/sync v0.16.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	golang.org/x/text v0.28.0 // indirect
	golang.org/x/tools v0.35.0 // indirect
	golang.org/x/xerrors v0.0.0-20231012003039-104605ab7028 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250826171959-ef028d996bc1 // indirect
	google.golang.org/protobuf v1.36.8 // indirect
)

// Use local development versions
replace github.com/data-power-io/noesis-connectors/sdks/go => ../../sdks/go

replace github.com/data-power-io/noesis-protocol/languages/go => ../../../noesis-protocol/languages/go
