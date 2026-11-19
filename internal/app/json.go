package app

import "google.golang.org/protobuf/encoding/protojson"

// protojsonMarshal keeps proto field names (snake_case), matching the .proto
// file and grpcurl output, and emits zero values so clients see every field.
func protojsonMarshal() protojson.MarshalOptions {
	return protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}
}
