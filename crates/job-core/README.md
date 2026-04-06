# amp-job-core

This crate contains protobuf definitions for job event streaming. The generated Rust bindings are committed to the repository and only need to be regenerated when the `.proto` file changes.

## Protobuf Generation

The build script uses a configuration flag `gen_job_proto` that enables protobuf code generation via `prost-build`. When enabled, the build script compiles `proto/events.proto` into Rust types under `src/proto/`.

To regenerate protobuf bindings, run:

```bash
just gen-job-events-proto
```

Or using the full `cargo` command:

```bash
RUSTFLAGS="--cfg gen_job_proto" cargo check -p amp-job-core
```

This will regenerate `src/proto/amp.job.events.v1.rs` from `proto/events.proto`.
