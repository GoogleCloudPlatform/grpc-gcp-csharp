# gRPC for GCP extensions (C#)

Copyright 2018
[The gRPC Authors](https://github.com/grpc/grpc/blob/master/AUTHORS)

## About This Repository

This repo is created to support GCP specific extensions for gRPC. To use the extension features, please refer to [Grpc.Gcp](Grpc.Gcp).

This repo also contains supporting infrastructures such as end2end tests and benchmarks for accessing cloud APIs with gRPC client libraries.

## Testing

### Authentication

Integration tests requires Google Cloud Platform credentials. See [Getting
Started With
Authentication](https://cloud.google.com/docs/authentication/getting-started).

```sh
$ export GOOGLE_APPLICATION_CREDENTIALS=path/to/key.json
```

### Run Tests

Grpc.Gcp.IntegrationTest can be built for .NET Core or .NET Framework.

If using Visual Studio 2017, open Grpc.Gcp.sln, and the tests will be loaded automatically under test explorer.

For UNIX, use [dotnet
cli](https://docs.microsoft.com/en-us/dotnet/core/tools/?tabs=netcore2x) to
build and run tests.

```sh
$ cd Grpc.Gcp/Grpc.Gcp.IntegrationTest
$ dotnet build
$ dotnet test
```
