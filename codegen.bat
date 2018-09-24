packages\Grpc.Tools.1.12.0\tools\windows_x86\protoc.exe -Iprotos --csharp_out Grpc.Gcp/Grpc.Gcp protos/grpc_gcp.proto
packages\Grpc.Tools.1.12.0\tools\windows_x86\protoc.exe -Iprotos --grpc_out Grpc.Gcp/Grpc.Gcp.IntegrationTest --csharp_out Grpc.Gcp/Grpc.Gcp.IntegrationTest protos/test_service.proto --plugin=protoc-gen-grpc=packages\Grpc.Tools.1.12.0\tools\windows_x86\grpc_csharp_plugin.exe
