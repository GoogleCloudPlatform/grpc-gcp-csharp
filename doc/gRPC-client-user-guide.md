# gRPC Client User Guide

## Overview

Instructions for creating grpc client and make request to Google Cloud APIs.
This can be used to test the functionality of the gRPC calls to the Cloud
Services. For this instruction, we take Firestore API as an example.

## Prerequisites

### Install .NET Core SDK

Install system components

```sh
$ sudo apt-get update
$ sudo apt-get install curl libunwind8 gettext apt-transport-https
```

Register the trusted Microsoft Product key

```sh
$ curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > microsoft.gpg
$ sudo mv microsoft.gpg /etc/apt/trusted.gpg.d/microsoft.gpg
```

Register Microsoft Product feed

```sh
$ sudo sh -c 'echo "deb [arch=amd64] https://packages.microsoft.com/repos/microsoft-debian-stretch-prod stretch main" > /etc/apt/sources.list.d/dotnetdev.list'
```

Update the products available for installation, then install the .NET SDK

```sh
$ sudo apt-get update
$ sudo apt-get install dotnet-sdk-2.1.105
```

To test dotnet is installed successfully

```sh
$ dotnet --version
```

### Disable dotnet SDK telemetry

dotnet telemetry is enabled by default. To disable, add this line to your
~/.bashrc file (and restart the shell):

```sh
$ export DOTNET_CLI_TELEMETRY_OPTOUT=true
```

## Main Steps

### Setup C# project for Cloud API

Create a root directory to hold the projects we are going to create. For example
we use a folder called grpc-client-test:

```sh
$ mkdir ~/grpc-client-test
$ cd ~/grpc-client-test
```

Create C# project for Firestore API:

```sh
$ dotnet new console -o Google.Cloud.Firestore.V1Beta1
```

Install grpc and protobuf packages:

```sh
$ dotnet add Google.Cloud.Firestore.V1Beta1 package Grpc
$ dotnet add Google.Cloud.Firestore.V1Beta1 package Grpc.Tools
$ dotnet add Google.Cloud.Firestore.V1Beta1 package Google.Protobuf
$ dotnet add Google.Cloud.Firestore.V1Beta1 package Google.Protobuf.Tools
$ dotnet add Google.Cloud.Firestore.V1Beta1 package Google.Api.Gax.Grpc
```

Alternatively, we can manually add the dependencies to the
Google.Cloud.Firestore.V1Beta1.csproj file, and then use `dotnet restore` to
install those packages:

```csproj
<Project Sdk="Microsoft.NET.Sdk">

  ...

  <ItemGroup>
    <PackageReference Include="Google.Api.Gax.Grpc" Version="2.3.0" />
    <PackageReference Include="Google.Protobuf" Version="3.5.1" />
    <PackageReference Include="Google.Protobuf.Tools" Version="3.5.1" />
    <PackageReference Include="Grpc" Version="1.11.0" />
    <PackageReference Include="Grpc.Tools" Version="1.11.0" />
  </ItemGroup>

</Project>
```

These packages will be installed to the default location for global-packages. To
check the default locations:

```sh
$ dotnet nuget locals all --list
```

### Generate grpc C# code for Google Cloud API

Download .proto file from [googleapis](https://github.com/googleapis/googleapis)

```sh
$ git clone https://github.com/googleapis/googleapis.git
```

Use protoc from Grpc.Tools to generate grpc client code based on the proto file
we have just downloaded. You will need to specify not only the .proto file that
you want to code-generate, but also the dependency proto libraries they share.
For example, if foo.proto has dependency on bar.proto, you need to specify both
foo.proto and bar.proto in the code generation command.

In this example, we only need the .proto files under googleapis/google/firestore/v1beta1/

```sh
# Setup environment variables.
PROTOC=$HOME/.nuget/packages/grpc.tools/1.11.0/tools/linux_x64/protoc
GRPC_PLUGIN=$HOME/.nuget/packages/grpc.tools/1.11.0/tools/linux_x64/grpc_csharp_plugin
PROTOBUF_TOOLS_DIR=$HOME/.nuget/packages/google.protobuf.tools/3.5.1/tools/
OUT_DIR=Google.Cloud.Firestore.V1Beta1/

# Generate csharp code.
$PROTOC -I googleapis/ -I $PROTOBUF_TOOLS_DIR \
  --csharp_out $OUT_DIR \
  --grpc_out $OUT_DIR \
  --plugin=protoc-gen-grpc=$GRPC_PLUGIN \
  googleapis/google/firestore/v1beta1/*proto
```

### Create C# project for gRPC client

First we need to create a C# project for the test client:

```sh
$ dotnet new console -o Google.Cloud.Firestore.V1Beta1.Test
```

In
Google.Cloud.Firestore.V1Beta1.Test/Google.Cloud.Firestore.V1Beta1.Test.csproj
add a ProjectReference to ItemGroup to include the API project we created
before.

```csproj
<Project Sdk="Microsoft.NET.Sdk">

  ...

  <ItemGroup>
    <ProjectReference Include="..\Google.Cloud.Firestore.V1Beta1\Google.Cloud.Firestore.V1Beta1.csproj" />
  </ItemGroup>
</Project>
```

In Google.Cloud.Firestore.V1Beta1.Test/Program.cs, write client code to make
grpc calls to Firestore API.

```cs
using System;
using Google.Cloud.Firestore.V1Beta1;
using sco = System.Collections.ObjectModel;
using gaxgrpc = Google.Api.Gax.Grpc;
using grpccore = Grpc.Core;

namespace Google.Cloud.Firestore.V1Beta1.Test
{
    class Program
    {
        static void Main(string[] args)
        {
            // Create a channel pool with scopes.
            string[] scopes = new string[] {
                "https://www.googleapis.com/auth/cloud-platform",
            };
            gaxgrpc::ChannelPool channelPool = new gaxgrpc::ChannelPool(scopes);

            // Get a channel from channel pool for a specified endpoint.
            gaxgrpc::ServiceEndpoint endpoint = new gaxgrpc::ServiceEndpoint(
                "firestore.googleapis.com", 443);
            grpccore::Channel channel = ChannelPool.GetChannel(endpoint);

            // Initialize a grpc client of firestore using the channel just created.
            Firestore.FirestoreClient grpcClient = new Firestore.FirestoreClient(channel);

            // Make a ListDocumentsRequest to firestore API and check the response.
            ListDocumentsRequest request = new ListDocumentsRequest{
                Parent = "projects/<project-id>/databases/(default)/documents"
            };
            ListDocumentsResponse response = grpcClient.ListDocuments(request);
            Console.WriteLine(response);
        }
    }
}
```

### Authentication

If running on Google Cloud Platform (GCP) then authentication is already setup.
Otherwise, download a service account key file from your GCP project. See
[Getting Started With Authentication](https://cloud.google.com/docs/authentication/getting-started) for more details.

After you downloaded the JSON key file, set the following environment variable
to refer to the file:

```sh
$ export GOOGLE_APPLICATION_CREDENTIALS=path/to/key.json
```

### Build and run application

```sh
$ dotnet build Google.Cloud.Firestore.V1Beta1.Test
$ dotnet run -p Google.Cloud.Firestore.V1Beta1.Test
```

