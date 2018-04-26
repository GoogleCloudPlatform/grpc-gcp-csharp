gRPC Firestore Example (C#)
===========================

This is an example of utilizing Cloud Firestore using gRPC in C#. 

BACKGROUND
-------------
For this sample, it is assumed the C# code for access to Firestore from the gRPC proto files has already been done. 

PREREQUISITES
-------------

- Mac OS X: Visual Studio for Mac Community 

BUILD
-------

Download the source files from github.

# Using Visual Studio

* Create a project
* Add the source files
* Add the C# generated files from the gRPC Firestore proto files
* Add the following packages:
  - Google.Apis
  - Google.Apis.Auth
  - Google.Apis.Core
  - Google.Apis.Storage.v1
  - Google.Cloud.Datastore.V1
  - Google.Cloud.DevTools.Common
  - Google.Cloud.Storage.V1
  - Google.LongRunning
  - Google.Protobuf
  - Grpc
  - Grpc.Auth
  - Grpc.Core
  - Grpc.Tools

* Build the project

Try it!
-------

* Add the environment varable GOOGLE_APPLICATION_CREDENTIALS to the Project options pointing to your Application Default Credentials (ADC) file generated from the GCP Console.  
More info is here: https://cloud.google.com/docs/authentication/getting-started

* Run the application from the Visual Studio IDE or from the command line with:
```
export GOOGLE_APPLICATION_CREDENTIALS="<location of donladed json file>"
mono <project location>/bin/Debug/<project name>.exe
```
