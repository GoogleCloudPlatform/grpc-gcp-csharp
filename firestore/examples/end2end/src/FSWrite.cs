using System;
using System.Collections.Generic;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using Google.Apis.Services;
using Google.Type;
using Google.Cloud.Firestore.Admin.V1Beta1;
using Google.Cloud.Firestore.V1Beta1;
using Grpc.Auth;
using Google.Protobuf.Collections;
using System.Threading.Tasks;

namespace FirestoreTest
{
    public partial class FirestoreTestClass
    {

        public async Task FSWrite()
        {
            try
            {
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\n:: Streaming Writes to a Document ::\n");
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "\nEnter Document Id: ");
                String docId = Console.ReadLine();
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\nStreaming writes to ");
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, docId);
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "...\n");

                Document updatedDoc = GetDocument(docId);
                MapField<string, Value> fields;
                if (updatedDoc == null)
                {
                    updatedDoc = new Document();
                    updatedDoc.Name = Parent + "/documents/GrpcTestData/" + docId;
                    fields = new MapField<string, Value>();
                }
                else
                {
                    FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "\nAvailable fields in this document to update:\n");
                    Utils.ProcessFields(updatedDoc);
                    fields = updatedDoc.Fields;
                }

                String streamId = "";
                Google.Protobuf.ByteString streamToken = null;

                using (var writeCall = FsClient.Write())
                {
                    var writeRequest = new WriteRequest();
                    writeRequest.Database = Parent;
                    await writeCall.RequestStream.WriteAsync(writeRequest);
                    await writeCall.ResponseStream.MoveNext();
                    var responseStr = writeCall.ResponseStream.Current;
                    streamId = responseStr.StreamId;
                    streamToken = responseStr.StreamToken;

                    var responseReaderTask = Task.Run(async () =>
                    {
                        while (await writeCall.ResponseStream.MoveNext())
                        {
                            var response = writeCall.ResponseStream.Current;
                            var writeResults = response.WriteResults;
                            foreach (var writeResult in writeResults)
                            {
                                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "\nWrite result:" + writeResult.ToString());
                            }
                        }
                    });

                    var currentWrite = new Write();
                    var docMask = new DocumentMask();
                    // update/create fields
                    while (true)
                    {
                        FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "\nEnter Field Name (blank when finished): ");
                        string fieldName = Console.ReadLine();
                        if (fieldName == "")
                        {
                            break;
                        }
                        else
                        {
                            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "\nEnter Field Value: ");
                            var fieldInValue = Console.ReadLine();
                            Utils.AddField(fields, fieldName, fieldInValue);
                            docMask.FieldPaths.Add(fieldName);
                        }
                    }
                    writeRequest.Database = Parent;
                    currentWrite.Update = updatedDoc;
                    currentWrite.UpdateMask = docMask;
                    writeRequest.StreamId = streamId;
                    writeRequest.StreamToken = streamToken;
                    writeRequest.Writes.Add(currentWrite);
                    await writeCall.RequestStream.WriteAsync(writeRequest);
                    await writeCall.RequestStream.CompleteAsync();
                    await responseReaderTask;
                }
            }
            catch (Grpc.Core.RpcException e)
            {
                Console.WriteLine("RPC failed", e);
                throw;
            }
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\nSuccessfully streamed updates to the document!\n");
        }
    }
}
