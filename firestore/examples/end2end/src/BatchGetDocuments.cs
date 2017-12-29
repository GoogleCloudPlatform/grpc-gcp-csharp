using System;
using System.Collections;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using Google.Apis.Services;
using Google.Type;
using Google.Cloud.Firestore.Admin.V1Beta1;
using Google.Cloud.Firestore.V1Beta1;
using Grpc.Auth;

namespace FirestoreTest
{
    public partial class FirestoreTestClass
    {
        public async System.Threading.Tasks.Task FSBatchGetDocuments()
        {
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\n:: Batch Retrieve Documents ::\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "\nAvailable Docments:\n");
            ListDocuments();

            var batchGetDocumentsRequest = new BatchGetDocumentsRequest();
            batchGetDocumentsRequest.Database = Parent;

            while (true)
            {
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\n\nEnter Collection [cities]: ");
                string collectionId = Console.ReadLine();
                if (collectionId == "")
                {
                    collectionId = "cities";
                }

                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\nEnter Document Name (blank when finished): ");
                string documentName = Console.ReadLine();
                if (documentName != "")
                {
                    Document doc = null;
                    try
                    {
                        doc = GetDocument(documentName, collectionId);
                    }
                    catch (Grpc.Core.RpcException e)
                    {
                        if (e.Status.StatusCode == Grpc.Core.StatusCode.NotFound)
                        {
                            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Red, "ERROR: Document " + documentName + " not found, ignoring ...");
                        }
                        continue;
                    }
                        batchGetDocumentsRequest.Documents.Add(doc.Name);
                }
                else
                {
                    break;
                }
            }

            var batchGetDocumentsResponse = new BatchGetDocumentsResponse();
            try
            {
                var ret = FsClient.BatchGetDocuments(batchGetDocumentsRequest);
                var responseStream = ret.ResponseStream;
                var cancellationToken = new System.Threading.CancellationToken();
                while (await responseStream.MoveNext(cancellationToken))
                {
                    batchGetDocumentsResponse = responseStream.Current;
                    Utils.ReadDocument(batchGetDocumentsResponse.Found);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} Exception caught.", e);
                return;
            }
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\nSuccessfully batch retrieved documents!\n");
        }
    }
}