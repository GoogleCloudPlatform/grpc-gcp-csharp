using System;
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
        public void FSCreateDocument()
        {
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\n:: Creating a new Document ::\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\nEnter Collection [cities]: ");
            string collectionId = Console.ReadLine();
            if (collectionId == "")
            {
                collectionId = "cities";
            }

            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\nEnter Document: ");
            var docId = Console.ReadLine();
            var createDocRequest = new CreateDocumentRequest();
            createDocRequest.Parent = Parent;
            createDocRequest.DocumentId = docId;
            createDocRequest.CollectionId = collectionId;
            var fsDocument = new Document();
            createDocRequest.Document = fsDocument;
            try
            {
                fsDocument = FsClient.CreateDocument(createDocRequest);
            }
            catch (Grpc.Core.RpcException e)
            {
                Grpc.Core.Status stat = e.Status;
                if (stat.StatusCode == Grpc.Core.StatusCode.AlreadyExists)
                {
                    FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Red, "\nDocument already exists.");
                }
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\n" + stat.Detail);
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} Exception caught.", e);
            }

            // get the document to ensure it was created
            Document retDoc;
            try 
            {
                retDoc = GetDocument(docId, collectionId);
            }
            catch (Grpc.Core.RpcException e)
            {
                if (e.Status.StatusCode == Grpc.Core.StatusCode.NotFound)
                {
                    FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Red, "\nDocument was not added.");
                }
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Red, "\n" +  e.Status.Detail);
                return;
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} Exception caught.", e);
                return;
            }
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\nSuccessfully created new document!\nName:" + retDoc.Name + "\n");
        }
    }
}