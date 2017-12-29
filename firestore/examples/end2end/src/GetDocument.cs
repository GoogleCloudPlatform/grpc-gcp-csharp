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
        public Document GetDocument(String documentName, String collectionID = "GrpcTestData")
        {
            var getDocumentRequest = new GetDocumentRequest();
            getDocumentRequest.Name = Parent + "/documents/" + collectionID + "/" + documentName;
            Document retDoc;
            try
            {
                retDoc = FsClient.GetDocument(getDocumentRequest);
            }
            catch (Grpc.Core.RpcException e)
            {
                if (e.Status.StatusCode == Grpc.Core.StatusCode.NotFound)
                {
//                    FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Red, "ERROR: Document " + documentName + " not found!");
                    throw e;
                }
                else
                {
                    Console.WriteLine("{0} Exception caught.", e);
                }
                throw e;
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} Exception caught.", e);
                throw e;
            }
            return retDoc;
        }

        public void FSGetDocument()
        {
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\n:: Fetch a Specific Document ::\n");

            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\nEnter Collection [cities]: ");
            string collectionId = Console.ReadLine();
            if (collectionId == "")
            {
                collectionId = "cities";
            }

            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\nEnter Document Id: ");
            String docId = Console.ReadLine();

            Document retDoc = null;
            try
            {
                retDoc = GetDocument(docId, collectionId);
            }
            catch (Grpc.Core.RpcException e)
            {
                if (e.Status.StatusCode == Grpc.Core.StatusCode.NotFound)
                {
                    FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Red, "\nERROR: Document " + docId + " not found!");
                }
            }
            if (retDoc != null)
                Utils.ReadDocument(retDoc);
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\nFinished getting document!\n");
        }
    }
}