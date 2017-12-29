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
        public void FSDeleteDocument()
        {
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\n:: Deleting a Document ::\n");

            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\nEnter Collection [cities]: ");
            string collectionId = Console.ReadLine();
            if (collectionId == "")
            {
                collectionId = "cities";
            }

            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\nEnter Document ID: ");
            var docId = Console.ReadLine();

            var deleteDocumentRequest = new DeleteDocumentRequest();
            deleteDocumentRequest.Name = Parent + "/documents/" + collectionId + "/" + docId;
            try
            {
                FsClient.DeleteDocument(deleteDocumentRequest);
            }
            catch (Grpc.Core.RpcException e)
            {
                if (e.Status.StatusCode == Grpc.Core.StatusCode.NotFound)
                {
                    FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\nDocument does not exists.");
                }
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Red, "\n" + e.Status.Detail);
                return;
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} Exception caught.", e);
                return;
            }
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\nSuccessfully deleted document!");
        }
    }
}