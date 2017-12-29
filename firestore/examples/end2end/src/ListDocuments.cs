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
        public void ListDocuments()
        {
            var listDocumentsRequest = new ListDocumentsRequest();
            listDocumentsRequest.Parent = Parent;
            var listDocumentsResponse = FsClient.ListDocuments(listDocumentsRequest);
            var documents = listDocumentsResponse.Documents;
            int i = 0;
            foreach (var document in documents)
            {
                i++;
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "\nDocument " + i + ": " + document.Name + "");
            }
        }

        public void FSListDocuments()
        {
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\n:: Listing all documents from Firestore... ::\n");
            ListDocuments();
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\nFinished listing documents!\n");
        }
    }
}