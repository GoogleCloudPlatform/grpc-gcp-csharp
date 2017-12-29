using System;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using Google.Apis.Services;
using Google.Type;
using Google.Cloud.Firestore.Admin.V1Beta1;
using Google.Cloud.Firestore.V1Beta1;
using Google.LongRunning;
using Grpc.Auth;

namespace FirestoreTest
{
    public partial class FirestoreTestClass
    {

        public void ListIndexes()
        {
            var listIndexesRequest = new ListIndexesRequest();
            listIndexesRequest.Parent = Parent;
            ListIndexesResponse listIndexesResponse = new ListIndexesResponse();
            try
            {
                listIndexesResponse = FsAdminClient.ListIndexes(listIndexesRequest);
            }
            catch (Exception e)
            {
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Red, "Exception caught\n" + e.Message);
            }
            var indexes = listIndexesResponse.Indexes;
            var i = 0;
            foreach (var index in indexes)
            {
                i++;
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "\nIndex " + i + ": " + index.Name);
            }
        }

        public void FSListIndexes()
        {
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\n:: Listing all Indexes ::\n");
            ListIndexes();
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\nFinished listing indexes!\n");
        }
    }
}