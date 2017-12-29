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
        public void FSListCollectionIds()
        {
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green,"\n:: Listing all Collection Ids from Document or Database... ::\n");
            var listCollectionIdsRequest = new ListCollectionIdsRequest();
            listCollectionIdsRequest.Parent = Parent;

            var listCollectionIdsResponse = FsClient.ListCollectionIds(listCollectionIdsRequest);
            var cids = listCollectionIdsResponse.CollectionIds;
            int i = 0;
            foreach (var cid in cids)
            {
                i++;
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "\n:: Collection Id " + i + ": " + cid.ToString());
            }
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\nFinished listing Collection Ids from Document or Database.");
        }
    }
}