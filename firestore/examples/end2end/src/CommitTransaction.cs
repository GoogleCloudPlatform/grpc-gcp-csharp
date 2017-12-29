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
        public void FSCommit()
        {
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\n :: Starting Commit ::\n");
            if (TransactionId == null)
            {
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "No transaction to commit, returning!\n");
                return;
            }
            var commitRequest = new CommitRequest();
            commitRequest.Database = Parent;
            commitRequest.Transaction = TransactionId;
            var commitResponse = new CommitResponse();
            try
            {
                commitResponse = FsClient.Commit(commitRequest);
            }
            catch (Exception e)
            {
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Red, "Exception caught\n" + e.Message);
                return;
            }
            var timestamp = commitResponse.CommitTime;
            if (timestamp == null)
            {
                timestamp = new Google.Protobuf.WellKnownTypes.Timestamp();
                // fix
            }
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\n Successfully commit at ");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, timestamp.ToDateTime().ToString());
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "!");
            // clear transactionId!
            ClearTransactionId();
        }
    }
}