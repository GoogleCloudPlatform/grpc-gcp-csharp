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
        public void FSRollback()
        {
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\n :: Starting Rollback ::\n");
            if (TransactionId == null)
            {
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\nNo transaction to rollback, returning!");
                return;
            }
            var rollbackRequest = new RollbackRequest();
            rollbackRequest.Database = Parent;
            rollbackRequest.Transaction = TransactionId;
            try
            {
                FsClient.Rollback(rollbackRequest);
            }
            catch (Exception e)
            {
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Red, "Exception caught\n" + e.Message);
                return;
            }
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\nSuccessfully rollback!");
            // clear transactionId!
            ClearTransactionId();
        }
    }
}