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
        public void FSBeginTransaction()
        {
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\n :: Starting New Transaction ::\n");
            if (TransactionId != null)
            {
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "Transaction in play, returning!\n");
                return;
            }
            var options = new TransactionOptions();
            var beginTransactionRequest = new BeginTransactionRequest();
            beginTransactionRequest.Database = Parent;
            beginTransactionRequest.Options = options;
            var beginTransactionResponse = new BeginTransactionResponse();
            try
            {
                beginTransactionResponse = FsClient.BeginTransaction(beginTransactionRequest);
            }
            catch (Exception e)
            {
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Red, "Exception caught\n" + e.Message);
                return;
            }
            TransactionId = beginTransactionResponse.Transaction;
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\n Successfully began new transaction '");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, TransactionId.ToBase64().ToString());
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "'!"); 
        }
    }
}