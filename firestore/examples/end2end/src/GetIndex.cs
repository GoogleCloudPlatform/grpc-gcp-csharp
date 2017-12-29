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
        public void FSGetIndex()
        {
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\n:: Fetch a Specific Index ::\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "\nAvailable Indexes:\n");
            ListIndexes();

            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "\nEnter Index Id: ");
            var indexId = Console.ReadLine();

            var getIndexRequest = new GetIndexRequest();
            getIndexRequest.Name = indexId;
            Index retIndex;
            try
            {
                retIndex = FsAdminClient.GetIndex(getIndexRequest);
            }
            catch (Grpc.Core.RpcException e)
            {
                if ((e.Status.StatusCode == Grpc.Core.StatusCode.NotFound) ||
                    (e.Status.StatusCode == Grpc.Core.StatusCode.InvalidArgument))
                {
                    FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Red, "\nERROR: Index " + indexId + " not found!\n");
                }
                else
                {
                    Console.WriteLine("{0} Exception caught.", e);
                }
                return;
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} Exception caught.", e);
                return;
            }
            Utils.PrintIndex(retIndex);
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\nFinished getting index!\n");
        }
    }
}