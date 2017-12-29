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
        public void FSCreateIndex()
        {
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\n:: Creating a Index ::\n");
            var index = new Index();
            while (true) 
            {
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "\nEnter Field Name (blank when finished): ");
                string fieldName = Console.ReadLine();
                if (fieldName != "")
                {
                    FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "\nEnter Mode (ASCENDING/DESCENDING) [ASCENDING]: ");
                    string fieldMode = Console.ReadLine();
                    IndexField.Types.Mode indexFieldMode = IndexField.Types.Mode.Ascending;
                    if (fieldMode == "" || fieldMode == "ASCENDING") { indexFieldMode = IndexField.Types.Mode.Ascending; }
                    else
                        if (fieldMode == "DESCENDING") { indexFieldMode = IndexField.Types.Mode.Descending; }
                        else
                            if (fieldMode != "ASCENDING" && fieldMode != "DESCENDING") 
                        { 
                            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Red, "\nUnrecognized Mode - Choosing ASCENDING"); 
                            indexFieldMode = IndexField.Types.Mode.Ascending; 
                        }
                    var indexField = new IndexField();
                    indexField.FieldPath = fieldName;
                    indexField.Mode = indexFieldMode;
                    index.Fields.Add(indexField);
                }
                else
                {
                    var createIndexRequest = new CreateIndexRequest();
                    createIndexRequest.Parent = Parent;
                    index.CollectionId = BaseCollectionId;
                    createIndexRequest.Index = index;
                    try
                    {
                        Operation operation = FsAdminClient.CreateIndex(createIndexRequest);
                    }
                    catch (Grpc.Core.RpcException e)
                    {
                        Grpc.Core.Status stat = e.Status;
                        if (stat.StatusCode == Grpc.Core.StatusCode.AlreadyExists)
                        {
                            Console.WriteLine("\nIndex already exists.");
                        }
                        Console.WriteLine(stat.Detail);
                    }
                    catch (Exception e)
                    {
                        FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Red, "\nException caught\n" + e.Message);
                    }
                    break;
                }
            }
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\nSuccessfully created new index!\n");
        }
    }
}