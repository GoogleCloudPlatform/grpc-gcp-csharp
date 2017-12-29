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
        public String Parent;
        public String BaseCollectionId;
        public Google.Protobuf.ByteString TransactionId;
        public Firestore.FirestoreClient FsClient;
        public FirestoreAdmin.FirestoreAdminClient FsAdminClient;
        public FirestoreTestUtils Utils;

        public FirestoreTestClass(String parent, 
                                  Firestore.FirestoreClient fsClient,
                                  FirestoreAdmin.FirestoreAdminClient fsAdminClient,
                                  String baseCollectionId)
        {
            Parent = parent;
            FsClient = fsClient;
            FsAdminClient = fsAdminClient;
            BaseCollectionId = baseCollectionId;
            Utils = new FirestoreTestUtils();
            ClearTransactionId();
        }

        public void ClearTransactionId()
        {
            TransactionId = null;
        }

    }


    class MainClass
    {
        public static FirestoreTestClass Ftc;

        public static void Main(string[] args)
        {
            Console.Title = typeof(MainClass).Name;
            var parent = "projects/mch-test-49bba/databases/(default)";
            Console.Title = parent;

            Boolean debug = false;
            if (args.Length > 0 && args[0] == "--debug")
            {
                debug = true;
            }

            if (debug)
            {
                Console.WriteLine("GOOGLE_APPLICATION_CREDENTIALS: " + Environment.GetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS"));
                var storage = StorageClient.Create();
                var buckets = storage.ListBuckets("mch-test-49bba");
                foreach (var bucket in buckets)
                {
                    Console.WriteLine(bucket.Name);
                }
            }

            GoogleCredential credential = GoogleCredential.FromFile(Environment.GetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS"));
            if (credential == null)
            {
                Console.WriteLine("Could not create credential from file.");
                Console.WriteLine("GOOGLE_APPLICATION_CREDENTIALS: " + Environment.GetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS"));
            }
            Grpc.Core.Channel channel = new Grpc.Core.Channel(
                "firestore.googleapis.com:443",
                GoogleGrpcCredentials.ToChannelCredentials(credential)
            );

            Firestore.FirestoreClient fsClient = new Firestore.FirestoreClient(channel);
            FirestoreAdmin.FirestoreAdminClient fsAdminClient = new FirestoreAdmin.FirestoreAdminClient(channel);

            String baseCollectionId = "GrpcTestData";
            Ftc = new FirestoreTestClass(parent, fsClient, fsAdminClient, baseCollectionId);

            Run();

        }

        static void Run()
        {
            Ftc.Utils.LoadData();
            while (true)
            {
                DrawMenu();
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\nEnter an Option ('quit' to exit): "); // Prompt
                string line = Console.ReadLine(); // Get string from user
                line.Trim();
                if (line.Contains("q")) // Check string
                {
                    return;
                }
                ExecuteMenuEntry(line);
            }

        }

        public static void ExecuteMenuEntry(string menuEntry)
        {
            switch (menuEntry)
            {
                case "1":
                    Ftc.FSBatchGetDocuments().Wait();
                    break;
                case "2":
                    Ftc.FSBeginTransaction();
                    break;
                case "3":
                    Ftc.FSCommit();
                    break;
                case "4":
                    Ftc.FSCreateDocument();
                    break;
                case "5":
                    Ftc.FSDeleteDocument();
                    break;
                case "6":
                    Ftc.FSGetDocument();
                    break;
                case "7":
                    Ftc.FSListCollectionIds();
                    break;
                case "8":
                    Ftc.FSListDocuments();
                    break;
                case "9":
                    Ftc.FSRollback();
                    break;
                case "10":
                    Ftc.FSRunQuery().Wait();
                    break;
                case "11":
                    Ftc.FSUpdateDocument();
                    break;
                case "12":
                    Ftc.FSWrite().Wait();
                    break;
                case "13":
                    Ftc.FSCreateIndex();
                    break;
                case "14":
                    Ftc.FSDeleteIndex();
                    break;
                case "15":
                    Ftc.FSGetIndex();
                    break;
                case "16":
                    Ftc.FSListIndexes();
                    break;
                default:
                    FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Red, "Not yet implemneted!\n");
                    break;
            }
        }

        public static void DrawMenu()
        {
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\n\n     Google Firestore RPC Menu \n\n");
            // draw.drawLineSeparator(); 
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, " 1|batchgetdocuments ........ BatchGetDocuments\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, " 2|begintransaction  ........ BeginTransaction\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, " 3|commit ................... Commit\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, " 4|createdocument ........... CreateDocument\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, " 5|deletedocument ........... DeleteDocument\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, " 6|getdocument .............. GetDocument\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, " 7|listcollectionids ........ ListCollectionIds\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, " 8|listdocuments ............ ListDocuments\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, " 9|rollback ................. Rollback\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "10|runquery ................. RunQuery\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "11|updatedocument ........... UpdateDocument\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "12|write .................... Write\n\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "     Firestore Admin RPC's         \n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "13|createindex .............. CreateIndex\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "14|deleteindex .............. DeleteIndex\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "15|getindex ................. GetIndex\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "16|listindexes .............. ListIndex\n\n");

        }
    }
}
