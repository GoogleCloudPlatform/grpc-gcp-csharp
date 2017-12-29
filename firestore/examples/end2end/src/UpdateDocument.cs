using System;
using System.Collections.Generic;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using Google.Apis.Services;
using Google.Type;
using Google.Cloud.Firestore.Admin.V1Beta1;
using Google.Cloud.Firestore.V1Beta1;
using Grpc.Auth;
using Google.Protobuf.Collections;

namespace FirestoreTest
{
    public partial class FirestoreTestClass
    {
        public void FSUpdateDocument()
        {
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\n:: Update/Create a Document ::\n");

            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\nEnter Collection [cities]: ");
            string collectionId = Console.ReadLine();
            if (collectionId == "")
            {
                collectionId = "cities";
            }
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\nEnter Document Id: ");
            String docId = Console.ReadLine();
            Document updatedDoc = GetDocument(docId,collectionId);
            MapField<string, Value> fields;
            if (updatedDoc == null)
            {
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\nDocument within " + collectionId + " does not exist, create it (y/n): ");
                String ynValue = Console.ReadLine();
                if ((ynValue.ToLower() != "y") &&
                    (ynValue.ToLower() != "yes"))
                {
                    FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Red, "\nDocument update aborted, returning");
                    return;
                }
                updatedDoc = new Document();
                updatedDoc.Name = Parent + "/documents/" + collectionId + "/" + docId;
                fields = new MapField<string, Value>();
            }
            else
            {
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.White, "\nAvailable fields in this document to update:\n");
                Utils.ProcessFields(updatedDoc);
                fields = updatedDoc.Fields;
            }
            // update/create fields
            while (true)
            {
                FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\nEnter Field Name (blank when finished): ");
                string fieldName = Console.ReadLine();
                if (fieldName != "")
                {
                    FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\nEnter Field Value: ");
                    string fieldInValue = Console.ReadLine();
                    // TODO must query for type and handle!!!!
                    // TODO only handling string!!!!
                    Value fieldValue = new Value();
                    fieldValue.StringValue = fieldInValue;
                    if (fields.ContainsKey(fieldName))
                    {
                        fields[fieldName].StringValue = fieldInValue;
                    }
                    else
                    {
                        fields.Add(fieldName,fieldValue);
                    }
                }
                else
                {
                    break;
                }
                   
            }
            var updateDocumentRequest = new UpdateDocumentRequest();
            updateDocumentRequest.Document = updatedDoc;
            Document returnDocument;
            try
            {
                returnDocument = FsClient.UpdateDocument(updateDocumentRequest);
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} Exception caught.", e);
            }
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\nSuccessfully updated document!\n");
        }
    }
}