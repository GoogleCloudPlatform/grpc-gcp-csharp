using System;
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
    public class FirestoreTestUtils
    {
        public void ProcessFields(Document document)
        {
            var fields = document.Fields;
            var i = 0;
            foreach (var field in fields)
            {
                i++;
                ColoredConsoleWrite(ConsoleColor.White, "\tField " + i + ": ");
                ColoredConsoleWrite(ConsoleColor.White, "\tKey: " + field.Key);
                ColoredConsoleWrite(ConsoleColor.White, "\tValue: " + field.Value.ToString() + "\n");
                //                Console.WriteLine("\tValue: " + field.Value);
                //                Console.WriteLine("\tToString: " + field.ToString());
                //                Console.WriteLine("\tType: " + field.GetType().ToString());
            }
        }

        public void ReadDocument(Document document)
        {
            ColoredConsoleWrite(ConsoleColor.Yellow, "Document Name: " + document.Name + "\n");
            ColoredConsoleWrite(ConsoleColor.Gray, "   Created: " + document.CreateTime.ToString() + "\n");
            ColoredConsoleWrite(ConsoleColor.Gray, "   Updated: " + document.UpdateTime.ToString() + "\n");
            ColoredConsoleWrite(ConsoleColor.Gray, "   Document Fields:\n");
            ProcessFields(document);
        }

        public void PrintIndex(Index index)
        {
            ColoredConsoleWrite(ConsoleColor.Yellow, "\n   Index Name: " + index.Name);
            ColoredConsoleWrite(ConsoleColor.Yellow, "\nCollection Id: " + index.CollectionId);
            ColoredConsoleWrite(ConsoleColor.Yellow, "\n        State: ");
            switch (index.State)
            {
                case Index.Types.State.Unspecified:
                    ColoredConsoleWrite(ConsoleColor.Yellow, Index.Types.State.Unspecified.ToString());
                    break;
                case Index.Types.State.Creating:
                    ColoredConsoleWrite(ConsoleColor.Yellow, Index.Types.State.Creating.ToString());
                    break;
                case Index.Types.State.Error:
                    ColoredConsoleWrite(ConsoleColor.Yellow, Index.Types.State.Error.ToString());
                    break;
                case Index.Types.State.Ready:
                    ColoredConsoleWrite(ConsoleColor.Yellow, Index.Types.State.Ready.ToString());
                    break;
                default:
                    ColoredConsoleWrite(ConsoleColor.Red, "Not Defined!");
                    break;
            }
            var fields = index.Fields;
            var i = 0;
            foreach (var field in fields)
            {
                i++;
                ColoredConsoleWrite(ConsoleColor.Gray, "\n     Field " + i + ": " + field.FieldPath);
                ColoredConsoleWrite(ConsoleColor.Gray, "     Mode: ");
                IndexField.Types.Mode indexFieldMode = field.Mode;
                switch (field.Mode)
                {
                    case IndexField.Types.Mode.Ascending:
                        ColoredConsoleWrite(ConsoleColor.Gray, "Ascending");
                        break;
                    case IndexField.Types.Mode.Descending:
                        ColoredConsoleWrite(ConsoleColor.Gray, "Descending");
                        break;
                    case IndexField.Types.Mode.Unspecified:
                        ColoredConsoleWrite(ConsoleColor.Gray, "Unspecified");
                        break;
                    default:
                        ColoredConsoleWrite(ConsoleColor.Red, "Not Defined!");
                        break;
                }

            }
        }

        public void AddField(MapField<string, Value> fields, String fieldName, String fieldValue)
        {
            if (fields.ContainsKey(fieldName))
            {
                fields[fieldName].StringValue = fieldValue;
            }
            else
            {
                Value strValue = new Value();
                strValue.StringValue = fieldValue;
                fields.Add(fieldName, strValue);
            }
        }

        public void AddField(MapField<string, Value> fields, String fieldName, bool fieldValue)
        {
            if (fields.ContainsKey(fieldName))
            {
                fields[fieldName].BooleanValue = fieldValue;
            }
            else
            {
                Value value = new Value();
                value.BooleanValue = fieldValue;
                fields.Add(fieldName, value);
            }
        }

        public void AddField(MapField<string, Value> fields, String fieldName, long fieldValue)
        {
            if (fields.ContainsKey(fieldName))
            {
                fields[fieldName].IntegerValue = fieldValue;
            }
            else
            {
                Value value = new Value();
                value.IntegerValue = fieldValue;
                fields.Add(fieldName, value);
            }
        }


        public static void ColoredConsoleWrite(ConsoleColor color, string text)
        {
            ConsoleColor originalColor = Console.ForegroundColor;
            Console.ForegroundColor = color;
            Console.Write(text);
            Console.ForegroundColor = originalColor;
        }

        private void AddCityData(String cityAbbr, 
                                 String name,
                                 String state,
                                 String country,
                                 Boolean capital,
                                 long population)
        {
            String collectionName = "cities";

            String docName = cityAbbr;

            Document updatedDoc = null;
            try
            {
                updatedDoc = MainClass.Ftc.GetDocument(docName, collectionName);
            }
            catch (Grpc.Core.RpcException e)
            {
                if (e.Status.StatusCode == Grpc.Core.StatusCode.NotFound)
                {
                    updatedDoc = new Document();
                    updatedDoc.Name = MainClass.Ftc.Parent + "/documents/" + collectionName + "/" + docName;
                }
            }
            MapField<string, Value> fields;

            fields = updatedDoc.Fields;
            // update/create fields
            AddField(fields, "name", name);
            AddField(fields, "state", state);
            AddField(fields, "country", country);
            AddField(fields, "capital", capital);
            AddField(fields, "population", population);

            var updateDocumentRequest = new UpdateDocumentRequest();
            updateDocumentRequest.Document = updatedDoc;
            Document returnDocument;
            try
            {
                returnDocument = MainClass.Ftc.FsClient.UpdateDocument(updateDocumentRequest);
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} Exception caught.", e);
            }

        }

        public void LoadData()
        {
            ColoredConsoleWrite(ConsoleColor.Green, "\n:: Loading Test Data ::\n");

            AddCityData("SF",
                       "San Francisco",
                        "CA",
                        "USA",
                        false, 
                        860000);
            AddCityData("LA",
                        "Los Angeles",
                        "CA",
                        "USA",
                        false, 
                        3900000);
            AddCityData("DC",
                        "Washington, D.C.",
                        "",
                        "USA",
                        true,
                        680000);
            AddCityData("TOK",
                        "Tokyo",
                        "",
                        "Japan", 
                        true,
                        9000000);
            AddCityData("BJ",
                        "Beijing",
                        "",
                        "China",
                        true,
                        21500000);
            AddCityData("NYC",
                        "New York",
                        "NY",
                        "USA",
                        false,
                        8550405);

            ColoredConsoleWrite(ConsoleColor.Green, "\nSuccessfully Loaded Data!\n");

        }
    }
}