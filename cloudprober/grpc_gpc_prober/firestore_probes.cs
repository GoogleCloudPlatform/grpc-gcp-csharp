using System.Collections.Generic;
using System.Diagnostics;
using Google.Cloud.Firestore.V1Beta1;
using ProbeTestsBase;

namespace FirestoreProbesTest
{

	public class FirestoreProbesTestClass : ProbeTestsBaseClass
	{
		static private readonly Stopwatch stopwatch = new Stopwatch();
		private static string _PARENT_RESOURCE = "projects/cloudprober-testing/databases/(default)/documents";
		private Dictionary<string, string> probFunctions;

		public FirestoreProbesTestClass()
		{
			//Constructor
			this.probFunctions = new Dictionary<string, string>(){
				{"documents", "document"}
			};
		}

		public static void document(Firestore.FirestoreClient client, ref Dictionary<string, long> metrics){

			ListDocumentsRequest list_document_request = new ListDocumentsRequest();
			list_document_request.Parent = _PARENT_RESOURCE;

            stopwatch.Start();
            client.ListDocuments(list_document_request);
            stopwatch.Stop();
            

            long lantency = stopwatch.ElapsedMilliseconds;
			metrics.Add("list_documents_latency_ms", lantency);
		}

        public Dictionary<string, string> GetProbFunctions()
        {
            return this.probFunctions;
        }
    }
}


